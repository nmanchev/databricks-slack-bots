import logging
import re

from typing import Dict, Any, List
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient
from model_serving_client import DatabricksModelServingClient

logger = logging.getLogger(__name__)

class SlackModelServingBot:
    """Slack bot that interfaces with Databricks Model Serving Endpoints"""

    def __init__(
        self, 
        slack_bot_token: str,
        slack_signing_secret: str,
        slack_app_token: str,
        model_serving_client: DatabricksModelServingClient,
        system_prompt: str = None
    ):
        """
        Initialize the Slack bot
        
        Args:
            slack_bot_token: Slack bot token
            slack_signing_secret: Slack signing secret
            slack_app_token: Slack app token for socket mode
            model_serving_client: Databricks Model Serving client instance
            system_prompt: Optional system prompt to guide the model's behavior
        """
        self.app = App(
            token=slack_bot_token,
            signing_secret=slack_signing_secret
        )
        self.slack_app_token = slack_app_token
        self.model_serving_client = model_serving_client
        self.client = WebClient(token=slack_bot_token)
        self.system_prompt = system_prompt

        # Store conversation history: slack_thread_ts -> list of message dicts
        self.conversation_history: Dict[str, List[Dict[str, str]]] = {}
        
        # Register event handlers
        self._register_handlers()


    def _register_handlers(self):
        """Register Slack event handlers"""

        @self.app.event("app_mention")
        def handle_app_mention(event, say, client):
            """Handle when the bot is mentioned"""
            self._handle_message(event, say, client)
        
        @self.app.event("message")
        def handle_message_events(event, say, client):
            """Handle direct messages to the bot"""
            # Only handle DMs and threaded replies
            if event.get("channel_type") == "im" or event.get("thread_ts"):
                self._handle_message(event, say, client)


    def _handle_message(self, event: Dict[str, Any], say, client):
        try:
            # Extract message details
            text = event.get("text", "")
            user = event.get("user")
            channel = event.get("channel")
            thread_ts = event.get("thread_ts") or event.get("ts")
            
            # Ignore bot messages
            if event.get("bot_id"):
                return
            
            # Remove bot mention from text
            text = self._clean_message_text(text)
            
            if not text.strip():
                say("Please ask me a question.", thread_ts=thread_ts)
                return
            
            # Send typing indicator
            client.chat_postMessage(
                channel=channel,
                text="🤔 Pre-Sales Assistant is thinking...",
                thread_ts=thread_ts
            )

            # Get conversation history for this thread
            conversation_history = self.conversation_history.get(thread_ts, [])
            
            # Ask the model
            logger.info(f"Asking model: {text}")
            result = self.model_serving_client.ask_question(
                text, 
                conversation_history=conversation_history,
                system_prompt=self.system_prompt
            )
            
            # Update conversation history
            if result.get("success"):
                # Add user message
                conversation_history.append({
                    "role": "user",
                    "content": text
                })
                
                # Add assistant response
                conversation_history.append({
                    "role": "assistant",
                    "content": result.get("response", "")
                })
                
                # Store updated history
                self.conversation_history[thread_ts] = conversation_history
            
            # Format and send response
            response_text = self._format_response(result)
            
            # Send the response
            say(response_text, thread_ts=thread_ts)
            
            # Send usage statistics if available
            usage = result.get("usage", {})
            if usage and usage.get("total_tokens"):
                self._send_usage_info(channel, thread_ts, usage, client)

        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)
            say(f"Sorry, I encountered an error: {str(e)}", thread_ts=thread_ts)


    def _clean_message_text(self, text: str) -> str:
        """
        Remove bot mention and clean up message text
        
        Args:
            text: Raw message text
            
        Returns:
            Cleaned text
        """
        # Remove bot mentions like <@U123456>
        text = re.sub(r'<@[A-Z0-9]+>', '', text)
        return text.strip()


    def _format_response(self, result: Dict[str, Any]) -> str:
        """
        Format the model response for Slack
        
        Args:
            result: Result dict from model serving client
            
        Returns:
            Formatted message text
        """
        if not result.get("success"):
            error = result.get("error", "Unknown error")
            return f"❌ {error}"
        
        response = result.get("response", "")
        
        return response if response else "✅ Response generated successfully"


    def _send_usage_info(
        self, 
        channel: str, 
        thread_ts: str, 
        usage: dict, 
        client
    ):
        """
        Send token usage information to Slack
        
        Args:
            channel: Slack channel ID
            thread_ts: Thread timestamp
            usage: Usage dict with token counts
            client: Slack client
        """
        try:
            prompt_tokens = usage.get("prompt_tokens")
            completion_tokens = usage.get("completion_tokens")
            total_tokens = usage.get("total_tokens")
            
            if total_tokens:
                usage_text = f"_Tokens used: {total_tokens}"
                if prompt_tokens and completion_tokens:
                    usage_text += f" (prompt: {prompt_tokens}, completion: {completion_tokens})"
                usage_text += "_"
                
                client.chat_postMessage(
                    channel=channel,
                    text=usage_text,
                    thread_ts=thread_ts
                )
                
        except Exception as e:
            logger.error(f"Error sending usage info: {e}")


    def clear_conversation_history(self, thread_ts: str):
        """
        Clear conversation history for a specific thread
        
        Args:
            thread_ts: Thread timestamp
        """
        if thread_ts in self.conversation_history:
            del self.conversation_history[thread_ts]
            logger.info(f"Cleared conversation history for thread {thread_ts}")


    def get_conversation_length(self, thread_ts: str) -> int:
        """
        Get the number of messages in a conversation
        
        Args:
            thread_ts: Thread timestamp
            
        Returns:
            Number of messages in the conversation
        """
        return len(self.conversation_history.get(thread_ts, []))


    def start(self):
        """Start the Slack bot in socket mode"""
        handler = SocketModeHandler(self.app, self.slack_app_token)
        logger.info("Starting Slack bot in socket mode...")
        handler.start()

