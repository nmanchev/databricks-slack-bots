"""
Databricks Model Serving Endpoint Client for conversational interactions
"""
import logging
from typing import Optional, Dict, Any, List
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole


logger = logging.getLogger(__name__)


class DatabricksModelServingClient:
    """
    Client for interacting with Databricks Model Serving Endpoints for chat/conversational AI.
    
    This client uses the Databricks SDK with automatic OAuth M2M (machine-to-machine) 
    authentication via service principals.
    """
    
    def __init__(self, endpoint_name: str, max_tokens: Optional[int] = None, temperature: Optional[float] = None):
        """
        Initialize the Databricks Model Serving client using Databricks SDK with OAuth M2M authentication.
        
        Args:
            endpoint_name: Name of the Model Serving endpoint to use
            max_tokens: Optional maximum tokens for responses
            temperature: Optional temperature for response generation (0.0-1.0)
        
        Service Principal Authentication:
            When you create a Databricks App, Databricks automatically provisions a dedicated 
            service principal for that app. This client leverages that for authentication.
            
            Key facts:
            - ✅ Service principals are automatically provisioned when creating apps
            - ✅ Each app gets its own unique service principal (cannot reuse existing ones)
            - ✅ OAuth Client ID and Secret are found in the app's Authorization tab
            - ✅ The Databricks SDK automatically reads these credentials at runtime
            - ✅ OAuth tokens are managed automatically (no manual handling needed)
        
        Authentication Flow:
            **In Databricks Apps (Recommended for Production):**
            1. You create a Databricks App → service principal auto-provisioned
            2. Platform provides DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET
            3. SDK detects app environment and uses OAuth 2.0 M2M authentication
            4. All API calls use: Authorization: Bearer <oauth_access_token>
            5. Token refresh is handled automatically
            
            **In Local Development:**
            - Set DATABRICKS_HOST and DATABRICKS_TOKEN in environment (.env file)
            - The SDK reads these and authenticates with personal access token
            - Useful for testing before deployment
        
        Permissions Required:
            The app's service principal must have explicit permissions:
            - CAN QUERY permission on the Model Serving endpoint
            - Access to the endpoint itself
            
            Note: Service principal authorization operates independently of user-level 
            permissions. All app users share the same permissions as the app's principal.
        """
        self.endpoint_name = endpoint_name
        self.max_tokens = max_tokens
        self.temperature = temperature
        
        # Initialize Databricks SDK WorkspaceClient
        # The SDK automatically detects the environment and uses appropriate authentication:
        # 
        # In Databricks Apps (Production):
        #   - Reads DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET (auto-provided by platform)
        #   - Uses OAuth 2.0 M2M (machine-to-machine) authentication
        #   - Service principal was auto-provisioned when the app was created
        #   - All API calls include: Authorization: Bearer <oauth_access_token>
        #   - Token refresh happens automatically
        # 
        # In Local Development:
        #   - Reads DATABRICKS_HOST and DATABRICKS_TOKEN from environment
        #   - Uses personal access token authentication
        #   - Useful for testing before deploying to Databricks Apps
        logger.info("Initializing Databricks SDK with OAuth M2M authentication")
        self.workspace_client = WorkspaceClient()
        
        # Get the serving endpoints API
        self.serving_endpoints = self.workspace_client.serving_endpoints
        
        # Get host from workspace client config
        self.host = self.workspace_client.config.host.rstrip('/')
        
        logger.info(f"✓ Connected to Databricks workspace: {self.host}")
        logger.info(f"✓ Using OAuth M2M authentication via app service principal")
        logger.info(f"✓ Model Serving endpoint: {self.endpoint_name}")
        
        # Verify endpoint exists
        try:
            endpoint = self.serving_endpoints.get(self.endpoint_name)
            logger.info(f"✓ Endpoint '{self.endpoint_name}' found with state: {endpoint.state.config_update}")
        except Exception as e:
            logger.warning(f"Could not verify endpoint existence: {e}")
    

    def create_conversation(self) -> Optional[str]:
        """
        Create a new conversation context.
        
        Note: Model Serving endpoints are stateless. Each request is independent.
        This method returns a client-side conversation ID for tracking purposes.
        The conversation history must be maintained by the client and passed with each request.
        
        Returns:
            A new conversation ID (UUID) for client-side tracking
        """
        import uuid
        conversation_id = str(uuid.uuid4())
        logger.info(f"Created new conversation context: {conversation_id}")
        return conversation_id
    

    def send_message(
        self, 
        message: str, 
        conversation_history: Optional[List[ChatMessage]] = None,
        system_prompt: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Send a message to the Model Serving endpoint.
        
        Args:
            message: The user's message/question
            conversation_history: Optional list of previous ChatMessage objects for context
            system_prompt: Optional system prompt to guide the model's behavior
            
        Returns:
            Response dict with 'content', 'role', and metadata if successful, None otherwise
        """
        try:
            # Build the messages list
            messages = []
            
            # Add system prompt if provided
            if system_prompt:
                messages.append(ChatMessage(role=ChatMessageRole.SYSTEM, content=system_prompt))
            
            # Add conversation history if provided
            if conversation_history:
                messages.extend(conversation_history)
            
            # Add the current user message
            messages.append(ChatMessage(role=ChatMessageRole.USER, content=message))
            
            # Prepare request parameters
            request_params = {
                "messages": messages
            }
            
            # Add optional parameters if set
            if self.max_tokens is not None:
                request_params["max_tokens"] = self.max_tokens
            
            if self.temperature is not None:
                request_params["temperature"] = self.temperature
            
            # Query the endpoint
            logger.info(f"Sending message to endpoint '{self.endpoint_name}'")
            response = self.serving_endpoints.query(
                name=self.endpoint_name,
                **request_params
            )
            
            # Extract the response content
            # The response structure varies by model, but typically has 'choices'
            if hasattr(response, 'choices') and response.choices:
                choice = response.choices[0]
                
                # Extract message content
                if hasattr(choice, 'message'):
                    content = choice.message.content
                    role = choice.message.role
                elif hasattr(choice, 'text'):
                    content = choice.text
                    role = ChatMessageRole.ASSISTANT
                else:
                    content = str(choice)
                    role = ChatMessageRole.ASSISTANT
                
                # Extract usage statistics if available
                usage = {}
                if hasattr(response, 'usage'):
                    usage = {
                        "prompt_tokens": getattr(response.usage, 'prompt_tokens', None),
                        "completion_tokens": getattr(response.usage, 'completion_tokens', None),
                        "total_tokens": getattr(response.usage, 'total_tokens', None)
                    }
                
                result = {
                    "content": content,
                    "role": role,
                    "usage": usage,
                    "finish_reason": getattr(choice, 'finish_reason', None),
                    "raw_response": response
                }
                
                logger.info(f"Received response from endpoint (tokens: {usage.get('total_tokens', 'N/A')})")
                return result
            
            # Fallback for different response structures
            elif hasattr(response, 'predictions'):
                content = response.predictions[0] if response.predictions else str(response)
                return {
                    "content": content,
                    "role": ChatMessageRole.ASSISTANT,
                    "usage": {},
                    "raw_response": response
                }
            
            else:
                logger.error(f"Unexpected response structure: {response}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to send message to endpoint: {e}")
            return None
    

    def ask_question(
        self, 
        question: str, 
        conversation_history: Optional[List[Dict[str, str]]] = None,
        system_prompt: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        High-level method to ask a question and get the response.
        
        Args:
            question: The question to ask
            conversation_history: Optional list of previous messages as dicts with 'role' and 'content'
            system_prompt: Optional system prompt to guide the model's behavior
            
        Returns:
            Dict with 'success', 'response', 'usage', and other metadata
        """
        # Convert conversation history from dicts to ChatMessage objects
        chat_history = None
        if conversation_history:
            chat_history = []
            for msg in conversation_history:
                role_str = msg.get('role', '').upper()
                # Map string roles to ChatMessageRole enum
                if role_str == 'USER':
                    role = ChatMessageRole.USER
                elif role_str == 'ASSISTANT':
                    role = ChatMessageRole.ASSISTANT
                elif role_str == 'SYSTEM':
                    role = ChatMessageRole.SYSTEM
                else:
                    logger.warning(f"Unknown role '{role_str}', defaulting to USER")
                    role = ChatMessageRole.USER
                
                chat_history.append(ChatMessage(
                    role=role,
                    content=msg.get('content', '')
                ))
        
        # Send the message
        result = self.send_message(question, chat_history, system_prompt)
        
        if not result:
            return {
                "success": False,
                "error": "Failed to get response from endpoint"
            }
        
        return {
            "success": True,
            "response": result.get("content", ""),
            "usage": result.get("usage", {}),
            "finish_reason": result.get("finish_reason"),
            "role": result.get("role")
        }
    

    def get_conversation_messages(
        self, 
        conversation_history: List[Dict[str, str]]
    ) -> List[ChatMessage]:
        """
        Convert a list of message dicts to ChatMessage objects.
        
        Args:
            conversation_history: List of dicts with 'role' and 'content' keys
            
        Returns:
            List of ChatMessage objects
        """
        messages = []
        for msg in conversation_history:
            role_str = msg.get('role', '').upper()
            
            # Map string roles to ChatMessageRole enum
            if role_str == 'USER':
                role = ChatMessageRole.USER
            elif role_str == 'ASSISTANT':
                role = ChatMessageRole.ASSISTANT
            elif role_str == 'SYSTEM':
                role = ChatMessageRole.SYSTEM
            else:
                logger.warning(f"Unknown role '{role_str}', defaulting to USER")
                role = ChatMessageRole.USER
            
            messages.append(ChatMessage(
                role=role,
                content=msg.get('content', '')
            ))
        
        return messages
    
    
    def stream_message(
        self,
        message: str,
        conversation_history: Optional[List[ChatMessage]] = None,
        system_prompt: Optional[str] = None
    ):
        """
        Send a message and stream the response (if supported by the endpoint).
        
        Args:
            message: The user's message/question
            conversation_history: Optional list of previous ChatMessage objects for context
            system_prompt: Optional system prompt to guide the model's behavior
            
        Yields:
            Response chunks as they arrive
            
        Note: Streaming support depends on the specific model and endpoint configuration.
        """
        try:
            # Build the messages list
            messages = []
            
            # Add system prompt if provided
            if system_prompt:
                messages.append(ChatMessage(
                    role=ChatMessageRole.SYSTEM,
                    content=system_prompt
                ))
            
            # Add conversation history if provided
            if conversation_history:
                messages.extend(conversation_history)
            
            # Add the current user message
            messages.append(ChatMessage(
                role=ChatMessageRole.USER,
                content=message
            ))
            
            # Prepare request parameters with streaming enabled
            request_params = {
                "messages": messages,
                "stream": True
            }
            
            # Add optional parameters if set
            if self.max_tokens is not None:
                request_params["max_tokens"] = self.max_tokens
            
            if self.temperature is not None:
                request_params["temperature"] = self.temperature
            
            # Query the endpoint with streaming
            logger.info(f"Streaming message to endpoint '{self.endpoint_name}'")
            
            for chunk in self.serving_endpoints.query(
                name=self.endpoint_name,
                **request_params
            ):
                yield chunk
                
        except Exception as e:
            logger.error(f"Failed to stream message from endpoint: {e}")
            yield None