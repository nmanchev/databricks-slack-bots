"""
Main application entry point for Databricks Genie Slack Bot
"""
import logging
import sys
from config import Config
from model_serving_client import DatabricksModelServingClient
from slack_bot import SlackModelServingBot


def setup_logging():
    """Configure application logging"""
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('app.log')
        ]
    )


def main():
    """Main application entry point"""
    logger = logging.getLogger(__name__)
    
    try:
        # Setup logging
        setup_logging()
        logger.info("Starting Databricks Slack Bot...")
        
        # Validate configuration
        Config.validate()
        logger.info("Configuration validated successfully")
        
        # Initialize Databricks client with SDK Config
        # The SDK automatically uses service principal in Databricks Apps
        # or reads from environment variables for local development
        model_serving_client = DatabricksModelServingClient(Config.MODEL_SERVING_ENDPOINT_NAME, max_tokens=Config.MAX_TOKENS)
        system_prompt = Config.SYSTEM_PROMPT

        logger.info(f"Model serving client initialized successfully")
        logger.info(f"Using endpoint: {Config.MODEL_SERVING_ENDPOINT_NAME}")
        
        # Initialize and start Slack bot
        slack_bot = SlackModelServingBot(
            slack_bot_token=Config.SLACK_BOT_TOKEN,
            slack_signing_secret=Config.SLACK_SIGNING_SECRET,
            slack_app_token=Config.SLACK_APP_TOKEN,
            model_serving_client = model_serving_client,
            system_prompt = system_prompt
        )
        
        logger.info("Slack bot initialized, starting socket mode handler...")
        slack_bot.start()
        
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

