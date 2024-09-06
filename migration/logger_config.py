import logging
import os

def setup_logging(log_file='migration.log'):
    """Set up logging configuration to write logs to a file."""
    log_dir = 'logs'
    
    # Ensure the logs directory exists
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(f"{log_dir}/{log_file}"),
            logging.StreamHandler()
        ]
    )
