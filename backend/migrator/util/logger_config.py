import logging
import os
from datetime import datetime

def setup_logging(log_file='migration.log'):
    """Set up logging configuration to write logs to a file with improved readability."""
    log_dir = 'logs'

    # Ensure the logs directory exists
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Define a custom logging format with short datetime and indentation
    log_format = (
        "\n%(asctime)s - %(name)s - %(levelname)s:\n"
        "-----------------------------------------------------\n"
        "%(message)s\n"
        "-----------------------------------------------------\n"
    )

    # Configure logging with shortened date format
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt='%Y-%m-%d %H:%M:%S',  # Shortened date format (YYYY-MM-DD HH:MM:SS)
        handlers=[
            logging.FileHandler(f"{log_dir}/{log_file}"),
            logging.StreamHandler()
        ]
    )

def log_error_with_line_info(logger, exception):
    """Log error with the line number and traceback."""
    logger.error(
        "An error occurred at line %s in %s:\n%s",
        exception.__traceback__.tb_lineno,  # Get the line number
        exception.__traceback__.tb_frame.f_code.co_filename,  # Get the file name
        str(exception)  # Error message
    )
