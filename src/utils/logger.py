"""
Module for setting up logging.
"""
import logging
import os
import sys
from datetime import datetime
from typing import Optional

from src.utils.config import config


def setup_logger(name: str = 'food_clustering',
                log_level: Optional[str] = None,
                log_file: Optional[str] = None) -> logging.Logger:
    """
    Set up logger.

    Args:
        name: Logger name.
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file: Path to log file. If None, logs will be output only to console.

    Returns:
        Configured logger.
    """
    # Get logging level from configuration if not explicitly specified
    if log_level is None:
        log_level = config.get('DEFAULT', 'log_level', fallback='INFO')

    # Convert string representation of logging level to constant
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid logging level: {log_level}')

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(numeric_level)

    # Clear existing handlers if any
    if logger.handlers:
        logger.handlers.clear()

    # Create formatter for logs
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Create handler for console output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # If log file is specified, create handler for writing to file
    if log_file is not None:
        # Create log directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Create handler for writing to file
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# Create log directory if it doesn't exist
logs_dir = 'logs'
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

# Create log filename with current date and time
log_filename = f'logs/food_clustering_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

# Create global logger
logger = setup_logger(log_file=log_filename)
