"""
Logging utility for TMDB Movie Analysis Pipeline.

This module provides a centralized logging setup that outputs logs to both
console and file, making it easy to track pipeline execution and debug issues.
"""

import logging
import os
from datetime import datetime


def setup_logger(name='tmdb_pipeline', log_file='output/pipeline.log'):
    """
    Set up and configure a logger with both file and console handlers.
    
    Args:
        name (str): Name of the logger (default: 'tmdb_pipeline')
        log_file (str): Path to the log file (default: 'output/pipeline.log')
    
    Returns:
        logging.Logger: Configured logger instance
    
    Example:
        >>> logger = setup_logger()
        >>> logger.info("Pipeline started")
        >>> logger.error("Something went wrong")
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger
    
    # Create output directory if it doesn't exist
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    simple_formatter = logging.Formatter(
        '%(levelname)-8s | %(message)s'
    )
    
    # File handler - logs everything (DEBUG and above)
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    
    # Console handler - logs INFO and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Log initialization
    logger.info(f"Logger initialized at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return logger


def get_logger(name='tmdb_pipeline'):
    """
    Get an existing logger or create a new one.
    
    Args:
        name (str): Name of the logger
    
    Returns:
        logging.Logger: Logger instance
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        return setup_logger(name)
    return logger


# Create a default logger instance for easy import
logger = None


def init_default_logger():
    """Initialize the default logger when the module is first used."""
    global logger
    if logger is None:
        logger = setup_logger()
    return logger
