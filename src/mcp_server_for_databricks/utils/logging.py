"""Logging configuration and utilities."""

import sys
import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logging(log_dir: str = ".logs", log_filename: str = "mcp_unity.log") -> logging.Logger:
    """
    Set up logging configuration to write to a file and console.
    Creates the log directory if it doesn't exist.
    
    Args:
        log_dir: Directory to store log files
        log_filename: Name of the log file
        
    Returns:
        Configured root logger
    """
    # Create log directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file = os.path.join(log_dir, log_filename)
    
    # First clear any existing handlers to avoid duplicates
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create file handler with immediate flush
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024, 
        backupCount=5,
        delay=False  
    )
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    # Configure the root logger
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Log the setup and flush immediately
    root_logger.info(f"Logging configured to write to {log_file}")
    for handler in root_logger.handlers:
        handler.flush()
    
    return root_logger

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger configured to write to the .logs directory.
    This function ensures all loggers use the same configuration.
    
    Args:
        name: Name for the logger (typically __name__ from the calling module)
        
    Returns:
        Properly configured logger
    """
    # Use the root logger's handlers
    return logging.getLogger(name) 