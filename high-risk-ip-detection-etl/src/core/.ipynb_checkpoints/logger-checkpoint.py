# src/core/logger.py - Bulletproof version that always works
import logging
import sys
from pathlib import Path
from typing import Dict, Any, Optional

def setup_logging(config: Optional[Dict[str, Any]] = None) -> logging.Logger:
    """
    Setup simple logging that always works - ignores complex config
    """
    # Always use simple logging to avoid configuration issues
    return setup_simple_logging()

def setup_simple_logging(level: str = "INFO") -> logging.Logger:
    """
    Setup simple logging that always works
    """
    # Clear any existing handlers to avoid conflicts
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Create logs directory if it doesn't exist (but don't fail if we can't)
    try:
        Path("logs").mkdir(exist_ok=True)
        log_file = "logs/pipeline.log"
    except Exception:
        log_file = None
    
    # Setup basic configuration with console output
    handlers = [logging.StreamHandler(sys.stdout)]
    
    # Add file handler only if we can create the log file
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file, mode='a')
            file_handler.setFormatter(logging.Formatter(
                '[%(asctime)s] %(levelname)s [%(name)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ))
            handlers.append(file_handler)
        except Exception as e:
            print(f"Warning: Could not create log file {log_file}: {e}")
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format='[%(asctime)s] %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=handlers,
        force=True  # Force reconfiguration
    )
    
    # Reduce noise from external libraries
    logging.getLogger('google.cloud').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('google.auth').setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    logger.info("Simple logging configured successfully")
    return logger

def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name"""
    return logging.getLogger(name)