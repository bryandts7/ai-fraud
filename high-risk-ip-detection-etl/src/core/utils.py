# src/core/utils.py
import os
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timedelta

def ensure_directory(directory_path: str) -> Path:
    """Ensure directory exists, create if it doesn't"""
    path = Path(directory_path)
    path.mkdir(parents=True, exist_ok=True)
    return path

def format_date(date_obj: datetime, format_str: str = "%Y%m%d") -> str:
    """Format datetime object to string"""
    return date_obj.strftime(format_str)

def get_yesterday_date(format_str: str = "%Y%m%d") -> str:
    """Get yesterday's date as formatted string"""
    yesterday = datetime.utcnow() - timedelta(days=1)
    return format_date(yesterday, format_str)

def validate_environment():
    """Validate required environment variables"""
    required_vars = ['GOOGLE_APPLICATION_CREDENTIALS']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {missing_vars}")
