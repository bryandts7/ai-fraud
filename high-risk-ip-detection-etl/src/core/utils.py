# src/core/utils.py
import os
from pathlib import Path
from typing import Dict, Any, List
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

def generate_list_of_hour(end_hour: str, lookback_period: int) -> List[str]:
    """
    Generate a list of hour strings in format 'YYYYMMDD_HH' within the lookback period.
    
    Args:
        end_hour (str): End hour in format 'YYYYMMDD_HH' (e.g., '20250610_23')
        lookback_period (int): Number of hours to look back from end_hour
        
    Returns:
        list: List of hour strings in format 'YYYYMMDDHH'
        
    Example:
        generate_list_of_hour('2025061023', 3)
        # Returns: ['20250610_21', '20250610_22', '20250610_23']
    """
    # Parse the end_hour string into a datetime object
    end_datetime = datetime.strptime(end_hour, '%Y%m%d_%H')
    
    # Calculate the start datetime by subtracting lookback_period hours
    start_datetime = end_datetime - timedelta(hours=lookback_period - 1)
    
    # Generate list of hours from start to end (inclusive)
    hour_list = []
    current_datetime = start_datetime
    
    while current_datetime <= end_datetime:
        hour_string = current_datetime.strftime('%Y%m%d_%H')
        hour_list.append(hour_string)
        current_datetime += timedelta(hours=1)
    
    return hour_list