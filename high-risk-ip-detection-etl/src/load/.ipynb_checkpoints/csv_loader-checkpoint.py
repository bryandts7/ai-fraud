# src/load/csv_loader.py
import pandas as pd
from pathlib import Path
from typing import Dict, Any
import logging

from .base_loader import BaseLoader
from core.exceptions import LoadError

class CSVLoader(BaseLoader):
    """Load data to CSV files"""
    
    def __init__(self, config):
        super().__init__(config)
        self.output_dir = config.get('client', {}).get('csv_folder', 'output')
        self.logger = logging.getLogger(__name__)
    
    def save_anomalies(self, anomalies_df: pd.DataFrame, context: Dict[str, Any]) -> str:
        """Save anomalies to CSV file"""
        try:
            # Ensure output directory exists
            output_path = Path(self.output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Build filename
            filename_template = self.config.get('naming', {}).get('csv_filename', 
                                                                 '{client_name}_flagged_ips_{DATE}_{START_HOUR}_{END_HOUR}.csv')
            filename = filename_template.format(**context)
            
            # Save to CSV
            filepath = output_path / filename
            anomalies_df.to_csv(filepath, index=False)
            
            self.logger.info(f"Anomalies saved to CSV: {filepath}")
            return str(filepath)
            
        except Exception as e:
            raise LoadError(f"CSV loading failed: {str(e)}") from e
    
    def load(self, data: pd.DataFrame, context: Dict[str, Any]) -> str:
        """Base loader interface implementation"""
        return self.save_anomalies(data, context)
