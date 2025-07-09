# src/load/bigquery_loader.py
import pandas as pd
from typing import Dict, Any
import logging
from .base_loader import BaseLoader
from clients.bigquery_client import BigQueryClient
from core.exceptions import LoadError

class BigQueryLoader(BaseLoader):
    """Load data to BigQuery"""
    
    def __init__(self, config):
        super().__init__(config)
        self.client = BigQueryClient(config)
        self.dataset = config.get('bigquery', {}).get('destination_dataset')
        self.logger = logging.getLogger(__name__)
    
    def save_anomalies(self, anomalies_df: pd.DataFrame, context: Dict[str, Any]) -> str:
        """Save anomalies to BigQuery table"""
        try:
            # Build table name
            table_template = self.config.get('naming', {}).get('table_prefix_all',
                                                               'flagged_ips_{client_name}_{start_hour}_{end_hour}')
            table_name = table_template.format(**context)
            full_table_id = f"{self.dataset}.{table_name}"
            
            # Load to BigQuery
            self.client.load_dataframe_to_table(
                anomalies_df, 
                self.dataset, 
                table_name,
                write_disposition="WRITE_TRUNCATE"
            )
            
            self.logger.info(f"Anomalies loaded to BigQuery: {full_table_id}")
            return full_table_id
            
        except Exception as e:
            raise LoadError(f"BigQuery loading failed: {str(e)}") from e
    
    def save_full_data(self, full_data_df: pd.DataFrame, context: Dict[str, Any]) -> str:
        """Save full data (including both anomalies and normal data) to BigQuery table"""
        try:
            # Build table name for full data
            table_template = self.config.get('naming', {}).get('table_prefix_full_data', 
                                                              'full_data_ips_anomaly_{client_name}_{start_hour}_{end_hour}')
            table_name = table_template.format(**context)
            full_table_id = f"{self.dataset}.{table_name}"
            
            # Load to BigQuery
            self.client.load_dataframe_to_table(
                full_data_df, 
                self.dataset, 
                table_name,
                write_disposition="WRITE_TRUNCATE"
            )
            
            self.logger.info(f"Full data loaded to BigQuery: {full_table_id}")
            return full_table_id
            
        except Exception as e:
            raise LoadError(f"BigQuery full data loading failed: {str(e)}") from e
    
    def load(self, data: pd.DataFrame, context: Dict[str, Any]) -> str:
        """Base loader interface implementation"""
        return self.save_anomalies(data, context)