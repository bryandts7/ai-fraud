import pandas as pd
from typing import Dict, Any
from pathlib import Path

from .base_extractor import BaseExtractor
from clients.bigquery_client import BigQueryClient
from core.exceptions import ExtractionError


class BigQueryExtractor(BaseExtractor):
    """Extract features from BigQuery"""
    
    def __init__(self, config):
        super().__init__(config)
        self.client = BigQueryClient(config)
    
    def extract_features(self, context: Dict[str, Any]) -> pd.DataFrame:
        """Extract IP features from BigQuery"""
        try:
            # Build query context
            query_context = self._build_query_context(context)
            
            # Execute feature extraction query
            df = self.client.run_template(
                "01_clients_raw_features",
                template_params=query_context
            )
            
            # if not self.validate_extraction(df):
            #     raise ExtractionError("Data validation failed")
            
            return df.rename(columns={'ip': 'IP'})
            
        except Exception as e:
            raise ExtractionError(f"Feature extraction failed: {str(e)}") from e
    
    def _build_query_context(self, context: Dict[str, Any]) -> Dict[str, str]:
        """Build query template context"""
        from extract.client_fetcher import ClientFetcher
        
        client_fetcher = ClientFetcher(self.config)
        unioned_tables = client_fetcher.get_event_tables(
            context['active_clients'],
            context['start_date'],
            context['start_hour'],
            context['end_hour']
        )
        
        return {
            'UNIONED_TABLES': unioned_tables,
            'DATE': context['start_date'],
            'START_HOUR': context['start_hour'],
            'END_HOUR': context['end_hour'],
            'client': context['client_name']
        }
    
    def validate_extraction(self, data: pd.DataFrame) -> bool:
        """Validate extracted data"""
        if data.empty:
            return False
        
        required_columns = ['IP', 'totalPerDevice']  # Add more as needed
        return all(col in data.columns for col in required_columns)