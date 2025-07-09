import pandas as pd
from typing import Dict, Any
from pathlib import Path
from google.cloud import bigquery

from .base_extractor import BaseExtractor
from clients.bigquery_client import BigQueryClient
from core.exceptions import ExtractionError


class BigQueryExtractor(BaseExtractor):
    """Extract features from BigQuery"""
    
    def __init__(self, config):
        super().__init__(config)
        self.client = BigQueryClient(config)
        self.dataset = config.get('bigquery', {}).get('destination_dataset')
        self.project_id = config.get('project', {}).get('id')
    
    def extract_intermediaries(self, context: Dict[str, Any]) -> str:
        """Convert PING TABLE to EVENT TABLE and directly store to BQ"""
        try:
            query_context = self._build_query_context(context)
            
            table_template = context['intermediary_table_name']
            table_name = table_template.format(**context)
            table_ref = self.client.write_client.dataset(self.dataset).table(table_name)
            full_table_id = f"{self.project_id}.{self.dataset}.{table_name}"
            
            job_config = bigquery.QueryJobConfig(
                destination=table_ref,
                write_disposition="WRITE_TRUNCATE"  # or WRITE_APPEND, WRITE_EMPTY
                )
            query = query_context['UNIONED_TABLES']
            
            query_job = self.client.write_client.query(query, job_config=job_config)
            query_job.result()
            
            return full_table_id 
        
        except Exception as e:
            raise ExtractionError(f"Intermediaries extraction failed: {str(e)}") from e

    
    def extract_features(self, context: Dict[str, Any]) -> pd.DataFrame:
        """Extract IP features from BigQuery"""
        try:
            query_context = {'EVENT_FROM_PING': context['event_from_ping_table'] }
            
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
        # unioned_tables = client_fetcher.get_event_tables(
        #     context['active_clients'],
        #     context['start_date'],
        #     context['start_hour'],
        #     context['end_hour']
        # )
        
        unioned_tables = client_fetcher.get_event_tables_from_ping(
            context['active_clients'],
            context['list_of_hour']
        )
        
        return {
            'UNIONED_TABLES': unioned_tables,
            # 'DATE': context['start_date'],
            # 'START_HOUR': context['start_hour'],
            # 'END_HOUR': context['end_hour'],
            # 'client': context['client_name']
        }
    
    def validate_extraction(self, data: pd.DataFrame) -> bool:
        """Validate extracted data"""
        if data.empty:
            return False
        
        required_columns = ['IP', 'totalPerDevice']  # Add more as needed
        return all(col in data.columns for col in required_columns)