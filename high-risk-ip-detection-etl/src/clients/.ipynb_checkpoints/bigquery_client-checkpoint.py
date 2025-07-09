# src/clients/bigquery_client.py
import os
import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from google.cloud import bigquery
from google.cloud import bigquery_storage
from google.oauth2 import service_account

from core.exceptions import ExtractionError, LoadError

class BigQueryClient:
    """Enhanced BigQuery client with improved error handling and monitoring"""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._setup_clients()
    
    def _setup_clients(self):
        """Initialize BigQuery clients"""
        try:
            # Get configuration
            project_config = self.config.get('project', {})
            project_pull_config = self.config.get('project_pull', {})
            
            # Setup pull client (for data extraction)
            pull_project_id = project_pull_config.get('id')
            pull_creds_path = project_pull_config.get('creds')
            
            if pull_creds_path:
                pull_credentials = service_account.Credentials.from_service_account_file(
                    pull_creds_path,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
            else:
                pull_credentials = None
            
            # Setup write client (for data loading)
            write_project_id = project_config.get('id')
            write_creds_path = project_config.get('creds')
            
            if write_creds_path:
                write_credentials = service_account.Credentials.from_service_account_file(
                    write_creds_path,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
            else:
                write_credentials = None
            
            # Location
            location = os.getenv('BQ_LOCATION', 'US')
            
            # Initialize clients
            self.pull_client = bigquery.Client(
                project=pull_project_id, 
                location=location, 
                credentials=pull_credentials
            )
            
            self.write_client = bigquery.Client(
                project=write_project_id,
                location=location, 
                credentials=write_credentials
            )
            
            self.storage_client = bigquery_storage.BigQueryReadClient(credentials=pull_credentials)
            self.destination_dataset = self.config.get('bigquery', {}).get('destination_dataset')
            
        except Exception as e:
            raise ExtractionError(f"Failed to setup BigQuery clients: {str(e)}") from e
    
    def get_table(self, table_id: str):
        """Get table information"""
        return self.pull_client.get_table(table_id)
    
    def run_template(self, template_name: str, template_params: Dict[str, Any] = None) -> pd.DataFrame:
        """Run SQL template with parameters"""
        try:
            # Load SQL template
            sql = self._load_query_template(template_name, template_params)
            
            # Execute query
            start_time = time.perf_counter()
            
            job_config = bigquery.QueryJobConfig()
            job = self.pull_client.query(sql, job_config=job_config)
            
            # Convert to DataFrame
            df = job.result().to_dataframe(bqstorage_client=self.storage_client)
            
            elapsed = time.perf_counter() - start_time
            self.logger.info(f"Query executed successfully: {len(df)} rows in {elapsed:.2f}s")
            
            return df
            
        except Exception as e:
            raise ExtractionError(f"Query execution failed: {str(e)}") from e
    
    def load_dataframe_to_table(self, df: pd.DataFrame, dataset: str, 
                               table_name: str, write_disposition: str = "WRITE_TRUNCATE"):
        """Load DataFrame to BigQuery table"""
        try:
            table_ref = self.write_client.dataset(dataset).table(table_name)
            job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
            
            job = self.write_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()  # Wait for job completion
            
            self.logger.info(f"Loaded {len(df)} rows to {dataset}.{table_name}")
            
        except Exception as e:
            raise LoadError(f"Failed to load data to BigQuery: {str(e)}") from e
    
    def _load_query_template(self, template_name: str, params: Dict[str, Any] = None) -> str:
        """Load and format SQL template"""
        # Get project root directory
        base_dir = Path(__file__).parent.parent.parent
        sql_path = base_dir / "queries" / f"{template_name}.sql"
        
        if not sql_path.exists():
            raise FileNotFoundError(f"Query template not found: {sql_path}")
        
        template = sql_path.read_text()
        
        if params:
            template = template.format(**params)
        
        return template