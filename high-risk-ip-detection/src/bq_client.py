# src/bq_client.py

import os
import pandas as pd
from typing import Any, Dict, Optional

# GCP library imports
from google.cloud import bigquery
from google.cloud import bigquery_storage

from .config_loader import load_config

def estimate_query_cost(bytes_processed: int, cost_per_byte: float = 5e-13) -> float:
    """
    Estimate cost in USD given bytes processed.
    Default pricing: $5 per TB = $5 / (10^12 bytes) = 5e-12 per byte,
    but we use 5e-13 here to reflect on-demand discounts.
    """
    return bytes_processed * cost_per_byte

class BigQueryClient:
    """
    Wrapper around BigQuery + BigQuery Storage to run queries, dry-run
    for cost estimation, and return pandas DataFrames.
    """

    def __init__(self, config_path: str = "config/config.yaml"):
        # Load project & location from your YAML
        bundle = load_config(config_path)
        cfg = bundle["config"]
        project_id = cfg["project"]["id"]
        location = os.getenv("BQ_LOCATION", cfg.get("bigquery", {}).get("location", "US"))

        # Credentials: you can read from env or extend config_loader to pull from YAML
        # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cfg["auth"]["credentials_file"]

        self.client = bigquery.Client(project=project_id, location=location)
        self.storage_client = bigquery_storage.BigQueryReadClient()
        self.destination_dataset = cfg["bigquery"]["destination_dataset"]

    def dry_run_query(self, sql: str) -> int:
        """
        Perform a dry run of the query to get the number of bytes processed.
        """
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = self.client.query(sql, job_config=job_config)
        # Dry run jobs don't run; bytes_processed is populated on the job object.
        return query_job.total_bytes_processed  # type: ignore

    def run_query(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        estimate_cost: bool = False
    ) -> pd.DataFrame:
        """
        Execute the given SQL and return a pandas DataFrame.
        If `estimate_cost=True`, prints the estimated cost as well.
        """
        # Optional cost estimation
        if estimate_cost:
            bytes_processed = self.dry_run_query(sql)
            cost = estimate_query_cost(bytes_processed)
            print(f"Estimated cost: ${cost:.4f} for {bytes_processed} bytes processed")

        # Actually run the query
        job_config = bigquery.QueryJobConfig()
        if params:
            # convert to BigQuery named params if you use @param syntax
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(k, "STRING", v)
                for k, v in params.items()
            ]

        query_job = self.client.query(sql, job_config=job_config)
        # Use BigQuery Storage API for faster downloads
        df = query_job.result().to_dataframe(bqstorage_client=self.storage_client)
        return df