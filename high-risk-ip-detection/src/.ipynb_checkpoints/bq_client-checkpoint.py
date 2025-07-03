# src/bq_client.py

import os
import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from google.cloud import bigquery
from google.cloud import bigquery_storage
from google.oauth2 import service_account

from config_loader import load_config

def estimate_query_cost_bytes(
    bytes_processed: int,
    price_per_tb: float = 5.0,
    bytes_per_tb: int = 10**12,
) -> float:
    """
    Compute raw BigQuery cost in USD.
    """
    price_per_byte = price_per_tb / bytes_per_tb
    return bytes_processed * price_per_byte


def format_usd(amount: float, precision: int = 4) -> str:
    """
    Format a float amount into a USD string with commas and fixed decimals.
    E.g. format_usd(1.23456, 3) -> '$1.235'
    """
    return f"${amount:,.{precision}f}"


def load_query(
    name: str,
    params: Optional[Dict[str, Any]] = None
) -> str:
    """
    Load ./queries/{name}.sql and interpolate with `params` if given.
    """
    base_dir = Path(__file__).parent.parent  # project root
    sql_path = base_dir / "queries" / f"{name}.sql"
    if not sql_path.exists():
        raise FileNotFoundError(f"Query file not found: {sql_path}")
    template = sql_path.read_text()
    logging.info("Loaded SQL template %r", name)
    return template.format(**params) if params else template


class BigQueryClient:
    """
    Wrapper around BigQuery + Storage to run queries,
    dry-run for cost estimation, and return pandas DataFrames.
    """

    def __init__(self, config_path: str = "config/config.yaml"):
        bundle = load_config(config_path)
        cfg = bundle["config"]
        project_id = cfg["project"]["id"]
        credentials_path = cfg["project"]["creds"]
        credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
        
        location = os.getenv(
            "BQ_LOCATION",
            cfg.get("bigquery", {}).get("location", "US")
        )

        self.client = bigquery.Client(project=project_id, location=location, credentials=credentials)
        self.storage_client = bigquery_storage.BigQueryReadClient()
        self.destination_dataset = cfg["bigquery"]["destination_dataset"]

    def get_table(self, table_id: str):
        """
        Proxy through to google.cloud.bigquery.Client.get_table
        so that filter_existing_clients can call bq.get_table(...)
        """
        return self.client.get_table(table_id)

    def dry_run_query(self, sql: str) -> int:
        """
        Dry-run the query to get bytes processed.
        """
        job_config = bigquery.QueryJobConfig(
            dry_run=True, use_query_cache=False
        )
        job = self.client.query(sql, job_config=job_config)
        return job.total_bytes_processed  # type: ignore

    def run_query(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        estimate_cost: bool = False
    ) -> pd.DataFrame:
        """
        Execute SQL → DataFrame. If `estimate_cost`, log an estimate first.
        """
        logging.info("Executing BigQuery query")
        if estimate_cost:
            bytes_processed = self.dry_run_query(sql)
            raw_cost = estimate_query_cost_bytes(bytes_processed)
            cost_str = format_usd(raw_cost)
            logging.info(
                f"Estimated cost: {cost_str} for {bytes_processed:,} bytes processed"
            )

        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(k, "STRING", v)
                for k, v in params.items()
            ]

        start = time.perf_counter()
        df = (
            self.client
                .query(sql, job_config=job_config)
                .result()
                .to_dataframe(bqstorage_client=self.storage_client)
        )
        elapsed = time.perf_counter() - start
        logging.info(f"Query returned {len(df)} rows in {elapsed:.2f}s")
        return df

    def run_template(
        self,
        name: str,
        template_params: Optional[Dict[str, Any]] = None,
        estimate_cost: bool = False
    ) -> pd.DataFrame:
        """
        Load queries/{name}.sql, render with template_params,
        then execute with optional cost estimate.
        """
        sql = load_query(name, template_params)
        return self.run_query(sql, estimate_cost=estimate_cost)