import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
import logging
import numpy as np
import os
from google.cloud import bigquery


def save_df_to_csv(
    df: pd.DataFrame,
    output_dir: str,
    filename: str,
    index: bool = False
) -> Path:
    """
    Save a DataFrame to a CSV file under the given directory, creating the directory if needed.

    :param df: DataFrame to save
    :param output_dir: Base directory for CSV outputs
    :param filename: Name of the CSV file (with .csv extension)
    :param index: Whether to write row names (index)
    :return: Path to the saved CSV file
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    file_path = out_dir / filename
    df.to_csv(file_path, index=index)
    return file_path


def save_anomalies_csv(
    anomalies_df: pd.DataFrame,
    out_dir: str,
    cfg: Dict[str, Any],
    context: Dict[str, str]
) -> str:
    """
    Build filename from context and config, save the DataFrame locally, and return the file path.

    Updates `context['variables']` with the slug used.
    """
    # Build slug and filename
    variables_slug = cfg['naming']['variables'].format(**context)
    context['variables'] = variables_slug

    csv_name = cfg['naming']['csv_filename'].format(**context)
    file_path = os.path.join(out_dir, csv_name)

    # Save locally
    save_df_to_csv(anomalies_df, out_dir, csv_name)
    logging.info(f"Flagged apps saved locally as: {file_path}")

    return file_path


def write_flagged_apps_to_bq(
    bq_client: bigquery.Client,
    df_flagged: pd.DataFrame,
    dataset: str,
    table_name: str,
    write_disposition: str = "WRITE_TRUNCATE",
):
    table_ref = bq_client.dataset(dataset).table(table_name)
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    job = bq_client.load_table_from_dataframe(df_flagged, table_ref, job_config=job_config)
    job.result()
    return job


def write_anomalies_bq(anomalies_df, bq_client, cfg, context):
    """
    Write anomalies to BigQuery and return the destination table.
    """
    dataset    = cfg['bigquery']['destination_dataset']
    table_name = cfg['naming']['table_prefix_all'].format(**context)
    full_table = f"{dataset}.{table_name}"

    logging.info(f"Writing flagged apps to BigQuery: {full_table}")
    write_flagged_apps_to_bq(
        bq_client,
        anomalies_df,
        dataset=dataset,
        table_name=table_name
    )
    logging.info("Pipeline complete. All steps succeeded.")
    return full_table


