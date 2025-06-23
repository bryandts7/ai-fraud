# src/writer.py

from google.cloud import bigquery
import pandas as pd

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