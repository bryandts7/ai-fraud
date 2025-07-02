
# stdlib
import argparse
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict
from google.cloud.exceptions import NotFound

from src.config_loader import load_config
from src.bq_client import BigQueryClient, estimate_query_cost
from src.models import GraphAnomalyDetector
from src.writer import write_flagged_apps_to_bq
from src.utils import ensure_directory, compute_dates, load_configs, get_client_list, filter_existing_clients, get_event_tables_std
from src.writer import save_anomalies_csv, write_anomalies_bq

# Set up a simple logger
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s %(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the High Risk IP Detection pipeline.
    Returns an argparse.Namespace with:
      - config:      path to main config YAML
      - start_date:  optional override YYYYMMDD

    """
    parser = argparse.ArgumentParser(
        description="High Risk IP Detection pipeline"
    )
    parser.add_argument("--config",     default="config/config.yaml",    help="Main config file")
    parser.add_argument("--start-date", help="Override start date (YYYYMMDD)")

    return parser.parse_args()

def main():
    args = parse_args()  
    cfg = load_configs(args.config)
    start_date = compute_dates(
        cfg["dates"]["date_format"], args.start_date
    )
    
    context = {
        "DATE"  :  start_date,
        "client":  cfg["client"]["name"],
    }
    
    out_dir = cfg["client"]["csv_folder"]
    ensure_directory(out_dir)
    logging.info(f"Output directory prepared at: {out_dir}")

    logging.info("Initializing BigQuery client")
    bq = BigQueryClient(config_path=args.config)
    
    raw_clients = get_client_list(exclude_test=True)
    active_clients = filter_existing_clients(bq, raw_clients, start_date)
    unioned_tables = get_event_tables_std(active_clients, start_date)
    
    context["UNIONED_TABLES"] = unioned_tables
    
    df = bq.run_template(
        "01_clients_raw_features",
        template_params=context,
        estimate_cost=args.estimate_cost,
    )
    df = df.rename(columns={'ip': 'IP'})
    df_clean = df[cfg["feature_engineering"]["columns_to_stay"]]
    df_clean = df_clean.dropna()
    
    detector = GraphAnomalyDetector(df_clean, use_gpu=True)
    results = detector.run()
    results_df = results['ip_anomalies_result']
    
    X = df_clean.drop(columns=['IP'])
    results_df_anomaly = results_df[results_df["is_anomaly"]==True]
    probs = isolation_forest_to_probability(results_df_anomaly["anomaly_score"], "severity_ranking")
    results_df_anomaly["probability"] = probs
    
    output = results_df_anomaly[['IP', 'anomaly_reason', 'probability']]
    
    csv_path = save_anomalies_csv(output, out_dir, cfg, context)

    bq_table = write_anomalies_bq(output, bq.client, cfg, context)

    
if __name__ == "__main__":
    main()
    