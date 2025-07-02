from pathlib import Path
from typing import Any, Dict, Optional
import pandas as pd
import numpy as np
from scipy import stats


def load_query(name: str, params: Optional[Dict[str, Any]] = None) -> str:
    """
    Load a SQL template from the queries/ directory and optionally format it with provided params.

    :param name: Base filename of the SQL (without .sql extension)
    :param params: Dict of template placeholders and their values
    :return: Rendered SQL string
    """
    sql_path = Path("queries") / f"{name}.sql"
    if not sql_path.exists():
        raise FileNotFoundError(f"Query file not found: {sql_path}")
    template = sql_path.read_text()
    return template.format(**params) if params is not None else template


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


def ensure_directory(path: str) -> Path:
    """
    Ensure that the directory exists; create it if it doesn't.

    :param path: Directory path
    :return: Path object to the directory
    """
    dir_path = Path(path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path

def get_client_list(exclude_test=False):
    url = "http://pipeline-xlarge-aggregator.us-east-1.elasticbeanstalk.com/pixalate/v1/aggregator/health/clients"
    headers = {'authorization': 'Basic:cGl4ZWxz'}
    resp = requests.get(url, headers=headers)
    text = json.loads(resp.text)
    if exclude_test:
        return [str(x).upper() for x in text if str(x) not in ["iab","px","ftvf","tht","lkda","ainf","test","pingdom","pixalate", "mf", "tw", "xad", "nad", "pkt", "we"]]
    else:
        return [str(x).upper() for x in text]
    

def filter_existing_clients(bq_client,
                            client_list, 
                            start_date,
                            project="pixalate.com",
                            dataset="pixalate"):
    """
    Returns only those client_ids for which
    <project>.<dataset>.<CLIENT>.Event_<YYYYMMDD> exists.
    """
    date_suffix = start_date.replace("-", "")
    good = []
    for cid in client_list:
        table_id = f"{project}:{dataset}.{cid.upper()}.Event_{date_suffix}"
        try:
            # This will raise NotFound if the table doesn't exist
            bq_client.get_table(table_id)
            good.append(cid)
        except NotFound:
            # silently skip missing tables
            continue
    return good


def get_event_tables_std(client_list, start_date, start_hour, end_hour):
    """
    Returns a UNION ALL of all Event_<YYYYMMDD> tables for the given clients,
    using Standard SQL table identifiers.
    """
    date_suffix = start_date.replace('-', '')
    project     = "pixalate.com"
    dataset     = "pixalate"
    
    hour1 = f"{date_suffix} {start_hour}:00:00"
    hour2 = f"{date_suffix} {end_hour}:00:00"

    selects = [
        # note the change here: use dots, not colon, inside the backticks
        f"SELECT ip, kv18, os, eventTime, kv19, kv20, kv21, kv22, visitorId, impressions, kv4, s18, kv19, kv20, kv21, kv22, s24, sessionTime, b3, deviceType, adInstanceTime "
        f"FROM `{project}:{dataset}.{client_id.upper()}.Event_{date_suffix}`"
        f"WHERE FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', TIMESTAMP_TRUNC(TIMESTAMP_MICROS(CAST(eventTime AS INT64) * 1000), HOUR, 'UTC')) >= {hour1}"
        f"AND FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', TIMESTAMP_TRUNC(TIMESTAMP_MICROS(CAST(eventTime AS INT64) * 1000), HOUR, 'UTC')) < {hour2}"
        for client_id in client_list
    ]

    return "\nUNION ALL\n".join(selects)


def isolation_forest_to_probability(anomaly_scores, method='severity_ranking'):
    """
    Convert Isolation Forest anomaly scores to probabilities in range [0.5, 1]
    
    Parameters:
    anomaly_scores: array of isolation forest scores (negative values)
    method: approach to calculate probability
    
    Returns:
    probabilities: array of values between 0.5 and 1
    """
    
    if method == 'severity_ranking':
        """
        RECOMMENDED: Rank anomalies by severity within the anomaly set
        Most negative (severe) anomaly gets highest probability
        """
        # More negative = more anomalous = higher probability
        # Invert so more negative becomes higher value
        inverted_scores = -anomaly_scores  # Convert to positive, more positive = more anomalous
        
        # Min-max scale to [0,1]
        min_score = inverted_scores.min()
        max_score = inverted_scores.max()
        
        if max_score == min_score:
            return np.ones(len(anomaly_scores)) * 0.75  # Return 0.75 instead of 0.5
        
        prob_0_to_1 = (inverted_scores - min_score) / (max_score - min_score)
        # Scale from [0,1] to [0.5,1]
        prob = 0.5 + 0.5 * prob_0_to_1
        return prob
    
    elif method == 'percentile_within_anomalies':
        """
        Use percentile ranking within the anomaly group
        """
        # More negative = higher rank = higher probability
        inverted_scores = -anomaly_scores
        ranks = stats.rankdata(inverted_scores, method='average')
        prob_0_to_1 = (ranks - 1) / (len(ranks) - 1)
        # Scale from [0,1] to [0.5,1]
        prob = 0.5 + 0.5 * prob_0_to_1
        return prob
    
    elif method == 'exponential_severity':
        """
        Exponential transformation emphasizing most severe anomalies
        """
        inverted_scores = -anomaly_scores
        min_score = inverted_scores.min()
        max_score = inverted_scores.max()
        
        if max_score == min_score:
            return np.ones(len(anomaly_scores)) * 0.75
        
        normalized = (inverted_scores - min_score) / (max_score - min_score)
        prob_0_to_1 = (np.exp(2 * normalized) - 1) / (np.exp(2) - 1)  # Scale to [0,1]
        # Scale from [0,1] to [0.5,1]
        prob = 0.5 + 0.5 * prob_0_to_1
        return prob
    
    elif method == 'sigmoid_centered':
        """
        Sigmoid transformation centered around median anomaly
        """
        inverted_scores = -anomaly_scores
        median_score = np.median(inverted_scores)
        std_score = inverted_scores.std()
        
        if std_score == 0:
            return np.ones(len(anomaly_scores)) * 0.75
        
        # Center around median, scale by std
        normalized = (inverted_scores - median_score) / std_score
        prob_0_to_1 = 1 / (1 + np.exp(-2 * normalized))  # Sigmoid with scaling factor 2
        # Scale from [0,1] to [0.5,1]
        prob = 0.5 + 0.5 * prob_0_to_1
        return prob
    
    elif method == 'threshold_based':
        """
        Create probability tiers based on severity thresholds
        """
        inverted_scores = -anomaly_scores
        
        # Define percentile thresholds
        p75 = np.percentile(inverted_scores, 75)  # Top 25% most severe
        p50 = np.percentile(inverted_scores, 50)  # Top 50% most severe
        p25 = np.percentile(inverted_scores, 25)  # Bottom 25% least severe
        
        prob = np.zeros(len(inverted_scores))
        prob[inverted_scores >= p75] = 0.95  # Highest severity (was 0.9)
        prob[(inverted_scores >= p50) & (inverted_scores < p75)] = 0.8   # High severity (was 0.7)
        prob[(inverted_scores >= p25) & (inverted_scores < p50)] = 0.7   # Medium severity (was 0.5)
        prob[inverted_scores < p25] = 0.6    # Lower severity (was 0.3)
        
        return prob