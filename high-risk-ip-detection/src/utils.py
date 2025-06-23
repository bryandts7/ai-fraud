from pathlib import Path
from typing import Any, Dict, Optional
import pandas as pd


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
