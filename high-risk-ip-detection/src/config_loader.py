import os
from pathlib import Path
from typing import Any, Dict

import yaml

class ConfigError(Exception):
    """Raised when the config is invalid or missing required fields."""


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        raise ConfigError(f"Config file not found: {path}")
    with path.open("r") as f:
        return yaml.safe_load(f)


def load_config(
    config_path: str = "config/config.yaml",
) -> Dict[str, Any]:
    """
    Load and merge main config and guardrails.

    Returns a dict with two top-level keys:
      - 'config': your config.yaml contents
    """
    cfg_path = Path(config_path)

    config = _load_yaml(cfg_path)

    # Basic validation
    if "project" not in config or "id" not in config["project"]:
        raise ConfigError("Missing required 'project.id' in config.yaml")


    # Merge into one dict
    return {
        "config": config,
    }
