import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from .exceptions import ConfigurationError


class ConfigManager:
    """Centralized configuration management"""
    
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self._config = self._load_config()
        self._validate_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if not self.config_path.exists():
            raise ConfigurationError(f"Config file not found: {self.config_path}")
        
        try:
            with self.config_path.open('r') as f:
                return yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in config file: {e}") from e
    
    def _validate_config(self):
        """Validate required configuration fields"""
        required_fields = [
            'project.id',
            'project_pull.id',
            'bigquery.destination_dataset'
        ]
        
        for field in required_fields:
            if not self._get_nested_value(field):
                raise ConfigurationError(f"Missing required config field: {field}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value with dot notation support"""
        return self._get_nested_value(key) or default
    
    def _get_nested_value(self, key: str) -> Optional[Any]:
        """Get nested dictionary value using dot notation"""
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return None
        
        return value
    
    @property
    def raw_config(self) -> Dict[str, Any]:
        """Get raw configuration dictionary"""
        return self._config.copy()