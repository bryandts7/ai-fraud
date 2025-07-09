# src/load/base_loader.py
from abc import ABC, abstractmethod
from typing import Dict, Any
import pandas as pd

class BaseLoader(ABC):
    """Base interface for data loading"""
    
    def __init__(self, config):
        self.config = config
    
    @abstractmethod
    def load(self, data: pd.DataFrame, context: Dict[str, Any]) -> str:
        """Load data to destination"""
        pass