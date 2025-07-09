# src/transform/base_transformer.py
from abc import ABC, abstractmethod
import pandas as pd

class BaseTransformer(ABC):
    """Base interface for data transformation"""
    
    def __init__(self, config):
        self.config = config
    
    @abstractmethod
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform data"""
        pass