from abc import ABC, abstractmethod
from typing import Dict, Any
import pandas as pd


class BaseExtractor(ABC):
    """Base interface for data extraction"""
    
    def __init__(self, config):
        self.config = config
    
    @abstractmethod
    def extract_features(self, context: Dict[str, Any]) -> pd.DataFrame:
        """Extract features based on context"""
        pass
    
    def validate_extraction(self, data: pd.DataFrame) -> bool:
        """Validate extracted data - default implementation"""
        if data.empty:
            return False
        
        # Basic validation - can be overridden
        return True