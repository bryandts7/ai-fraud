# src/transform/feature_engineer.py - Fixed implementation
import pandas as pd
import logging
from typing import List
from .base_transformer import BaseTransformer
from core.exceptions import TransformationError

class FeatureEngineer(BaseTransformer):
    """Handle feature engineering and data preparation"""
    
    def __init__(self, config):
        super().__init__(config)
        self.columns_to_keep = config.get('feature_engineering', {}).get('columns_to_stay', [])
        self.logger = logging.getLogger(__name__)
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform interface implementation - required by BaseTransformer"""
        return self.prepare_features(data)
    
    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare features for anomaly detection"""
        try:
            self.logger.info(f"Starting feature cleaning with {len(df)} rows")
            
            # Select relevant columns
            if self.columns_to_keep:
                available_columns = [col for col in self.columns_to_keep if col in df.columns]
                missing_columns = [col for col in self.columns_to_keep if col not in df.columns]
                
                if missing_columns:
                    self.logger.warning(f"Missing columns: {missing_columns}")
                
                if not available_columns:
                    raise TransformationError("No required columns found in data")
                
                self.logger.info(f"Using columns: {available_columns}")
                df_clean = df[available_columns].copy()
            else:
                df_clean = df.copy()
            
            # Remove rows with missing critical data
            initial_rows = len(df_clean)
            df_clean = df_clean.dropna()
            final_rows = len(df_clean)
            
            if final_rows == 0:
                raise TransformationError("No data remaining after cleaning")
            
            self.logger.info(f"Feature Cleaning: {initial_rows} -> {final_rows} rows after cleaning")
            
            return df_clean
            
        except Exception as e:
            self.logger.error(f"Feature preparation failed: {str(e)}")
            raise TransformationError(f"Feature preparation failed: {str(e)}") from e