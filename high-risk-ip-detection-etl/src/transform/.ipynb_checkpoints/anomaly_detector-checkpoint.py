# src/transform/anomaly_detector.py - Modified implementation
import pandas as pd
import numpy as np
import logging
from sklearn.ensemble import IsolationForest
from typing import Dict, Any, Tuple

from .base_transformer import BaseTransformer
from core.exceptions import TransformationError


class AnomalyDetector(BaseTransformer):
    """Detect anomalies using machine learning"""
    
    def __init__(self, config):
        super().__init__(config)
        self.contamination = config.get('model', {}).get('contamination', 0.02)
        self.random_state = config.get('model', {}).get('random_state', 42)
        self.n_jobs = config.get('model', {}).get('n_jobs', -1)
        self.logger = logging.getLogger(__name__)
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform interface implementation - returns full data with anomaly info"""
        anomalies, full_data = self.detect_anomalies(data)
        return anomalies
    
    def get_full_result(self, data: pd.DataFrame) -> pd.DataFrame:
        """Get only the anomalies from the data"""
        anomalies, full_data = self.detect_anomalies(data)
        return full_data
    
    def detect_anomalies(self, features_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Detect anomalies in the feature data and return both anomalies and full data"""
        try:
            self.logger.info(f"Starting anomaly detection with {len(features_df)} rows")
            
            # Prepare features for ML model
            X, feature_cols = self._prepare_features(features_df)
            
            # Train isolation forest
            self.logger.info(f"Training Isolation Forest with contamination={self.contamination}")
            model = IsolationForest(
                contamination=self.contamination,
                random_state=self.random_state,
                n_jobs=self.n_jobs
            )
            
            # Predict anomalies
            anomaly_labels = model.fit_predict(X)
            anomaly_scores = model.decision_function(X)
            
            # Build results dataframe
            results_df = features_df.copy()
            results_df['is_anomaly'] = anomaly_labels == -1
            results_df['anomaly_score'] = anomaly_scores
            
            # Add evidence for both outliers and inliers
            results_df = self._add_evidence_explanations(
                results_df, X, feature_cols, anomaly_labels == -1
            )
            
            # Add probabilities for all data points
            results_df['probability'] = self._convert_scores_to_probabilities(
                results_df['anomaly_score']
            )
            
            # Create full data with evidence
            full_data = results_df[['IP', 'evidence', 'probability', 'is_anomaly']].copy()
            
            # Filter to anomalies only
            anomalies = results_df[results_df['is_anomaly']].copy()
            
            if len(anomalies) == 0:
                self.logger.warning("No anomalies detected!")
                # Return empty DataFrame with required columns
                final_anomalies = pd.DataFrame(columns=['IP', 'evidence', 'probability'])
            else:
                # Select final columns for anomalies
                final_anomalies = anomalies[['IP', 'evidence', 'probability']].copy()
            
            self.logger.info(f"Detected {len(final_anomalies)} anomalies out of {len(full_data)} total records")
            
            return final_anomalies, full_data
            
        except Exception as e:
            self.logger.error(f"Anomaly detection failed: {str(e)}")
            raise TransformationError(f"Anomaly detection failed: {str(e)}") from e
    
    def _prepare_features(self, df: pd.DataFrame) -> tuple:
        """Prepare features for ML model"""
        feature_cols = [col for col in df.columns if col != 'IP']
        X = df[feature_cols].fillna(0).values
        X = np.nan_to_num(X)
        
        self.logger.info(f"Prepared {X.shape[0]} samples with {X.shape[1]} features")
        return X, feature_cols
    
    def _add_evidence_explanations(self, results_df: pd.DataFrame, 
                                 X: np.ndarray, feature_cols: list, 
                                 anomaly_mask: np.ndarray) -> pd.DataFrame:
        """Add evidence explanations for both outliers and inliers"""
        results_df['evidence'] = 'Normal'
        
        # Calculate normal data statistics
        normal_data = X[~anomaly_mask]
        if len(normal_data) == 0:
            # If all data is anomalous, use global statistics
            normal_mean = np.mean(X, axis=0)
            normal_std = np.std(X.astype(np.float64), axis=0) + 1e-5
        else:
            normal_mean = np.mean(normal_data, axis=0)
            normal_std = np.std(normal_data.astype(np.float64), axis=0) + 1e-5
        
        # Generate evidence for all data points
        for idx in range(len(results_df)):
            if idx < len(X):
                actual_values = X[idx]
                deviations = np.abs((actual_values - normal_mean) / normal_std)
                top_feature_indices = np.argsort(deviations)[-3:][::-1]
                
                if anomaly_mask[idx]:
                    # Evidence for outliers (why they are anomalous)
                    evidence_details = []
                    for feat_idx in top_feature_indices:
                        feat_name = feature_cols[feat_idx]
                        actual_val = actual_values[feat_idx]
                        normal_val = normal_mean[feat_idx]
                        deviation = deviations[feat_idx]
                        evidence_details.append(f"{feat_name}: {actual_val:.2f} vs normal {normal_val:.2f} (deviation: {deviation:.2f})")
                    
                    results_df.iloc[idx, results_df.columns.get_loc('evidence')] = \
                        'Outlier - ' + ', '.join(evidence_details)
                else:
                    # Evidence for inliers (why they are normal)
                    evidence_details = []
                    for feat_idx in top_feature_indices:
                        feat_name = feature_cols[feat_idx]
                        actual_val = actual_values[feat_idx]
                        normal_val = normal_mean[feat_idx]
                        deviation = deviations[feat_idx]
                        evidence_details.append(f"{feat_name}: {actual_val:.2f} close to normal {normal_val:.2f} (deviation: {deviation:.2f})")
                    
                    results_df.iloc[idx, results_df.columns.get_loc('evidence')] = \
                        'Inlier - ' + ', '.join(evidence_details)
        
        self.logger.info(f"Generated evidence for {len(results_df)} data points")
        return results_df
    
    def _convert_scores_to_probabilities(self, anomaly_scores: pd.Series) -> pd.Series:
        """Convert isolation forest scores to probabilities [0.5, 1] for anomalies, [0, 0.5] for normal"""
        inverted_scores = -anomaly_scores
        min_score = inverted_scores.min()
        max_score = inverted_scores.max()
        
        if max_score == min_score:
            return pd.Series(np.ones(len(anomaly_scores)) * 0.5, index=anomaly_scores.index)
        
        # Normalize to [0, 1]
        prob_0_to_1 = (inverted_scores - min_score) / (max_score - min_score)
        
        # For anomalies (negative scores), map to [0.5, 1]
        # For normal points (positive scores), map to [0, 0.5]
        prob = np.where(anomaly_scores < 0, 
                       0.5 + 0.5 * prob_0_to_1,  # Anomalies: [0.5, 1]
                       0.5 * prob_0_to_1)        # Normal: [0, 0.5]
        
        return pd.Series(prob, index=anomaly_scores.index)