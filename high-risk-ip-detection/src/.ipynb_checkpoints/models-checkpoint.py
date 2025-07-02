import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import time

class GraphAnomalyDetector:
    def __init__(self, appId_IP_df, use_gpu=True):
        self.appId_IP_df = appId_IP_df
        self.use_gpu = use_gpu
        self.results = {}
        
        # Initialize GPU if available
        if self.use_gpu:
            pass

    @timer
    def create_ip_appid_edges(self):
        appid_IP_features = self.appId_IP_df.copy()
        
        # Fill NaN values 
        appid_IP_features = appid_IP_features.fillna(0)
        
        # Drop non-numeric columns for anomaly detection
        feature_cols = appid_IP_features.drop(['IP'], axis=1).columns
        
        return appid_IP_features, feature_cols
        
    
    @timer
    def create_deviceid_appid_edges(self):
        """Create features for deviceId-appId edges"""
        pass
    
    @timer
    def detect_anomalies(self, features_df, feature_cols, anomaly_type, contamination=0.3):
        """Detect anomalies using Isolation Forest and LOF"""
        # Extract features for anomaly detection
        X = features_df[feature_cols].fillna(0).values
        
        # Handle sparse data
        X = np.nan_to_num(X)
        
        # scaler = StandardScaler()
        # X = scaler.fit_transform(X)
        
        # Isolation Forest
        iso_forest = IsolationForest(contamination=0.02, random_state=42, n_jobs=-1)
        iso_forest_scores = iso_forest.fit_predict(X)
        iso_forest_anomalies = iso_forest_scores == -1
        

        # Combine results (an edge is anomalous if both methods flag it)
        combined_anomalies = iso_forest_anomalies # & lof_anomalies
        
        # Store results
        result_df = features_df.copy()
        result_df['is_anomaly'] = combined_anomalies
        result_df['anomaly_score'] = iso_forest.decision_function(X)

        
        # For interpretability, calculate feature importance
        if np.any(combined_anomalies):
            normal_data = X[~combined_anomalies]
            anomaly_data = X[combined_anomalies]
            
            # Calculate z-scores for each feature in anomalies
            normal_mean = np.mean(normal_data, axis=0)
            normal_std = np.std(normal_data.astype(np.float64), axis=0) + 1e-5
            z_scores = np.abs((anomaly_data - normal_mean) / normal_std)
            
            # Average z-scores across anomalies
            feature_importance = np.mean(z_scores, axis=0)
            
            # Map feature importance back to feature names
            importance_dict = dict(zip(feature_cols, feature_importance))
            top_features = sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)[:5]
            
            # Add top anomalous features for each anomaly
            # result_df['anomaly_reason'] = 'Normal'
            # for idx in np.where(combined_anomalies)[0]:
            #     if idx < len(result_df):
            #         anomaly_z_scores = np.abs((X[idx] - normal_mean) / normal_std)
            #         top_anomaly_features = sorted(zip(feature_cols, anomaly_z_scores), key=lambda x: x[1], reverse=True)[:3]
            #         result_df.iloc[idx, result_df.columns.get_loc('anomaly_reason')] = ', '.join([f"{feat}: {score:.2f}σ" for feat, score in top_anomaly_features])
            
            result_df['anomaly_reason'] = 'Normal'
            for idx in np.where(combined_anomalies)[0]:
                if idx < len(result_df):
                    actual_values = X[idx]
                    top_anomaly_indices = np.argsort(np.abs((actual_values - normal_mean) / normal_std))[-3:][::-1]

                    anomaly_details = []
                    for feat_idx in top_anomaly_indices:
                        feat_name = feature_cols[feat_idx]
                        actual_val = actual_values[feat_idx]
                        normal_val = normal_mean[feat_idx]
                        anomaly_details.append(f"{feat_name}: {actual_val:.2f} vs {normal_val:.2f}")

                    result_df.iloc[idx, result_df.columns.get_loc('anomaly_reason')] = ', '.join(anomaly_details)
        else:
            result_df['anomaly_reason'] = 'Normal'
        
        print(f"Found {sum(combined_anomalies)} anomalies out of {len(features_df)} {anomaly_type}")
        return result_df, iso_forest
    

    @timer
    def run(self):
        """Run the entire anomaly detection pipeline"""
        # Create edge features
        ip_appid_features, ip_appid_cols = self.create_ip_appid_edges()
        # deviceid_appid_features, deviceid_appid_cols = self.create_deviceid_appid_edges()
        
        # Detect anomalies
        ip_appid_results, model = self.detect_anomalies(ip_appid_features, ip_appid_cols, "IP-appId edges")
        
        # Store results
        self.results = {
            'ip_anomalies_result': ip_appid_results,
            'ip_anomalies_model': model
        }
        
        return self.results