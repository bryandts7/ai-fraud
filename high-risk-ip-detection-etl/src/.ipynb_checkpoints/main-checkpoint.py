#!/usr/bin/env python3
"""
High-Risk IP Detection ETL Pipeline
Main orchestrator for the ETL process
"""
import argparse
import sys
import os
from datetime import datetime
from typing import Dict, Any
from pathlib import Path

from core.config_manager import ConfigManager
from core.logger import setup_simple_logging  
from core.exceptions import PipelineError
from extract.bigquery_extractor import BigQueryExtractor
from extract.client_fetcher import ClientFetcher
from transform.feature_engineer import FeatureEngineer
from transform.anomaly_detector import AnomalyDetector
from load.csv_loader import CSVLoader
from load.bigquery_loader import BigQueryLoader

from core.utils import generate_list_of_hour

class ETLPipeline:
    """Main ETL Pipeline orchestrator"""
    
    def __init__(self, config_path: str, log_level: str = "INFO"):
        # Always use simple logging
        self.logger = setup_simple_logging(log_level)
        
        try:
            self.config = ConfigManager(config_path)
            self.logger.info(f"Configuration loaded from: {config_path}")
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            raise
        
        # Ensure output directories exist
        self._ensure_directories()
        
    def _ensure_directories(self):
        """Ensure required directories exist"""
        directories = [
            'output',
            'logs',
            self.config.get('client', {}).get('csv_folder', 'output')
        ]
        
        for directory in directories:
            try:
                Path(directory).mkdir(parents=True, exist_ok=True)
                self.logger.debug(f"Created/verified directory: {directory}")
            except Exception as e:
                self.logger.warning(f"Could not create directory {directory}: {e}")
        
    def extract(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Extract phase: Get data from BigQuery"""
        self.logger.info("Starting extraction phase")
        
        try:
            # Get client list
            client_fetcher = ClientFetcher(self.config.raw_config)
            raw_clients = client_fetcher.get_active_clients(
                exclude_test=True,
                list_of_hour=context['list_of_hour']
            )
            context['active_clients'] = raw_clients
            self.logger.info(f"Found {len(raw_clients)} active clients")
            
            # Extract features from BigQuery
            extractor = BigQueryExtractor(self.config.raw_config)
            
            intermediary_table = extractor.extract_intermediaries(context)
            context['event_from_ping_table'] = intermediary_table
            self.logger.info("Finishing extract intermediaries table. Starting Feature Engineering Extraction.")
            
            raw_data = extractor.extract_features(context)
            
            self.logger.info(f"Extracted {len(raw_data)} records")
            return {'raw_data': raw_data, 'context': context}
            
        except Exception as e:
            self.logger.error(f"Extraction phase failed: {e}")
            raise
    
    def transform(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform phase: Feature engineering and anomaly detection"""
        self.logger.info("Starting transformation phase")
        
        try:
            raw_data = extracted_data['raw_data']
            
            # Feature engineering
            feature_engineer = FeatureEngineer(self.config.raw_config)
            clean_features = feature_engineer.prepare_features(raw_data)
            self.logger.info(f"Prepared {len(clean_features)} features")
            
            # Anomaly detection
            anomaly_detector = AnomalyDetector(self.config.raw_config)
            anomalies, full_data = anomaly_detector.detect_anomalies(clean_features)
            
            self.logger.info(f"Detected {len(anomalies)} anomalies")
            return {
                'anomalies': anomalies,
                'full_data': full_data,
                'context': extracted_data['context']
            }
            
        except Exception as e:
            self.logger.error(f"Transformation phase failed: {e}")
            raise
    
    def load(self, transformed_data: Dict[str, Any]) -> Dict[str, str]:
        """Load phase: Save results to CSV and BigQuery"""
        self.logger.info("Starting load phase")
        
        try:
            anomalies = transformed_data['anomalies']
            context = transformed_data['context']
            full_data = transformed_data['full_data']
            
            results = {}
            
            # Load to CSV
            csv_loader = CSVLoader(self.config.raw_config)
            csv_path = csv_loader.save_anomalies(anomalies, context)
            csv_full_data_path = csv_loader.save_full_data(full_data, context)
            
            results['csv_path'] = csv_path
            results['csv_full_data_path'] = csv_full_data_path
            self.logger.info(f"CSV saved to: {csv_path}")
            self.logger.info(f"CSV Full Data saved to: {csv_full_data_path}")
            
            # Load to BigQuery
            bq_loader = BigQueryLoader(self.config.raw_config)
            table_name = bq_loader.save_anomalies(anomalies, context)
            table_full_data_name = bq_loader.save_full_data(full_data, context)
            
            results['bq_table'] = table_name
            results['bq_table_full_data'] = table_full_data_name
            self.logger.info(f"BigQuery table: {table_name}")
            self.logger.info(f"BigQuery Full Data table: {table_full_data_name}")
            
            self.logger.info("Load phase completed successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"Load phase failed: {e}")
            raise
    
    def run(self, start_date: str = None) -> Dict[str, str]:
        """Run the complete ETL pipeline"""
        start_time = datetime.now()
        
        try:
            self.logger.info("="*60)
            self.logger.info("ETL Pipeline Starting")
            self.logger.info(f"Start Date: {start_date or 'auto (yesterday)'}")
            self.logger.info("="*60)
            
            # Prepare context
            context = self._prepare_context(start_date)
            self.logger.info(f"Context prepared: {context}")
            
            # Execute ETL phases
            extracted_data = self.extract(context)
            transformed_data = self.transform(extracted_data)
            results = self.load(transformed_data)
            
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.info("="*60)
            self.logger.info(f"ETL Pipeline Completed Successfully in {duration:.2f} seconds")
            self.logger.info(f"Results: {results}")
            self.logger.info("="*60)
            
            return results
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error("="*60)
            self.logger.error(f"ETL Pipeline Failed after {duration:.2f} seconds")
            self.logger.error(f"Error: {str(e)}")
            self.logger.error("="*60)
            raise PipelineError(f"ETL Pipeline failed: {str(e)}") from e
    
    def _prepare_context(self, start_date: str = None) -> Dict[str, Any]:
        """Prepare execution context"""
        dates_config = self.config.get('dates', {})
        
        if start_date is None:
            from datetime import datetime, timedelta
            today = datetime.utcnow() 
            start_date = today.strftime(dates_config.get('date_format', '%Y%m%d'))
            
            delay_1_hour = today - timedelta(hours=1)
            end_hour = delay_1_hour.strftime('%Y%m%d_%H')
            lookback_period = dates_config.get('lookback_period', 4)
            list_of_hour = generate_list_of_hour(end_hour, lookback_period)
            start_hour = list_of_hour[0]
        
        # Convert date format if needed (remove dashes)
        if '-' in start_date:
            start_date = start_date.replace('-', '')
        
        context = {
            'start_date': start_date,
            # 'start_hour': dates_config.get('start_hour', '04'),
            # 'end_hour': dates_config.get('end_hour', '08'),
            'start_hour': start_hour,
            'end_hour': end_hour,
            'list_of_hour': list_of_hour,
            'client_name': self.config.get('client', {}).get('name', 'all_clients'),
            'intermediary_table_name': self.config.get('naming', {}).get('intermediary_table_name', 'ping_to_event_{client_name}_{start_hour}_{end_hour}')
        }
        
        return context


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="High-Risk IP Detection ETL Pipeline")
    parser.add_argument(
        "--config", 
        default="config/config.yaml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--start-date", 
        help="Start date in YYYYMMDD format"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Validate configuration without running pipeline"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level"
    )
    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_args()
    
    try:
        pipeline = ETLPipeline(args.config, args.log_level)
        
        if args.dry_run:
            pipeline.logger.info("Dry run mode - validating configuration")
            pipeline.logger.info("Configuration validation successful")
            return 0
        
        results = pipeline.run(args.start_date)
        pipeline.logger.info("Pipeline execution completed successfully")
        return 0
        
    except Exception as e:
        # Final fallback logging
        print(f"ERROR: Pipeline failed: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
