# src/extract/client_fetcher.py
import requests
import json
from typing import List, Dict, Any
import logging
from google.cloud.exceptions import NotFound

from core.exceptions import ExtractionError

class ClientFetcher:
    """Fetch and manage client information"""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.client_api_url = "http://pipeline-xlarge-aggregator.us-east-1.elasticbeanstalk.com/pixalate/v1/aggregator/health/clients"
        self.auth_header = {'authorization': 'Basic:cGl4ZWxz'}
        
        # Default excluded test clients
        self.excluded_test_clients = {
            "iab", "px", "ftvf", "tht", "lkda", "ainf", "test", 
            "pingdom", "pixalate", "mf", "tw", "xad", "nad", "pkt", "we"
        }
    
    def get_active_clients(self, exclude_test: bool = True, start_date: str = None) -> List[str]:
        """Get list of active clients with existing data"""
        try:
            # Get all clients from API
            raw_clients = self._fetch_client_list(exclude_test)
            self.logger.info(f"Fetched {len(raw_clients)} clients from API")
            
            # Filter to clients with existing data tables
            if start_date:
                active_clients = self._filter_existing_clients(raw_clients, start_date)
                self.logger.info(f"Filtered to {len(active_clients)} clients with data for {start_date}")
            else:
                active_clients = raw_clients
                self.logger.info(f"Using all {len(active_clients)} clients (no date filter)")
            
            return active_clients
            
        except Exception as e:
            self.logger.error(f"Failed to get active clients: {str(e)}")
            raise ExtractionError(f"Failed to get active clients: {str(e)}") from e
    
    def _fetch_client_list(self, exclude_test: bool) -> List[str]:
        """Fetch client list from API"""
        try:
            self.logger.debug(f"Fetching client list from API: {self.client_api_url}")
            
            response = requests.get(self.client_api_url, headers=self.auth_header, timeout=30)
            response.raise_for_status()
            
            clients_data = json.loads(response.text)
            
            if not isinstance(clients_data, list):
                raise ExtractionError(f"Expected list from API, got {type(clients_data)}")
            
            # Convert to uppercase strings
            clients = [str(client).upper() for client in clients_data]
            
            # Filter out test clients if requested
            if exclude_test:
                clients = [client for client in clients if client.lower() not in self.excluded_test_clients]
                self.logger.debug(f"Excluded test clients, remaining: {len(clients)}")
            
            return clients
            
        except requests.RequestException as e:
            raise ExtractionError(f"Failed to fetch client list from API: {str(e)}") from e
        except json.JSONDecodeError as e:
            raise ExtractionError(f"Failed to parse client list JSON: {str(e)}") from e
    
    def _filter_existing_clients(self, client_list: List[str], start_date: str) -> List[str]:
        """Filter clients to only those with existing data tables"""
        # Import here to avoid circular imports
        from clients.bigquery_client import BigQueryClient
        
        try:
            bq_client = BigQueryClient(self.config)
            date_suffix = start_date.replace("-", "")
            active_clients = []
            
            project = "pixalate.com"
            dataset = "pixalate"
            
            self.logger.debug(f"Checking table existence for {len(client_list)} clients on date {start_date}")
            
            for client_id in client_list:
                table_id = f"{project}:{dataset}.{client_id.upper()}.Event_{date_suffix}"
                
                try:
                    # Check if table exists
                    table = bq_client.get_table(table_id)
                    active_clients.append(client_id)
                    
                    # Optional: Check if table has data
                    # if self._table_has_data(bq_client, table_id):
                    #     active_clients.append(client_id)
                    #     self.logger.debug(f"✓ Client {client_id} has data table: {table_id}")
                    # else:
                    #     self.logger.debug(f"✗ Client {client_id} table exists but is empty: {table_id}")
                        
                except NotFound:
                    self.logger.debug(f"✗ Client {client_id} table not found: {table_id}")
                    continue
                except Exception as e:
                    self.logger.warning(f"Error checking table for client {client_id}: {str(e)}")
                    continue
            
            return active_clients
            
        except Exception as e:
            self.logger.error(f"Error filtering existing clients: {str(e)}")
            raise ExtractionError(f"Failed to filter existing clients: {str(e)}") from e
    
    def _table_has_data(self, bq_client, table_id: str, min_rows: int = 1000) -> bool:
        """Check if a table has sufficient data"""
        try:
            # Quick row count check
            count_query = f"SELECT COUNT(*) as row_count FROM `{table_id}` LIMIT 1"
            
            # Use a simple query to check row count
            job_config = bq_client.pull_client.query(count_query)
            result = job_config.result()
            
            row_count = list(result)[0].row_count
            
            return row_count >= min_rows
            
        except Exception as e:
            self.logger.warning(f"Could not check data volume for {table_id}: {str(e)}")
            # If we can't check, assume it has data
            return True
        
    def get_event_tables(self, client_list: List[str], start_date: str, 
                        start_hour: str, end_hour: str) -> str:
        """Generate UNION ALL query for event tables"""
        try:
            if not client_list:
                raise ExtractionError("No clients provided for event table query")
            
            date_suffix = start_date.replace('-', '')
            project = "pixalate.com"
            dataset = "pixalate"
            
            # Format time filters
            hour1 = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]} {start_hour}:00:00"
            hour2 = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]} {end_hour}:00:00"
            
            selects = []
            for client_id in client_list:
                select_query = f"""
        SELECT 
            ip, 
            kv18, 
            os, 
            eventTime, 
            kv19, 
            kv20, 
            kv21, 
            kv22, 
            visitorId, 
            impressions, 
            kv4, 
            s18, 
            s24, 
            sessionTime, 
            b3, 
            deviceType, 
            adInstanceTime 
        FROM `{project}:{dataset}.{client_id.upper()}.Event_{date_suffix}`
        WHERE FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', 
                              TIMESTAMP_TRUNC(TIMESTAMP_MICROS(CAST(eventTime AS INT64) * 1000), HOUR, 'UTC')) >= "{hour1}"
        AND FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', 
                            TIMESTAMP_TRUNC(TIMESTAMP_MICROS(CAST(eventTime AS INT64) * 1000), HOUR, 'UTC')) < "{hour2}"
        AND kv18 IS NOT NULL
        AND deviceType LIKE '%mobile%'"""
                selects.append(select_query)
            
            union_query = "\nUNION ALL\n".join(selects)
            
            self.logger.info(f"Generated UNION query for {len(client_list)} clients, "
                           f"time range: {hour1} to {hour2}")
            
            return union_query
            
        except Exception as e:
            self.logger.error(f"Failed to generate event tables query: {str(e)}")
            raise ExtractionError(f"Failed to generate event tables query: {str(e)}") from e
