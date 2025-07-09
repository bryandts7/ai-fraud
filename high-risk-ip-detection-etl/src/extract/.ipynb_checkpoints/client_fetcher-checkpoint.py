# src/extract/client_fetcher.py - Updated with ping table functions
import requests
import json
from typing import List, Dict, Any
import logging
from datetime import datetime, timedelta
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
    
    def get_active_clients(self, exclude_test: bool = True, list_of_hour: List[str] = None) -> List[str]:
        """Get list of active clients with existing data"""
        try:
            # Get all clients from API
            raw_clients = self._fetch_client_list(exclude_test)
            self.logger.info(f"Fetched {len(raw_clients)} clients from API")
            
            # Filter to clients with existing data tables
            if list_of_hour:
                active_clients = self._filter_existing_clients(raw_clients, list_of_hour)
                self.logger.info(f"Filtered to {len(active_clients)} clients with data for {list_of_hour[0]} to {list_of_hour[-1]}")
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
    
    def _filter_existing_clients(self, client_list: List[str], list_of_hour: List[str]) -> List[str]:
        """Filter clients to only those with existing data tables for all specified hours."""
        # Import here to avoid circular imports
        from clients.bigquery_client import BigQueryClient

        try:
            bq_client = BigQueryClient(self.config)
            active_clients = []

            project = "pixalate.com"
            dataset = "pixalate"

            # The 'start_date' variable was in the original log message but not defined in the function signature.
            # Assuming it's available in the class instance as self.start_date or similar.
            # For this example, I'll just use the first item in list_of_hour for logging context.
            log_date_identifier = list_of_hour[0] if list_of_hour else "N/A"
            self.logger.debug(f"Checking table existence for {len(client_list)} clients on date {log_date_identifier}")

            for client_id in client_list:
                all_tables_found = True  # Flag to track if all tables exist for the client
                for date_suffix in list_of_hour:
                    table_id = f"{project}:{dataset}.{client_id.upper()}.Pings_{date_suffix}"

                    try:
                        # Check if the specific table exists
                        bq_client.get_table(table_id)
                        self.logger.debug(f"✓ Table exists for client {client_id}: {table_id}")

                    except NotFound:
                        self.logger.debug(f"✗ Client {client_id} table not found: {table_id}. This client will be excluded.")
                        all_tables_found = False
                        break  # Exit the inner loop; no need to check other dates for this client

                    except Exception as e:
                        self.logger.warning(f"Error checking table for client {client_id}: {str(e)}")
                        all_tables_found = False
                        break  # Also exclude client on unexpected errors

                # Only append the client_id if all its tables were successfully found
                if all_tables_found and list_of_hour: # Ensure list_of_hour is not empty
                    active_clients.append(client_id)
                    self.logger.debug(f"✓✓ Client {client_id} has all required tables and has been added to the active list.")

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
            

    def get_event_tables_from_ping(self, client_list: List[str], list_of_hour: List[str]) -> str:
        """
        Returns a UNION ALL of converted Event data from Ping tables for the given clients,
        using the optimized ping-to-event conversion query.
        
        Args:
            client_list: List of client IDs
            list_of_hour: List of hour strings in format 'YYYYMMDD_HH'
            
        Returns:
            str: SQL query string with UNION ALL of ping-to-event conversions
        """
        try:
            if not client_list:
                raise ExtractionError("No clients provided for ping table query")
            
            if not list_of_hour:
                raise ExtractionError("No hours provided for ping table query")
            
            self.logger.info(f"Generating ping-to-event query for {len(client_list)} clients, "
                           f"{len(list_of_hour)} hours")
            
            project = "pixalate.com"
            dataset = "pixalate"
            
            # Convert first and last hour to unix timestamps in milliseconds
            start_hour_str = list_of_hour[0]
            end_hour_str = list_of_hour[-1]
            
            # Parse hour strings and convert to unix timestamps
            start_datetime = datetime.strptime(start_hour_str, '%Y%m%d_%H')
            end_datetime = datetime.strptime(end_hour_str, '%Y%m%d_%H')
            
            # Add 1 hour to end_datetime to make it exclusive (< condition)
            end_datetime = end_datetime + timedelta(hours=1)
            
            # Convert to unix timestamps in milliseconds
            start_timestamp = int(start_datetime.timestamp() * 1000)
            end_timestamp = int(end_datetime.timestamp() * 1000)
            
            self.logger.debug(f"Time range: {start_timestamp} to {end_timestamp} (ms)")
            
            selects = []
            
            for client_id in client_list:
                # Build the ping table union for this client
                ping_tables = [f"`{project}:{dataset}.{client_id.upper()}.Pings_{hour}`" 
                             for hour in list_of_hour]
                ping_union = "\nUNION ALL\nSELECT * FROM ".join(ping_tables)
                ping_union = f"SELECT * FROM {ping_union}"
                
                selects.append(ping_union)
                self.logger.debug(f"Added ping query for client {client_id}")
                
            # Create the conversion query for all client
            union_all = "\nUNION ALL\n".join(selects)
            
            client_query = f"""
                SELECT 
                    SINGLEROW.ip,
                    SINGLEROW.kv18,
                    SINGLEROW.os,
                    SINGLEROW.eventTime,
                    SINGLEROW.kv19,
                    SINGLEROW.kv20,
                    SINGLEROW.kv21,
                    SINGLEROW.kv22,
                    SINGLEROW.visitorId,
                    SINGLEROW.impressions,
                    SINGLEROW.kv4,
                    SINGLEROW.s18,
                    SINGLEROW.s24,
                    SINGLEROW.sessionTime,
                    SINGLEROW.b3,
                    SINGLEROW.deviceType,
                    SINGLEROW.adInstanceTime
                FROM (
                    SELECT 
                        ip,
                        kv18,
                        os,
                        SAFE_CAST(eventTime as INT64) AS eventTime,
                        kv19,
                        kv20,
                        kv21,
                        kv22,
                        visitorId,
                        SAFE_CAST(impressions as INT64) AS impressions,
                        kv4,
                        s18,
                        s24,
                        SAFE_CAST(sessionTime as INT64) AS sessionTime,
                        SAFE_CAST(SAFE_CAST(b3 as INT64) as BOOL) AS b3,
                        deviceType,
                        SAFE_CAST(adInstanceTime as INT64) AS adInstanceTime,
                        -- Keep partition keys for deduplication
                        IFNULL(adInstanceId, "N/A") AS adInstanceId,
                        IFNULL(adInstanceTime, "0") AS adInstanceTime_null,
                        IFNULL(advertiserId, "N/A") AS advertiserId,
                        IFNULL(campaignId, "N/A") AS campaignId,
                        IFNULL(clientId, "N/A") AS clientId,
                        IFNULL(partnerId, "N/A") AS partnerId,
                        IFNULL(placementId, "N/A") AS placementId,
                        IFNULL(sessionId, "N/A") AS sessionId,
                        IFNULL(sessionTime, "0") AS sessionTime_null,
                        IFNULL(visitorId, "N/A") AS visitorId_null,
                        IFNULL(visitorTime, "0") AS visitorTime,
                        ROW_NUMBER() OVER(
                            PARTITION BY 
                                adInstanceId, 
                                adInstanceTime, 
                                advertiserId, 
                                campaignId, 
                                clientId, 
                                partnerId, 
                                placementId, 
                                sessionId, 
                                sessionTime, 
                                visitorId, 
                                visitorTime 
                            ORDER BY eventTime DESC
                        ) AS ranking
                    FROM (
                        {union_all}
                    )
                    WHERE impressions IS NOT NULL
                        AND IFNULL(SAFE_CAST(adInstanceTime AS INT64), 0) >= {start_timestamp}
                        AND IFNULL(SAFE_CAST(adInstanceTime AS INT64), 0) < {end_timestamp}
                ) AS SINGLEROW
                WHERE ranking = 1"""



            self.logger.info(f"Generated ping-to-event UNION query for {len(client_list)} clients")

            return client_query
            
        except Exception as e:
            self.logger.error(f"Failed to generate ping tables query: {str(e)}")
            raise ExtractionError(f"Failed to generate ping tables query: {str(e)}") from e

    def validate_ping_table_access(self, client_id: str, hour_string: str) -> bool:
        """
        Validate access to a specific client's ping table for a given hour
        
        Args:
            client_id: Client identifier
            hour_string: Hour in format 'YYYYMMDD_HH'
            
        Returns:
            bool: True if table exists and is accessible
        """
        try:
            from clients.bigquery_client import BigQueryClient
            
            bq_client = BigQueryClient(self.config)
            
            project = "pixalate.com"
            dataset = "pixalate"
            table_id = f"{project}:{dataset}.{client_id.upper()}.Pings_{hour_string}"
            
            # Check if table exists
            table = bq_client.get_table(table_id)
            
            self.logger.debug(f"✓ Ping table exists: {table_id}")
            return True
            
        except NotFound:
            self.logger.debug(f"✗ Ping table not found: {table_id}")
            return False
        except Exception as e:
            self.logger.warning(f"Error checking ping table {table_id}: {str(e)}")
            return False

    def get_available_ping_hours(self, client_list: List[str], list_of_hour: List[str]) -> Dict[str, List[str]]:
        """
        Get available ping table hours for each client
        
        Args:
            client_list: List of client IDs
            list_of_hour: List of hour strings to check
            
        Returns:
            dict: Mapping of client_id to list of available hours
        """
        try:
            self.logger.info(f"Checking ping table availability for {len(client_list)} clients, "
                           f"{len(list_of_hour)} hours")
            
            available_hours = {}
            
            for client_id in client_list:
                client_hours = []
                for hour_string in list_of_hour:
                    if self.validate_ping_table_access(client_id, hour_string):
                        client_hours.append(hour_string)
                
                available_hours[client_id] = client_hours
                self.logger.debug(f"Client {client_id}: {len(client_hours)}/{len(list_of_hour)} hours available")
            
            # Summary logging
            total_available = sum(len(hours) for hours in available_hours.values())
            total_possible = len(client_list) * len(list_of_hour)
            
            self.logger.info(f"Ping table availability: {total_available}/{total_possible} "
                           f"({total_available/total_possible*100:.1f}%)")
            
            return available_hours
            
        except Exception as e:
            self.logger.error(f"Failed to check ping table availability: {str(e)}")
            raise ExtractionError(f"Failed to check ping table availability: {str(e)}") from e

    def get_filtered_clients_for_ping(self, client_list: List[str], list_of_hour: List[str], 
                                    min_coverage: float = 0.8) -> List[str]:
        """
        Filter clients that have sufficient ping table coverage
        
        Args:
            client_list: List of client IDs to check
            list_of_hour: List of hours to check
            min_coverage: Minimum fraction of hours that must be available (0.0-1.0)
            
        Returns:
            List[str]: Filtered list of clients with sufficient coverage
        """
        try:
            available_hours = self.get_available_ping_hours(client_list, list_of_hour)
            
            filtered_clients = []
            required_hours = int(len(list_of_hour) * min_coverage)
            
            for client_id, hours in available_hours.items():
                coverage = len(hours) / len(list_of_hour)
                if len(hours) >= required_hours:
                    filtered_clients.append(client_id)
                    self.logger.debug(f"✓ Client {client_id}: {coverage:.1%} coverage (accepted)")
                else:
                    self.logger.debug(f"✗ Client {client_id}: {coverage:.1%} coverage (rejected)")
            
            self.logger.info(f"Filtered clients for ping tables: {len(filtered_clients)}/{len(client_list)} "
                           f"clients with ≥{min_coverage:.0%} coverage")
            
            return filtered_clients
            
        except Exception as e:
            self.logger.error(f"Failed to filter clients for ping: {str(e)}")
            raise ExtractionError(f"Failed to filter clients for ping: {str(e)}") from e