import duckdb
import numpy as np
import pandas as pd
import re
from typing import Dict, List, Any, Optional
from core.logger import AuditLogger
from db.connections import SnowflakeConnection, DuckDBConnection
from db.warehouse_manager import WarehouseManager
from config.config_loader import Config
from config.schema_loader import SchemaLoader
from datetime import datetime
from processing.monitoring.metrics_tracker import MetricsTracker


class DuplicateChecker:
    """
    Checks for duplicate records between streaming DataFrame and existing Snowflake DWH
    Prevents duplicate inserts by identifying records that already exist in production
    """

    # Fact tables that need duplicate checking (streaming data)
    FACT_TABLES = {
        'orders': 'fact_orders',
        'tickets': 'fact_tickets',
        'ticket_events': 'fact_ticket_events'
    }

    # Dimension tables don't need duplicate checking (they are overwritten)
    DIMENSION_TABLES = {
        'customers', 'drivers', 'restaurants', 'agents', 'cities',
        'regions', 'reasons', 'categories', 'segments', 'teams',
        'channels', 'priorities', 'reason_categories'
    }

    def __init__(self, metrics_tracker: MetricsTracker, duckdb: DuckDBConnection,
                 database="FASTFEASTDWH", schema="SILVER"):
        """Initialize with Snowflake connection (production DWH)"""
        self.logger = AuditLogger()
        self.config = Config()
        self.schema_loader = SchemaLoader(self.config.req_cols_path())
        self.metrics_tracker = metrics_tracker

        # Connect to Snowflake (production DWH)
        self.snowflake = SnowflakeConnection()
        self.database = database
        self.schema = schema

        # DuckDB for local processing
        self.duckdb = duckdb.conn

        # Initialize warehouse manager
        self.warehouse_manager = WarehouseManager(self.snowflake.conn, "COMPUTE_WH")

        # Quality metrics
        self.quality_metrics = {
            'total_records_checked': 0,
            'duplicates_found': 0,
            'duplicate_rate': 0.0,
            'new_records': 0,
            'check_timestamp': None,
            'table_checked': None
        }

    def _needs_duplicate_check(self, table_name: str) -> bool:
        """Check if table needs duplicate checking (only fact tables)."""
        return table_name in self.FACT_TABLES

    def _get_target_table_name(self, table_name: str) -> str:
        """Get the target table name in Snowflake."""
        if table_name in self.FACT_TABLES:
            return f"{self.database}.{self.schema}.{self.FACT_TABLES[table_name]}"
        elif table_name in self.DIMENSION_TABLES:
            return f"{self.database}.{self.schema}.dim_{table_name}"
        else:
            return f"{self.database}.{self.schema}.{table_name}"

    def _fetch_existing_keys_from_snowflake(self, table_name: str, primary_keys: List[str],
                                            temp_table: str) -> pd.DataFrame:
        """
        Fetch existing primary keys from Snowflake fact table.
        Uses warehouse manager to ensure warehouse is running.
        """
        target_table = self._get_target_table_name(table_name)
        pk_columns = ", ".join([f'"{pk}"' for pk in primary_keys])

        # Get distinct key combinations from incoming data
        distinct_keys_query = f"""
            SELECT DISTINCT {pk_columns}
            FROM {temp_table}
        """
        distinct_keys = self.duckdb.execute(distinct_keys_query).fetchdf()

        if len(distinct_keys) == 0:
            return pd.DataFrame()

        # Build WHERE clause for Snowflake query
        where_conditions = []
        for pk in primary_keys:
            values = distinct_keys[pk].dropna().unique().tolist()
            if values:
                escaped_values = [str(v).replace("'", "''") for v in values] #This handles values that contain apostrophes
                values_str = ", ".join([f"'{v}'" for v in escaped_values])
                where_conditions.append(f'"{pk}" IN ({values_str})')

        if not where_conditions:
            return pd.DataFrame()

        where_clause = " AND ".join(where_conditions)
        snowflake_query = f"""
            SELECT {pk_columns}
            FROM {target_table}
            WHERE {where_clause}
        """

        try:
            self.logger.log_msg(f"Querying Snowflake for existing keys in {table_name}")

            # Use warehouse manager to ensure warehouse is running
            with self.warehouse_manager.auto_manage():
                cursor = self.snowflake.conn.cursor()
                cursor.execute(snowflake_query)
                result = cursor.fetch_pandas_all()
                cursor.close()

            self.logger.log_msg(f"Found {len(result)} existing records in Snowflake")
            return result
        except Exception as e:
            self.logger.log_warning(f"Could not query Snowflake: {e}")
            return pd.DataFrame()

    def check_duplicates(self, relation, table_name: str, batch_id: str = None) -> Dict[str, Any]:
        """
        Check for duplicates between incoming data and Snowflake DWH.
        Only checks fact tables; dimension tables skip duplicate checking.
        """
        self.quality_metrics['check_timestamp'] = datetime.now()
        self.quality_metrics['table_checked'] = table_name

        # Skip duplicate check for dimension tables
        if not self._needs_duplicate_check(table_name):
            self.logger.log_msg(f"Skipping duplicate check for dimension table: {table_name}")
            return {
                'unique_relation': relation,
                'duplicate_relation': None,
                'metrics': {'skipped': True, 'reason': 'dimension_table'},
                'warning': 'Duplicate check skipped for dimension table'
            }

        # Convert input to DataFrame for processing
        if hasattr(relation, 'df'):
            input_df = relation.df()
        elif isinstance(relation, pd.DataFrame):
            input_df = relation
        else:
            input_df = pd.DataFrame(relation)

        if len(input_df) == 0:
            self.logger.log_warning(f"No records to check for {table_name}")
            return {
                'unique_relation': self.duckdb.from_df(input_df),
                'duplicate_relation': None,
                'metrics': self.quality_metrics,
                'warning': 'Empty DataFrame'
            }

        self.quality_metrics['total_records_checked'] = len(input_df)

        # Get primary keys for the table
        schema = self.schema_loader.tables.get(table_name, {})
        primary_keys = schema.get('primary_keys', [])

        if not primary_keys:
            self.logger.log_err(f"No primary keys defined for {table_name} in schema.yaml")
            return {
                'unique_relation': relation,
                'duplicate_relation': None,
                'metrics': self.quality_metrics,
                'error': f'No primary keys defined for {table_name}'
            }

        try:
            # Register input_df as temp table for Snowflake query
            temp_table = f"temp_{table_name}_{int(datetime.now().timestamp())}"
            self.duckdb.register(temp_table, input_df)

            # Fetch existing keys from Snowflake (warehouse managed inside)
            existing_keys_df = self._fetch_existing_keys_from_snowflake(table_name, primary_keys, temp_table)
            self.duckdb.unregister(temp_table)

            if len(existing_keys_df) > 0:
                # Build a set of existing key tuples for fast lookup
                existing_keys_set = set()
                for _, row in existing_keys_df.iterrows():
                    key_tuple = tuple(row[pk] for pk in primary_keys)
                    existing_keys_set.add(key_tuple)

                # Filter input_df to find new records
                mask = []
                for _, row in input_df.iterrows():
                    key_tuple = tuple(row[pk] for pk in primary_keys)
                    is_duplicate = key_tuple in existing_keys_set
                    mask.append(not is_duplicate)

                unique_df = input_df[mask].copy()
                duplicate_df = input_df[~np.array(mask)].copy() if any(~np.array(mask)) else pd.DataFrame()

                # Update metrics
                self.quality_metrics['duplicates_found'] = len(duplicate_df)
                self.quality_metrics['new_records'] = len(unique_df)
                self.quality_metrics['duplicate_rate'] = (
                    len(duplicate_df) / len(input_df) if len(input_df) > 0 else 0
                )

                # Update metrics tracker
                if self.metrics_tracker:
                    self.metrics_tracker.update_records(len(input_df))
                    self.metrics_tracker.update_duplicates(len(duplicate_df))

                # Log results
                self.logger.log_msg(
                    f"Duplicate check for {table_name} (batch: {batch_id}): "
                    f"{self.quality_metrics['new_records']} new, "
                    f"{self.quality_metrics['duplicates_found']} duplicates "
                    f"({self.quality_metrics['duplicate_rate']:.2%} duplicate rate)"
                )

                # Alert if duplicate rate too high
                max_duplicate_rate = self.config.load_the_yaml().get('quality_thresholds', {}).get('max_duplicate_rate',
                                                                                                   0.10)
                if self.quality_metrics['duplicate_rate'] > max_duplicate_rate:
                    self.logger.log_warning(
                        f"⚠️ HIGH DUPLICATE RATE: {self.quality_metrics['duplicate_rate']:.2%} "
                        f"for {table_name} (threshold: {max_duplicate_rate:.2%})"
                    )

            else:
                # No existing records in Snowflake
                self.quality_metrics['duplicates_found'] = 0
                self.quality_metrics['new_records'] = len(input_df)
                self.quality_metrics['duplicate_rate'] = 0.0
                unique_df = input_df.copy()
                duplicate_df = pd.DataFrame()

                self.logger.log_msg(
                    f"No existing records found in Snowflake for {table_name}. "
                    f"All {len(input_df)} records are new."
                )

            # Return as DuckDB relation from DataFrame (no temp table dependency)
            return {
                'unique_relation': self.duckdb.from_df(unique_df),
                'duplicate_relation': self.duckdb.from_df(duplicate_df) if len(duplicate_df) > 0 else None,
                'metrics': self.quality_metrics.copy()
            }

        except Exception as e:
            error_msg = f"Error checking duplicates for {table_name}: {e}"
            self.logger.log_err(error_msg)

            # On error, return original as DuckDB relation
            return {
                'unique_relation': self.duckdb.from_df(input_df),
                'duplicate_relation': None,
                'metrics': self.quality_metrics,
                'error': str(e)
            }
