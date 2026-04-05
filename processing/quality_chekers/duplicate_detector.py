import duckdb
import pandas as pd
from typing import Dict, List, Any, Optional
from core.logger import AuditLogger
from db.connections import SnowflakeConnection
from config.config_loader import Config
from config.schema_loader import SchemaLoader
from datetime import datetime


class DuplicateChecker:
    """
    Checks for duplicate records between streaming DataFrame and existing Snowflake DWH
    Prevents duplicate inserts by identifying records that already exist in production
    """

    def __init__(self):
        """Initialize with Snowflake connection (production DWH)"""
        self.logger = AuditLogger()
        self.config = Config()
        self.schema_loader = SchemaLoader(self.config.req_cols_path())

        # Connect to Snowflake (production DWH)
        self.snowflake = SnowflakeConnection().conn
        self.database = self._get_current_database()

        # DuckDB for local processing
        self.duckdb = duckdb.connect(':memory:')

        # Quality metrics
        self.quality_metrics = {
            'total_records_checked': 0,
            'duplicates_found': 0,
            'duplicate_rate': 0.0,
            'new_records': 0,
            'check_timestamp': None,
            'table_checked': None
        }

    def check_duplicates(self, df, table_name: str, batch_id: str = None) -> Dict[str, Any]:
        """
        Check for duplicates between incoming DataFrame and Snowflake DWH

        Args:
            df: DuckDB relation, pandas DataFrame, or list of dicts with incoming data
            table_name: Name of the table (orders, tickets, customers, etc.)
            batch_id: Optional batch identifier for logging (e.g., '2026-02-20' or '2026-02-20_09')

        Returns:
            Dictionary with:
                - unique_df: DataFrame with only new records (no duplicates)
                - duplicate_df: DataFrame with records that already exist in Snowflake
                - metrics: Quality metrics about the check
        """
        self.quality_metrics['check_timestamp'] = datetime.now()
        self.quality_metrics['table_checked'] = table_name

        # Convert input to DuckDB for processing
        temp_table = f"temp_{table_name}_{batch_id or 'stream'}_{int(datetime.now().timestamp())}"

        if hasattr(df, 'register'):
            # Already a DuckDB relation
            self.duckdb.register(temp_table, df)
        elif isinstance(df, pd.DataFrame):
            self.duckdb.register(temp_table, df)
        else:
            # Assume it's a list of dicts
            temp_df = pd.DataFrame(df)
            self.duckdb.register(temp_table, temp_df)

        # Get total records
        total_records = self.duckdb.execute(f"SELECT COUNT(*) FROM {temp_table}").fetchone()[0]
        self.quality_metrics['total_records_checked'] = total_records

        if total_records == 0:
            self.logger.log_warning(f"No records to check for {table_name}")
            return {
                'unique_df': df,
                'duplicate_df': None,
                'metrics': self.quality_metrics,
                'warning': 'Empty DataFrame'
            }

        # Get primary keys for the table
        primary_keys = self.schema_loader.schemas.get(table_name, {}).get('primary_keys', [])

        if not primary_keys:
            self.logger.log_err(f"No primary keys defined for {table_name} in schema.yaml")
            return {
                'unique_df': df,
                'duplicate_df': None,
                'metrics': self.quality_metrics,
                'error': f'No primary keys defined for {table_name}'
            }

        # Get existing records from Snowflake for these primary keys
        snowflake_table = self._get_table_name_with_schema(table_name)
        pk_columns = ", ".join(primary_keys)

        # Build query to fetch existing primary keys from Snowflake
        snowflake_query = f"""
            SELECT {pk_columns} 
            FROM {snowflake_table}
            WHERE {self._build_where_clause(primary_keys, temp_table)}
        """

        try:
            # Fetch existing keys from Snowflake and load into DuckDB
            snowflake_cursor = self.snowflake.cursor()
            snowflake_cursor.execute(snowflake_query)
            existing_keys_df = snowflake_cursor.fetch_pandas_all()

            if len(existing_keys_df) > 0:
                # Register existing keys in DuckDB
                self.duckdb.register("existing_keys", existing_keys_df)

                # Build duplicate detection query
                join_conditions = " AND ".join([
                    f"t.{pk} = ek.{pk}" for pk in primary_keys
                ])

                # Find duplicates
                duplicate_query = f"""
                    SELECT t.* 
                    FROM {temp_table} t
                    INNER JOIN existing_keys ek
                    ON {join_conditions}
                """

                # Find new records
                new_records_query = f"""
                    SELECT t.* 
                    FROM {temp_table} t
                    LEFT JOIN existing_keys ek
                    ON {join_conditions}
                    WHERE ek.{primary_keys[0]} IS NULL
                """

                duplicate_df = self.duckdb.execute(duplicate_query).fetchdf()
                unique_df = self.duckdb.execute(new_records_query)

                # Update metrics
                self.quality_metrics['duplicates_found'] = len(duplicate_df)
                self.quality_metrics['new_records'] = total_records - len(duplicate_df)
                self.quality_metrics['duplicate_rate'] = (
                    len(duplicate_df) / total_records if total_records > 0 else 0
                )

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
                        f"for {table_name} (threshold: {max_duplicate_rate:.2%}). "
                        f"Check if data is being re-ingested."
                    )

                # Log duplicate examples
                if len(duplicate_df) > 0 and len(duplicate_df) <= 5:
                    for _, row in duplicate_df.iterrows():
                        pk_values = {pk: row[pk] for pk in primary_keys}
                        self.logger.log_warning(f"  Duplicate: {pk_values}")
                elif len(duplicate_df) > 0:
                    self.logger.log_warning(
                        f"  First 5 duplicates: {duplicate_df[primary_keys].head().to_dict('records')}")

            else:
                # No existing records in Snowflake
                self.quality_metrics['duplicates_found'] = 0
                self.quality_metrics['new_records'] = total_records
                self.quality_metrics['duplicate_rate'] = 0.0
                unique_df = self.duckdb.execute(f"SELECT * FROM {temp_table}")

                self.logger.log_msg(
                    f"No existing records found in Snowflake for {table_name}. "
                    f"All {total_records} records are new."
                )

            # Clean up
            self.duckdb.execute(f"DROP TABLE IF EXISTS {temp_table}")
            self.duckdb.execute("DROP TABLE IF EXISTS existing_keys")

            return {
                'unique_df': unique_df,
                'duplicate_df': duplicate_df if 'duplicate_df' in locals() and len(duplicate_df) > 0 else None,
                'metrics': self.quality_metrics.copy()
            }

        except Exception as e:
            error_msg = f"Error checking duplicates for {table_name} in Snowflake: {e}"
            self.logger.log_error(error_msg)
            self.duckdb.execute(f"DROP TABLE IF EXISTS {temp_table}")

            # On error, return original DF (fail safe - process anyway)
            return {
                'unique_df': df,
                'duplicate_df': None,
                'metrics': self.quality_metrics,
                'error': str(e)
            }

    def get_quality_report(self) -> Dict:
        """Return quality metrics summary"""
        return {
            'checker_type': 'duplicate_checker',
            'dwh_type': 'snowflake',
            'database': self.database,
            'metrics': self.quality_metrics
        }

    def _get_current_database(self) -> str:
        """Get current Snowflake database name"""
        # TODO: get dwh table names from config.yaml file
        try:
            result = self.snowflake.execute("SELECT CURRENT_DATABASE()").fetchone()
            return result[0] if result else "FASTFEAST_DWH"
        except:
            return "FASTFEAST_DWH"

    def _get_table_name_with_schema(self, table_name: str) -> str:
        """Get fully qualified table name for Snowflake"""
        #TODO replace schema name
        # Assuming schema is 'ANALYTICS' or 'DWH'
        return f"{self.database}.ANALYTICS.{table_name}"

    def _build_where_clause(self, primary_keys: List[str], temp_table: str) -> str:
        """Build WHERE clause to fetch only relevant existing records"""
        # Get distinct key combinations from temp table
        pk_columns = ", ".join(primary_keys)

        # Create a subquery to get distinct keys
        subquery = f"""
            WITH temp_keys AS (
                SELECT DISTINCT {pk_columns}
                FROM {temp_table}
            )
            SELECT {pk_columns}
            FROM temp_keys
        """

        try:
            distinct_keys = self.duckdb.execute(subquery).fetchdf()

            if len(distinct_keys) == 0:
                return "1=0"  # No records

            # Build IN clause for each primary key
            conditions = []
            for pk in primary_keys:
                # Get unique values for this primary key
                values = distinct_keys[pk].dropna().unique().tolist()
                if values:
                    # Escape single quotes in string values - FIXED VERSION
                    escaped_values = []
                    for val in values:
                        # Convert to string and escape single quotes
                        str_val = str(val)
                        escaped_val = str_val.replace("'", "''")
                        escaped_values.append(f"'{escaped_val}'")

                    values_str = ", ".join(escaped_values)
                    conditions.append(f"{pk} IN ({values_str})")

            return " AND ".join(conditions) if conditions else "1=1"

        except Exception as e:
            self.logger.log_warning(f"Could not build optimized WHERE clause: {e}")
            return "1=1"  # Fetch all records (slow but safe)
