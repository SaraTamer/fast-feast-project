import duckdb
from datetime import datetime
import math
import numpy as np
import pandas as pd

from core.logger import AuditLogger
from db.warehouse_manager import WarehouseManager
from db.connections import SnowflakeConnection


class DWHLoader:
    """
    Generic DWH loader that creates tables and loads data to Snowflake.
    Automatically generates CREATE TABLE statements from DuckDB relation schema.
    """

    TYPE_MAPPING = {
        'VARCHAR': 'STRING',
        'STRING': 'STRING',
        'INTEGER': 'NUMBER',
        'BIGINT': 'NUMBER',
        'FLOAT': 'FLOAT',
        'DOUBLE': 'FLOAT',
        'BOOLEAN': 'BOOLEAN',
        'TIMESTAMP': 'TIMESTAMP',
        'DATE': 'DATE',
        'DECIMAL': 'NUMBER',
    }

    def __init__(self, database="FASTFEASTDWH", schema="SILVER"):
        self.logger = AuditLogger()
        self.database = database
        self.schema = schema
        self.warehouse_manager = None
        self.snowflake_connection = SnowflakeConnection()

    def _convert_value_for_snowflake(self, value):
        """Convert Python values to Snowflake-compatible types."""
        # Handle NaN values
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        if isinstance(value, (pd.Timedelta,)):
            return str(value)
        if pd.isna(value):  # Catches NaN, NaT, None
            return None
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, pd.Timestamp):
            return value.isoformat()
        elif isinstance(value, (np.int64, np.int32)):
            return int(value)
        elif isinstance(value, (np.float64, np.float32)):
            if math.isnan(value):
                return None
            return float(value)
        else:
            return value
    def _get_row_count(self, relation):
        """Safely get row count from DuckDB relation."""
        try:
            df = relation.df()
            return len(df)
        except Exception as e:
            self.logger.log_warning(f"Could not get row count via DataFrame: {e}")
            return 0

    def _get_snowflake_type(self, duckdb_type):
        """Convert DuckDB type to Snowflake type."""
        duckdb_type_str = str(duckdb_type).upper()

        for duckdb_t, snowflake_t in self.TYPE_MAPPING.items():
            if duckdb_t in duckdb_type_str:
                return snowflake_t

        return 'STRING'

    def _get_column_definitions(self, df):
        """Extract column definitions from DataFrame."""
        column_defs = []
        for col in df.columns:
            # Infer Snowflake type from pandas dtype
            dtype = str(df[col].dtype)
            if 'datetime' in dtype or 'timestamp' in dtype:
                snowflake_type = 'TIMESTAMP'
            elif 'int' in dtype:
                snowflake_type = 'NUMBER'
            elif 'float' in dtype:
                snowflake_type = 'FLOAT'
            elif 'bool' in dtype:
                snowflake_type = 'BOOLEAN'
            else:
                snowflake_type = 'STRING'

            column_defs.append(f'"{col}" {snowflake_type}')

        return column_defs

    def _generate_create_table_sql(self, table_name, column_defs):
        """Generate CREATE TABLE SQL statement."""
        columns_sql = ',\n    '.join(column_defs)

        return f"""
            CREATE TABLE IF NOT EXISTS {self.database}.{self.schema}.{table_name} (
                {columns_sql}
            )
        """

    def _table_exists(self, cursor, table_name):
        """Check if table exists in Snowflake."""
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM {self.database}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{self.schema}' 
            AND TABLE_NAME = '{table_name.upper()}'
        """)
        result = cursor.fetchone()
        return result[0] > 0

    def _ensure_table_exists(self, cursor, table_name, df):
        """Create table if it doesn't exist."""
        if self._table_exists(cursor, table_name):
            self.logger.log_msg(f"Table {table_name} already exists, skipping creation")
            return

        column_defs = self._get_column_definitions(df)
        create_sql = self._generate_create_table_sql(table_name, column_defs)

        self.logger.log_msg(f"Creating table {table_name}...")
        cursor.execute(create_sql)
        self.logger.log_msg(f"Table {table_name} created successfully")

    def _convert_df_to_rows(self, df):
        """Convert DataFrame to list of Snowflake-compatible tuples."""
        rows = []
        for _, row in df.iterrows():
            converted_row = tuple(self._convert_value_for_snowflake(val) for val in row)
            rows.append(converted_row)

        return rows, df.columns.tolist()

    def _generate_insert_sql(self, table_name, columns):
        """Generate INSERT SQL statement."""
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join([f'"{col}"' for col in columns])

        return f"""
            INSERT INTO {self.database}.{self.schema}.{table_name} 
            ({columns_str}) 
            VALUES ({placeholders})
        """

    def _insert_batch(self, cursor, insert_sql, rows, batch_size=1000):
        """Insert rows in batches."""
        total_inserted = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            total_inserted += len(batch)
            self.logger.log_msg(f"Inserted batch {i // batch_size + 1}: {len(batch)} rows")

        return total_inserted

    def load(self, table_name: str, relation: duckdb.DuckDBPyRelation):
        """
        Load transformed relation to Snowflake table.
        """
        if relation is None:
            self.logger.log_warning(f"No data to load for {table_name}")
            return 0

        # Convert to DataFrame for easier handling
        try:
            df = relation.df()
        except Exception as e:
            self.logger.log_err(f"Failed to convert relation to DataFrame: {e}")
            return 0

        if len(df) == 0:
            self.logger.log_warning(f"Empty relation for {table_name}, nothing to load")
            return 0

        self.logger.log_msg(f"Loading {len(df)} rows to {self.database}.{self.schema}.{table_name}")

        self.warehouse_manager = WarehouseManager(self.snowflake_connection.conn, "COMPUTE_WH")

        with self.warehouse_manager.auto_manage():
            cursor = self.snowflake_connection.conn.cursor()
            try:
                cursor.execute(f"USE DATABASE {self.database}")
                cursor.execute(f"USE SCHEMA {self.schema}")

                self._ensure_table_exists(cursor, table_name, df)

                rows, columns = self._convert_df_to_rows(df)

                if not rows:
                    self.logger.log_warning(f"No rows to insert for {table_name}")
                    return 0

                insert_sql = self._generate_insert_sql(table_name, columns)
                total_inserted = self._insert_batch(cursor, insert_sql, rows)

                self.snowflake_connection.conn.commit()
                self.logger.log_msg(f"Successfully loaded {total_inserted} rows to {table_name}")
                return total_inserted

            except Exception as e:
                self.snowflake_connection.conn.rollback()
                self.logger.log_err(f"Failed to load data to {table_name}: {e}")
                raise
            finally:
                cursor.close()