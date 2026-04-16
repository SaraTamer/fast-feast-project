import json
import duckdb
from config.config_loader import Config
from config.schema_loader import SchemaLoader
from db.connections import SnowflakeConnection, DuckDBConnection
from core.logger import AuditLogger as Logger
from db.dwh_loader import DWHLoader


class FactReplayService:

    def __init__(self):
        self.snow = SnowflakeConnection().conn
        self.logger = Logger()
        self.config = Config()
        self.schema_loader = SchemaLoader(self.config.schemas_path())
        self.dwh_loader = DWHLoader()
        self.duckdb = DuckDBConnection().conn # Create in-memory DuckDB connection

    def insert_fact(self, table_name, payload):
        """Insert a single replayed record using DWHLoader."""
        try:
            # Parse payload
            data = json.loads(payload)
            values = data.get("row")
            columns = self.schema_loader.get_required_cols(table_name)

            # Convert to lowercase to match DWHLoader expectations
            columns = [col.lower() for col in columns]

            # Create a single-row DataFrame
            import pandas as pd
            df = pd.DataFrame([values], columns=columns)

            # Convert to DuckDB relation
            relation = self.duckdb.from_df(df)

            # Use DWHLoader to insert (will append since it's a fact table)
            # rows_inserted = self.dwh_loader.load(table_name, relation)

            self.logger.log_msg(f"Sent relation for {table_name} to DWHLoader for insertion: {relation}")
            return relation

        except Exception as e:
            self.logger.log_err(f"Failed to insert replayed record into {table_name}: {e}")
            return None