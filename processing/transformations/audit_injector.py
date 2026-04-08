# processing/transformations/audit_injector.py
import duckdb
import pandas as pd
from datetime import datetime
from .base import BaseTransformer
from core.logger import AuditLogger


class AuditInjector(BaseTransformer):
    """Handles injecting auditing metadata into every row."""

    def __init__(self, duckdb_connection):
        """Initialize with the shared DuckDB connection."""
        self.conn = duckdb_connection
        self.logger = AuditLogger()

    def transform(self, relation: duckdb.DuckDBPyRelation, batch_id: str) -> duckdb.DuckDBPyRelation:
        """Add ingested_at timestamp and batch_id to the relation."""

        if relation is None:
            self.logger.log_warning("AuditInjector: relation is None")
            return None

        try:
            # Convert to pandas (connection-agnostic)
            df = relation.df()

            # Add audit columns
            df['ingested_at'] = datetime.now()
            df['batch_id'] = batch_id

            # Convert back to DuckDB relation using the shared connection
            new_relation = self.conn.from_df(df)

            self.logger.log_msg(f"Successfully added audit columns to {len(df)} rows")
            return new_relation

        except Exception as e:
            self.logger.log_err(f"Failed to add audit columns: {e}")
            return relation