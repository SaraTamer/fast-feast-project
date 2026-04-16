# processing/transformations/audit_injector.py
import duckdb
import pandas as pd
from .base import BaseTransformer
from core.logger import AuditLogger


class AuditInjector(BaseTransformer):
    """Handles injecting auditing metadata into every row."""

    def __init__(self, duckdb_connection):
        self.conn = duckdb_connection
        self.logger = AuditLogger()

    def transform(self, relation, batch_id: str):
        """Add ingested_at timestamp and batch_id."""

        if relation is None:
            self.logger.log_warning("AuditInjector: relation is None")
            return None

        # Handle dictionary input
        if isinstance(relation, dict):
            relation = relation.get('data')
            if relation is None:
                return None

        # Handle pandas DataFrame
        if isinstance(relation, pd.DataFrame):
            relation = self.conn.from_df(relation)

        # Check if relation has rows
        try:
            df_check = relation.df()
            if len(df_check) == 0:
                self.logger.log_warning("AuditInjector: empty relation")
                return relation
        except Exception as e:
            self.logger.log_warning(f"Could not check relation: {e}")

        # Use pandas approach (more reliable)
        try:
            df = relation.df()
            df['ingested_at'] = pd.Timestamp.now()
            df['batch_id'] = batch_id
            result = self.conn.from_df(df)
            self.logger.log_msg(f"Added audit columns to {len(df)} rows via pandas")
            return result
        except Exception as e:
            self.logger.log_err(f"Failed to add audit columns: {e}")
            return relation