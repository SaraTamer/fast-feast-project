import duckdb
from datetime import datetime
from .base import BaseTransformer

class AuditInjector(BaseTransformer):
    # Handles injecting auditing metadata into every row.

    def transform(self, relation: duckdb.DuckDBPyRelation, batch_id: str) -> duckdb.DuckDBPyRelation:
        # Add ingested_at timestamp and a batch_id to the relation.
        return relation.project(f"*, now() as ingested_at, '{batch_id}' as batch_id")
