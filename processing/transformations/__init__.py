import duckdb
from .enricher import Enricher
from .metrics_engine import MetricsEngine
from .audit_injector import AuditInjector

class TransformationOrchestrator:
    # This is the only class the (main.py) will talk to. (Facade patterm)

    def __init__(self, duckdb_conn):
        self.enricher = Enricher(duckdb_conn.conn)
        self.metrics = MetricsEngine(duckdb_conn.conn)
        self.audit = AuditInjector(duckdb_conn.conn)

    def run_all(self, table_name: str, relation: duckdb.DuckDBPyRelation, batch_id: str) -> duckdb.DuckDBPyRelation:
        # Add audit metadata
        relation = self.audit.transform(relation, batch_id)

        # Enrich with dimension joins
        relation = self.enricher.transform(table_name, relation)

        # Calculate SLAs and KPIs
        relation = self.metrics.transform(table_name, relation)

        return relation