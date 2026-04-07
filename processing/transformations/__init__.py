import duckdb
from .pii_masker import PIIMasker
from .enricher import Enricher
from .metrics_engine import MetricsEngine
from .audit_injector import AuditInjector

class TransformationOrchestrator:
    # This is the only class the (main.py) will talk to. (Facade patterm)

    def __init__(self, duckdb_conn):
        self.masker = PIIMasker()
        self.enricher = Enricher(duckdb_conn)
        self.metrics = MetricsEngine()
        self.audit = AuditInjector()

    def run_all(self, table_name: str, relation: duckdb.DuckDBPyRelation, batch_id: str) -> duckdb.DuckDBPyRelation:
        # Add audit metadata
        relation = self.audit.transform(relation, batch_id)

        # Apply PII masking
        relation = self.masker.transform(table_name, relation)

        # Enrich with dimension joins (Gold Layer preparation)
        relation = self.enricher.transform(table_name, relation)

        # Calculate SLAs and KPIs
        relation = self.metrics.transform(table_name, relation)

        return relation