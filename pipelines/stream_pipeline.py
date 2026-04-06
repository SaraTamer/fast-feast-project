import uuid

from core.logger import AuditLogger
from ingestion.ingester_factory import FactoryIngester
from processing.schema_validator import SchemaValidator
from processing.quality_chekers.orphan_handling.orphan_detector import OrphanChecker


class StreamPipeline:

    def __init__(self, validator, dim_cache):

        self.logger = AuditLogger()
        self.validator = validator
        self.dim_cache = dim_cache
        self.orphan_checker = OrphanChecker()

    def process_event(self, file_path):

        ingester = FactoryIngester(file_path).get_reader()
        relation = ingester.ingest()
        if relation is None:
            self.logger.log_err(f"{file_path} ingestion failed")
            return
        valid_relation, table_name = self.validator.validate_schema(
            file_path,
            relation
        )

        if valid_relation is None:
            self.logger.log_err(f"{file_path} schema invalid")
            return
        batch_id = str(uuid.uuid4())
        self.orphan_checker.detect_orphans(
            table_name=table_name,
            fact_df=valid_relation,
            dims_names=self.dim_cache,
            batch_id=batch_id
        )