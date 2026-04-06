import uuid

from core.logger import AuditLogger
from ingestion.ingester_factory import FactoryIngester
from processing.schema_validator import SchemaValidator
from processing.quality_chekers.orphan_handling.orphan_detector import OrphanChecker


class BatchPipeline:

    def __init__(self, metadata_tracker, validator, dim_cache):

        self.logger = AuditLogger()
        self.metadata_tracker = metadata_tracker
        self.validator = validator
        self.dim_cache = dim_cache
        self.orphan_checker = OrphanChecker()

    def process_file(self, file_path):

        if self.metadata_tracker.is_file_processed(file_path):
            self.logger.log_msg(f"Skipping {file_path} (already processed)")
            return
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
            self.logger.log_err(f"{file_path} failed schema validation")
            return

        self.metadata_tracker.log_file_processed(file_path)