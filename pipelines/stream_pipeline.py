import uuid

from core.logger import AuditLogger
from ingestion.ingester_factory import FactoryIngester
from processing.quality_chekers.null_checker import NullChecker
from processing.quality_chekers.orphan_handling.orphan_detector import OrphanChecker


class StreamPipeline:

    def __init__(self, validator, dim_cache):

        self.logger = AuditLogger()
        self.validator = validator
        self.dim_cache = dim_cache
        self.orphan_checker = OrphanChecker()
        self.null_checker = NullChecker()

    def process_event(self, file_path):

        ingester = FactoryIngester(file_path).get_reader()
        relation = ingester.ingest()
        if relation['data'] is None or relation['is_empty']:
            return

        valid_relation, table_name = self.validator.validate_schema(
            file_path,
            relation['data']
        )
        if valid_relation is None:
            self.logger.log_err(f"{file_path} schema invalid")
            return

        batch_id = str(uuid.uuid4())
        table_name = ingester.get_table_name()
        null_check_result = self.null_checker.check_null_values(
            df=valid_relation,
            file_path=file_path,
            table_name=table_name,
            batch_id=batch_id,
        )
        clean_df = null_check_result['clean_df']

        self.orphan_checker.detect_orphans(
            table_name=table_name,
            fact_df=clean_df,
            dims_names=self.dim_cache,
            batch_id=batch_id
        )