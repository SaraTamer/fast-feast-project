import os
import uuid
from core.logger import AuditLogger
from ingestion.ingester_factory import FactoryIngester
from processing.monitoring.metrics_tracker import MetricsTracker
from processing.quality_chekers.null_checker import NullChecker
from processing.quality_chekers.orphan_handling.orphan_detector import OrphanChecker
from utils.utils import get_table_name
from processing.quality_chekers.orphan_handling.retry import RetryService
from config.schema_loader import SchemaLoader

class BatchPipeline:

    def __init__(self, metadata_tracker, validator, dim_cache):

        self.logger = AuditLogger()
        self.metadata_tracker = metadata_tracker
        self.validator = validator
        self.dim_cache = dim_cache
        self.metrics_tracker = MetricsTracker()
        self.orphan_checker = OrphanChecker()
        self.null_checker = NullChecker(self.metrics_tracker)
        self.retry_service = RetryService()
        self.schema_loader = SchemaLoader('config/schema.yaml')
        self.fact_tables = self.schema_loader.get_fact_table_names()

    def process_file(self, file_path):

        if self.metadata_tracker.is_file_processed(file_path):
            self.logger.log_msg(f"Skipping {file_path} (already processed)")
            return
        ingester = FactoryIngester(file_path).get_reader()
        batch_id = str(uuid.uuid4())
        table_name = get_table_name(file_path)
        try:
            if ingester:
                relation = ingester.ingest()
                if relation['data'] is not None and not relation['is_empty']:
                    valid_relation, _ = self.validator.validate_schema(file_path, relation['data'])
                    if valid_relation:
                        print(f"\nData validated, this is a sample:")
                        print(valid_relation.limit(5))

                        null_check_result = self.null_checker.check_null_values(
                            relation=valid_relation,
                            file_path=file_path,
                            table_name=table_name,
                            batch_id=batch_id,
                        )
                        clean_relation = null_check_result['clean_relation']
                        clean_count = null_check_result['metrics']['clean_records_count']
                        
                        if clean_count == 0:
                            self.logger.log_warning(f"No clean records for {table_name}. Skipping further processing.")
                            return

                        self.dim_cache.cache_dimension(table_name, clean_relation)
                        self.metadata_tracker.log_file_processed(file_path)
                    
                        self.logger.log_msg(f"Starting retry service for {table_name}. Fact tables to check: {self.fact_tables}")
                        for fact_table in self.fact_tables:
                            self.logger.log_msg(f"Calling retry service for dim={table_name}, fact_table={fact_table}")
                            self.retry_service.retry(
                                dim_name=table_name,
                                table_name=fact_table
                            )
                            self.logger.log_msg(f"Completed retry service for dim={table_name}, fact_table={fact_table}")
                    else:
                        print(f"{file_path} failed Schema Validation. Dropping file.")
        except Exception as e:
            print(f"An error occurred while processing the file: {e}")

