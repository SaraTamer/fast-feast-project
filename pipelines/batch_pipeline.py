import os
import uuid

from core.logger import AuditLogger
from ingestion.ingester_factory import FactoryIngester
from processing.monitoring.metrics_tracker import MetricsTracker
from processing.quality_chekers.null_checker import NullChecker
from processing.transformations import TransformationOrchestrator
from utils.utils import get_table_name

class BatchPipeline:

    def __init__(self, metadata_tracker, validator, format_checker, dim_cache, dwh_loader, duckdb_conn):

        self.logger = AuditLogger()
        self.metadata_tracker = metadata_tracker
        self.duckdb_conn = duckdb_conn
        self.validator = validator
        self.format_checker = format_checker
        self.dim_cache = dim_cache
        self.metrics_tracker = MetricsTracker()
        self.null_checker = NullChecker(self.metrics_tracker, duckdb_conn)
        self.dwh_loader = dwh_loader
        self.transformation_orchestrator = TransformationOrchestrator(duckdb_conn)

    # pipelines/batch_pipeline.py

    def process_file(self, file_path):
        if self.metadata_tracker.is_file_processed(file_path):
            self.logger.log_msg(f"Skipping {file_path} (already processed)")
            return

        ingester = FactoryIngester(file_path, self.duckdb_conn).get_reader()
        batch_id = str(uuid.uuid4())
        table_name = get_table_name(file_path)

        try:
            if ingester:
                relation = ingester.ingest()
                if relation['data'] is not None and not relation['is_empty']:
                    validated_relation, columns_meta = self.validator.validate_schema(file_path, relation['data'])
                    if validated_relation:
                        print(f"\nData validated, this is a sample:")
                        print(validated_relation.limit(5))

                        formatted_relation, bad_rows_df = self.format_checker.separate(
                            validated_relation,
                            columns_meta,
                            table_name=table_name,
                            batch_id=batch_id,
                            primary_key=self.validator.loader.get_primary_key(table_name)
                        )

                        bad_count = 0 if bad_rows_df is None else len(bad_rows_df)
                        if bad_count > 0:
                            self.logger.log_warning(
                                f"[FORMAT CHECK] {bad_count} invalid rows detected in {table_name} | batch_id={batch_id}"
                            )
                        else:
                            self.logger.log_msg(
                                f"[FORMAT CHECK] No format issues detected in {table_name} | batch_id={batch_id}"
                            )

                        null_check_result = self.null_checker.check_null_values(
                            relation=formatted_relation,
                            file_path=file_path,
                            table_name=table_name,
                            batch_id=batch_id,
                        )

                        clean_relation = null_check_result['clean_relation']

                        if clean_relation is not None:
                            try:
                                # Register temp and query count
                                temp_name = f"_temp_count_{table_name}_{batch_id[:8]}"
                                self.duckdb_conn.conn.register(temp_name, clean_relation)
                                count_result = self.duckdb_conn.conn.execute(
                                    f"SELECT COUNT(*) FROM {temp_name}").fetchone()
                                row_count = count_result[0] if count_result else 0
                                self.duckdb_conn.conn.unregister(temp_name)

                                if row_count > 0:
                                    self.logger.log_msg(f"Clean relation has {row_count} rows (via SQL)")
                                else:
                                    self.logger.log_warning(f"Clean relation has 0 rows for {table_name}")
                            except Exception as e2:
                                self.logger.log_warning(f"Could not get row count via SQL: {e2}")

                        # Cache dimension and process
                        self.dim_cache.cache_dimension(table_name, clean_relation)
                        self.metadata_tracker.log_file_processed(file_path)

                        transformed_relation = self.transformation_orchestrator.run_all(table_name, clean_relation,
                                                                                        batch_id)

                        if transformed_relation is not None:
                            self.dwh_loader.load(table_name, transformed_relation)
                        else:
                            self.logger.log_warning(f"No transformed relation for {table_name}")

                    else:
                        print(f"{file_path} failed Schema Validation. Dropping file.")
        except Exception as e:
            self.logger.log_err(f"An error occurred while processing the file: {e}")


