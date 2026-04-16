import os
import uuid

from core.logger import AuditLogger
from ingestion.ingester_factory import FactoryIngester
from processing.monitoring.metrics_tracker import MetricsTracker
from processing.quality_chekers.null_checker import NullChecker
from processing.quality_chekers.orphan_handling.orphan_detector import OrphanChecker
from processing.quality_chekers.orphan_handling.retry import RetryService
from config.schema_loader import SchemaLoader
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
        self.metrics_tracker = MetricsTracker(database="FASTFEASTDWH", schema="SILVER")
        self.orphan_checker = OrphanChecker(duckdb_conn)
        self.null_checker = NullChecker(self.metrics_tracker, duckdb_conn)
        self.dwh_loader = dwh_loader
        self.transformation_orchestrator = TransformationOrchestrator(duckdb_conn)
        self.retry_service = RetryService(duckdb_conn)
        self.schema_loader = SchemaLoader('config/schema.yaml')
        self.fact_tables = self.schema_loader.get_fact_table_names()

    def process_file(self, file_path):

        if self.metadata_tracker.is_file_processed(file_path):
            self.logger.log_msg(f"Skipping {file_path} (already processed)")
            return

        ingester = FactoryIngester(file_path, self.duckdb_conn).get_reader()
        batch_id = str(uuid.uuid4())
        table_name = get_table_name(file_path)

        # Start timing
        self.metrics_tracker.start_batch(batch_id, table_name)

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
                                    return
                            except Exception as e2:
                                self.logger.log_warning(f"Could not get row count via SQL: {e2}")

                        # Cache dimension and process
                        self.dim_cache.cache_dimension(table_name, clean_relation)
                        self.metadata_tracker.log_file_processed(file_path)

                        self.logger.log_msg(f"Starting retry service for {table_name}. Fact tables to check: {self.fact_tables}")
                        for fact_table in self.fact_tables:
                            self.logger.log_msg(f"Calling retry service for dim={table_name}, fact_table={fact_table}")
                            relation = self.retry_service.retry(
                                dim_name=table_name,
                                fact_table_name=fact_table
                            )
                            if relation is not None:
                                clean_relation = clean_relation.union(relation)
                                self.logger.log_msg(f"Completed retry for fact_table={fact_table},for dim={table_name} and appended {relation.row_count()} rows to clean_relation")
                            else:
                                self.logger.log_msg(f"No records to retry for fact_table={fact_table} and dim={table_name}")
                        self.metrics_tracker.increment_files_processed()

                        transformed_relation = self.transformation_orchestrator.run_all(table_name, clean_relation,
                                                                                        batch_id)

                        if transformed_relation is not None:
                            self.dwh_loader.load(table_name, transformed_relation)

                            self.metrics_tracker.save_to_snowflake(batch_id)

                            self.metrics_tracker.print_summary()
                        else:
                            self.logger.log_warning(f"No transformed relation for {table_name}")

                    else:
                        print(f"{file_path} failed Schema Validation. Dropping file.")
        except Exception as e:
            self.metrics_tracker.increment_files_failed()
            self.logger.log_err(f"An error occurred while processing the file: {e}")
        finally:
            # End timing
            self.metrics_tracker.end_batch(batch_id, table_name, file_path)