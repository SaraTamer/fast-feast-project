# pipelines/stream_pipeline.py
import uuid
import pandas as pd
from core.logger import AuditLogger
from ingestion.ingester_factory import FactoryIngester
from processing.monitoring.metrics_tracker import MetricsTracker
from processing.quality_chekers.null_checker import NullChecker
from processing.quality_chekers.orphan_handling.orphan_detector import OrphanChecker
from processing.transformations import TransformationOrchestrator
from utils.utils import get_table_name
from processing.quality_chekers.duplicate_detector import DuplicateChecker


class StreamPipeline:

    def __init__(self, metadata_tracker, validator, format_checker, dim_cache, dwh_loader, duckdb_conn):

        self.logger = AuditLogger()
        self.validator = validator
        self.format_checker = format_checker
        self.dim_cache = dim_cache
        self.metrics_tracker = MetricsTracker()
        self.orphan_checker = OrphanChecker(duckdb_conn)
        self.null_checker = NullChecker(self.metrics_tracker, duckdb_conn)
        self.metadata_tracker = metadata_tracker
        self.dwh_loader = dwh_loader
        self.transformation_orchestrator = TransformationOrchestrator(duckdb_conn)
        self.duck_db_connection = duckdb_conn
        self.duplicate_checker = DuplicateChecker(self.metrics_tracker, duckdb_conn)

    def process_event(self, file_path):

        ingester = FactoryIngester(file_path, self.duck_db_connection).get_reader()
        batch_id = str(uuid.uuid4())
        table_name = get_table_name(file_path)
        self.metrics_tracker.start_batch(batch_id, table_name)

        if self.metadata_tracker.is_file_processed(file_path):
            self.logger.log_msg(f"Skipping {file_path} (already processed)")
            return

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

                        if null_check_result['metrics']['clean_records_count'] == 0:
                            self.logger.log_warning(f"No clean records for {table_name}. Skipping further processing.")
                            return

                        # ✅ Materialize the relation as a DataFrame to break dependencies
                        clean_df = clean_relation.df()
                        self.logger.log_msg(f"Materialized clean relation as DataFrame with {len(clean_df)} rows")

                        # Remove orphans (pass DataFrame, will return DuckDB relation)
                        clean_relation = self.orphan_checker.detect_orphans(
                            table_name=table_name,
                            fact_df=self.duck_db_connection.conn.from_df(clean_df),
                            dims_names=self.dim_cache.get_all_cached_dimensions(),
                            batch_id=batch_id
                        )

                        # ✅ Materialize again before duplicate check
                        clean_df = clean_relation.df()
                        self.logger.log_msg(f"After orphan removal: {len(clean_df)} rows")

                        # Check for duplicates (pass DataFrame)
                        duplicate_result = self.duplicate_checker.check_duplicates(
                            self.duck_db_connection.conn.from_df(clean_df),
                            table_name,
                            batch_id
                        )

                        # Get unique records as DataFrame
                        unique_relation = duplicate_result['unique_relation']
                        unique_df = unique_relation.df() if hasattr(unique_relation, 'df') else pd.DataFrame(
                            unique_relation)

                        if len(unique_df) == 0:
                            self.logger.log_warning(f"No unique records for {table_name}. Skipping.")
                            return

                        self.logger.log_msg(f"After duplicate removal: {len(unique_df)} unique rows")

                        self.metadata_tracker.log_file_processed(file_path)
                        self.metrics_tracker.increment_files_processed()
                        # ✅ Create a fresh relation for transformation
                        fresh_relation = self.duck_db_connection.conn.from_df(unique_df)

                        # Transform
                        transformed_relation = self.transformation_orchestrator.run_all(
                            table_name=table_name,
                            relation=fresh_relation,
                            batch_id=batch_id
                        )

                        # Load to DWH
                        self.dwh_loader.load(table_name, transformed_relation)

                    else:
                        print(f"{file_path} failed Schema Validation. Dropping file.")
        except Exception as e:
            self.metrics_tracker.increment_files_failed()
            self.logger.log_err(f"An error occurred while processing the file: {e}")
        finally:
            # End timing
            self.metrics_tracker.end_batch(batch_id, table_name, file_path)

            # Save metrics to Snowflake periodically
            if self.metrics_tracker.files_processed % 10 == 0:
                self.metrics_tracker.save_to_snowflake(batch_id)
