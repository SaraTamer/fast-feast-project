import re
import duckdb
import pandas as pd
from typing import Dict, Any
from core.logger import AuditLogger
from config.config_loader import Config
from config.required_cols_loader import RequiredColsLoader
from db.connections import DuckDBConnection
from processing.error_batch_writer import ErrorBatchWriter
from datetime import datetime
from processing.monitoring.metrics_tracker import MetricsTracker


class NullChecker:
    """
    Checks for null values in required fields, primary keys, and foreign keys
    Provides detailed quality metrics about null percentages
    """

    def __init__(self, matrics_tracker: MetricsTracker = None, duckdb_conn: DuckDBConnection = None):
        self.logger = AuditLogger()
        self.config = Config()
        self.req_cols_loader = RequiredColsLoader(self.config.req_cols_path())
        self.duckdb = duckdb_conn.conn

        # Initialize ErrorBatchWriter for quarantining null records
        self.error_writer = ErrorBatchWriter()
        self.metrics_tracker = matrics_tracker

        # Quality metrics tracking
        self.quality_metrics = {
            'total_checks_performed': 0,
            'null_percentages': {},
            'failed_records': {},
            'quarantined_records': 0,
            'check_timestamp': None
        }

    def check_null_values(self, relation, file_path, table_name: str, batch_id: str = None,
                          check_pks: bool = True, check_fks: bool = True,
                          quarantine_nulls: bool = True) -> Dict[str, Any]:
        """
        Check for null values in required fields, primary keys, and foreign keys

        Returns:
            Dict containing:
                - clean_relation: DuckDB relation (no nulls in required columns)
                - null_records_relation: DuckDB relation (records with nulls)
                - null_summary: Dict of null statistics
                - metrics: Dict of metrics
        """
        self.quality_metrics['check_timestamp'] = datetime.now()
        self.quality_metrics['total_checks_performed'] += 1

        # Sanitize table name for DuckDB compatibility
        sanitized_table_name = self._sanitize_table_name(table_name)
        sanitized_batch_id = self._sanitize_table_name(batch_id)

        # Create sanitized temp table name
        timestamp = int(datetime.now().timestamp())
        temp_table = f"temp_null_check_{sanitized_table_name}_{sanitized_batch_id}_{timestamp}"

        # For JSON files, bypass DuckDB's automatic parsing and handle directly
        if file_path and file_path.endswith('.json'):
            try:
                duckdb_table = self._handle_json_file_directly(file_path, temp_table)
            except Exception as e:
                self.logger.log_err(f"Failed to handle JSON file directly: {e}")
                return {
                    'clean_relation': duckdb_table,
                    'null_records_relation': None,
                    'null_summary': {},
                    'metrics': {'error': f'JSON handling failed: {e}'}
                }
        else:
            # Convert the input to a DuckDB table (handles various types)
            try:
                duckdb_table = self._convert_to_duckdb_table(relation, temp_table, file_path)
            except Exception as e:
                self.logger.log_err(f"Failed to convert to DuckDB table: {e}")
                return {
                    'clean_relation': duckdb_table,
                    'null_records_relation': None,
                    'null_summary': {},
                    'metrics': {'error': f'Conversion failed: {e}'}
                }

        # Get schema definitions
        schema = self.req_cols_loader.tables.get(table_name, {})
        required_cols = schema.get('required_columns', [])
        primary_keys = schema.get('primary_keys', []) if check_pks else []
        foreign_keys = [fk['column'] for fk in schema.get('foreign_keys', [])] if check_fks else []

        # Combine all columns that must not be null
        non_null_columns = list(set(required_cols))

        if not non_null_columns:
            self.logger.log_warning(f"No non-null columns defined for {table_name}")
            self._safe_drop(temp_table)
            return {
                'clean_relation': relation,
                'null_records_relation': None,
                'null_summary': {},
                'metrics': {'warning': 'No null checks configured'}
            }

        # Get total records
        try:
            total_records = self.duckdb.execute(f"SELECT COUNT(*) FROM {temp_table}").fetchone()[0]
        except Exception as e:
            self.logger.log_err(f"Error getting record count: {e}")
            self._safe_drop(temp_table)
            return {
                'clean_relation': relation,
                'null_records_relation': None,
                'null_summary': {},
                'metrics': {'error': f'Query failed: {e}'}
            }

        if total_records == 0:
            self.logger.log_warning(f"No records to check for {table_name}")
            self._safe_drop(temp_table)
            return {
                'clean_relation': relation,
                'null_records_relation': None,
                'null_summary': {},
                'metrics': {'warning': 'Empty DataFrame'}
            }

        # Find records with nulls
        null_conditions = " OR ".join([f'"{col}" IS NULL' for col in non_null_columns])

        # Return as DuckDB relation (not DataFrame)
        null_records_relation = self.duckdb.execute(f'SELECT * FROM "{temp_table}" WHERE {null_conditions}').fetchdf()
        clean_records_count = total_records - len(null_records_relation)

        # Get clean records as DuckDB relation (not DataFrame)
        clean_records_query = f'SELECT * FROM "{temp_table}" WHERE NOT ({null_conditions})'
        clean_relation_df = self.duckdb.execute(clean_records_query).fetchdf()

        # Convert back to DuckDB relation
        clean_relation = self.duckdb.from_df(clean_relation_df)

        # Calculate null percentages per column
        null_summary = {}
        columns_with_nulls = []
        rows_with_nulls = []

        for col in non_null_columns:
            null_count_query = f'SELECT COUNT(*) FROM "{temp_table}" WHERE "{col}" IS NULL'
            null_count = self.duckdb.execute(null_count_query).fetchone()[0]
            null_percentage = (null_count / total_records * 100) if total_records > 0 else 0
            null_summary[col] = {
                'null_count': null_count,
                'null_percentage': null_percentage,
                'is_primary_key': col in primary_keys,
                'is_foreign_key': col in foreign_keys,
                'is_required': col in required_cols
            }

            if null_count > 0:
                columns_with_nulls.append(col)

        # Prepare rows for quarantine (records with nulls in required columns)
        if quarantine_nulls and len(null_records_relation) > 0:
            for idx, row in null_records_relation.iterrows():
                event_id = None
                for pk in primary_keys:
                    if pk in row and pd.notna(row[pk]):
                        event_id = str(row[pk])
                        break

                if event_id is None:
                    event_id = f"null_row_{idx}"

                null_columns = []
                for col in required_cols:
                    if col in row and pd.isna(row[col]):
                        null_columns.append(col)

                error_column = ", ".join(null_columns) if null_columns else "multiple_required_columns"

                rows_with_nulls.append((
                    event_id,
                    row.to_dict()
                ))

            try:
                self.error_writer.write_batch(
                    table_name=table_name,
                    batch_id=batch_id,
                    rows=rows_with_nulls,
                    error_type="NULL_REQUIRED_COLUMN",
                    error_column=error_column,
                    fk_table=None,
                    is_retryable=False
                )
                self.quality_metrics['quarantined_records'] = len(rows_with_nulls)
                self.logger.log_msg(
                    f"Quarantined {len(rows_with_nulls)} records with nulls in required columns "
                    f"for table {table_name} (batch: {batch_id})"
                )
            except Exception as e:
                self.logger.log_err(f"Failed to quarantine null records: {e}")

        if self.metrics_tracker:
            self.metrics_tracker.update_records(total_records)
            self.metrics_tracker.update_null_records(len(null_records_relation))
            self.metrics_tracker.update_clean_records(clean_records_count)
            self.metrics_tracker.update_quarantined(len(rows_with_nulls))
            self.metrics_tracker.update_null_percentages(table_name, null_summary)

        # Update quality metrics
        self.quality_metrics['null_percentages'][table_name] = null_summary
        self.quality_metrics['failed_records'][table_name] = {
            'total_records': total_records,
            'null_records_count': len(null_records_relation),
            'clean_records_count': total_records - len(null_records_relation),
            'null_rate': len(null_records_relation) / total_records if total_records > 0 else 0,
            'columns_with_nulls': columns_with_nulls,
            'batch_id': batch_id,
            'quarantined': len(null_records_relation) if quarantine_nulls else 0
        }

        # Log results
        self._log_null_results(table_name, batch_id, null_summary, total_records, len(null_records_relation))

        # Check threshold from config
        max_null_percentage = self.config.load_the_yaml().get('quality_thresholds', {}).get('max_null_percentage', 5.0)

        for col, stats in null_summary.items():
            if stats['null_percentage'] > max_null_percentage:
                if stats['is_required']:
                    self.logger.log_err(
                        f"CRITICAL: High null percentage for {table_name}.{col}: "
                        f"{stats['null_percentage']:.2f}% (threshold: {max_null_percentage}%)"
                    )
                else:
                    self.logger.log_warning(
                        f"High null percentage for {table_name}.{col}: "
                        f"{stats['null_percentage']:.2f}%"
                    )

        # Clean up
        self._safe_drop(temp_table)

        return {
            'clean_relation': clean_relation,
            'null_records_relation': self.duckdb.from_df(null_records_relation) if len(
                null_records_relation) > 0 else None,
            'null_summary': null_summary,
            'metrics': self.quality_metrics['failed_records'][table_name]
        }

    def _convert_to_duckdb_table(self, relation, temp_table: str, file_path: str = None):
        """
        Convert various input types to a DuckDB table.
        Returns DuckDB table reference.
        """
        # Case 1: Already a DuckDB relation
        if isinstance(relation, duckdb.DuckDBPyRelation):
            try:
                # Get the SQL query from the relation
                self.duckdb.execute(f"CREATE OR REPLACE TABLE {temp_table} AS SELECT * FROM relation")
                return self.duckdb.table(temp_table)
            except Exception as e:
                self.logger.log_warning(f"Failed to create from relation, falling back: {e}")
                # Convert to arrow and back
                arrow_table = relation.arrow()
                self.duckdb.execute(f"CREATE OR REPLACE TABLE {temp_table} AS SELECT * FROM arrow_table")
                return self.duckdb.table(temp_table)

        # Case 2: pandas DataFrame - convert to DuckDB table
        elif isinstance(relation, pd.DataFrame):
            self.duckdb.execute(f"CREATE OR REPLACE TABLE {temp_table} AS SELECT * FROM relation")
            return self.duckdb.table(temp_table)

        # Case 3: Dictionary
        elif isinstance(relation, dict):
            pandas_df = pd.DataFrame([relation]) if not isinstance(list(relation.values())[0], list) else pd.DataFrame(
                relation)
            self.duckdb.execute(f"CREATE OR REPLACE TABLE {temp_table} AS SELECT * FROM pandas_df")
            return self.duckdb.table(temp_table)

        # Case 4: List of dictionaries
        elif isinstance(relation, list):
            if len(relation) > 0 and isinstance(relation[0], dict):
                pandas_df = pd.DataFrame(relation)
            else:
                pandas_df = pd.DataFrame({'data': relation})
            self.duckdb.execute(f"CREATE OR REPLACE TABLE {temp_table} AS SELECT * FROM pandas_df")
            return self.duckdb.table(temp_table)

        # Case 5: Tuple or other iterable
        elif isinstance(relation, tuple):
            pandas_df = pd.DataFrame([relation])
            self.duckdb.execute(f"CREATE OR REPLACE TABLE {temp_table} AS SELECT * FROM pandas_df")
            return self.duckdb.table(temp_table)

        # Case 6: None or empty
        elif relation is None:
            self.logger.log_err(f"Input is None for {temp_table}")
            raise ValueError("Input DataFrame is None")

        # Case 7: Try to convert anything else
        else:
            try:
                pandas_df = pd.DataFrame(relation)
                self.duckdb.execute(f"CREATE OR REPLACE TABLE {temp_table} AS SELECT * FROM pandas_df")
                return self.duckdb.table(temp_table)
            except Exception as e:
                self.logger.log_err(f"Failed to convert {type(relation)} to DataFrame: {e}")
                raise

    def _safe_drop(self, table_name: str):
        """Safely drop a table or view if it exists"""
        try:
            self.duckdb.execute(f"DROP TABLE IF EXISTS {table_name}")
        except:
            try:
                self.duckdb.execute(f"DROP VIEW IF EXISTS {table_name}")
            except:
                pass

    def _log_null_results(self, table_name: str, batch_id: str, null_summary: Dict,
                          total: int, null_count: int):
        """Log null check results"""
        if null_count == 0:
            self.logger.log_msg(
                f"Null check passed for {table_name} (batch: {batch_id}): {total} records, no nulls in required fields")
        else:
            self.logger.log_warning(
                f"Null check for {table_name} (batch: {batch_id}): "
                f"{total} total, {null_count} records with nulls ({null_count / total * 100:.2f}%)"
            )

            for col, stats in null_summary.items():
                if stats['null_count'] > 0:
                    field_type = []
                    if stats.get('is_required', False):
                        field_type.append("REQUIRED")
                    if stats.get('is_primary_key', False):
                        field_type.append("PK")
                    if stats.get('is_foreign_key', False):
                        field_type.append("FK")

                    self.logger.log_warning(
                        f"  - {col}: {stats['null_count']} nulls ({stats['null_percentage']:.2f}%) - "
                        f"[{', '.join(field_type)}]"
                    )

    def _sanitize_table_name(self, name: str) -> str:
        """Sanitize table name to be DuckDB-compatible."""
        if name is None:
            return "stream"
        if not isinstance(name, str):
            name = str(name)
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        if sanitized and sanitized[0].isdigit():
            sanitized = f"t_{sanitized}"
        return sanitized

    def _handle_json_file_directly(self, file_path: str, temp_table: str):
        """
        Handle JSON file directly with proper date parsing.
        Returns DuckDB table reference.
        """
        try:
            import json

            # Read JSON file
            with open(file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)

            # Handle both array and object formats
            if isinstance(json_data, dict):
                if len(json_data) > 0 and isinstance(list(json_data.values())[0], dict):
                    records = list(json_data.values())
                else:
                    records = [json_data]
            elif isinstance(json_data, list):
                records = json_data
            else:
                records = [json_data]

            # Convert to pandas DataFrame
            df = pd.DataFrame(records)

            # Convert ALL string columns that look like dates to datetime
            for col in df.columns:
                if df[col].dtype == 'object':
                    sample = df[col].dropna()
                    if len(sample) > 0:
                        first_value = str(sample.iloc[0])
                        if self._looks_like_date(first_value):
                            df[col] = pd.to_datetime(df[col], errors='coerce')

            # ✅ Create DuckDB table directly (not register)
            self.duckdb.execute(f"CREATE OR REPLACE TABLE {temp_table} AS SELECT * FROM df")
            return self.duckdb.table(temp_table)

        except Exception as e:
            self.logger.log_err(f"Failed to handle JSON file directly: {e}")
            raise

    def _looks_like_date(self, value: str) -> bool:
        """Check if a string looks like a date"""
        if not isinstance(value, str):
            return False
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{2}-\d{2}-\d{4}',  # DD-MM-YYYY
        ]
        return any(re.match(pattern, value) for pattern in date_patterns)
