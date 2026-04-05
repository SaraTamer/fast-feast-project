import duckdb
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from core.logger import AuditLogger
from config.config_loader import Config
from config.required_cols_loader import RequiredColsLoader
from processing.error_batch_writer import ErrorBatchWriter
from datetime import datetime


class NullChecker:
    """
    Checks for null values in required fields, primary keys, and foreign keys
    Provides detailed quality metrics about null percentages
    """

    def __init__(self):
        self.logger = AuditLogger()
        self.config = Config()
        self.req_cols_loader = RequiredColsLoader(self.config.req_cols_path())
        self.duckdb = duckdb.connect(':memory:')

        # Initialize ErrorBatchWriter for quarantining null records
        self.error_writer = ErrorBatchWriter()

        # Quality metrics tracking
        self.quality_metrics = {
            'total_checks_performed': 0,
            'null_percentages': {},
            'failed_records': {},
            'quarantined_records': 0,
            'check_timestamp': None
        }

    def check_null_values(self, df, table_name: str, batch_id: str = None,
                          check_pks: bool = True, check_fks: bool = True,
                          quarantine_nulls: bool = True) -> Dict[str, Any]:
        """
        Check for null values in required fields, primary keys, and foreign keys

        Args:
            df: DuckDB relation, pandas DataFrame, or list of dicts
            table_name: Name of the table (orders, tickets, customers, etc.)
            batch_id: Optional batch identifier for logging (e.g., '2026-02-20' or '2026-02-20_09')
            check_pks: Whether to check primary keys for nulls
            check_fks: Whether to check foreign keys for nulls
            quarantine_nulls: Whether to quarantine records with nulls in required columns

        Returns:
            Dictionary with:
                - clean_df: DataFrame with no nulls in required fields
                - null_records_df: DataFrame containing records with nulls
                - null_summary: Summary of null counts per column
                - metrics: Quality metrics
        """
        global error_column
        self.quality_metrics['check_timestamp'] = datetime.now()
        self.quality_metrics['total_checks_performed'] += 1

        # Convert input to DuckDB
        temp_table = f"temp_null_check_{table_name}_{batch_id or 'stream'}_{int(datetime.now().timestamp())}"

        if hasattr(df, 'register'):
            self.duckdb.register(temp_table, df)
        elif isinstance(df, pd.DataFrame):
            self.duckdb.register(temp_table, df)
        else:
            temp_df = pd.DataFrame(df)
            self.duckdb.register(temp_table, temp_df)

        # Get schema definitions
        schema = self.req_cols_loader.tables.get(table_name, {})
        required_cols = schema.get('required_columns', [])
        primary_keys = schema.get('primary_keys', []) if check_pks else []
        foreign_keys = [fk['column'] for fk in schema.get('foreign_keys', [])] if check_fks else []

        # Combine all columns that must not be null
        non_null_columns = list(set(required_cols))

        if not non_null_columns:
            self.logger.log_warning(f"No non-null columns defined for {table_name}")
            self.duckdb.execute(f"DROP TABLE IF EXISTS {temp_table}")
            return {
                'clean_df': df,
                'null_records_df': None,
                'null_summary': {},
                'metrics': {'warning': 'No null checks configured'}
            }

        # Get total records
        total_records = self.duckdb.execute(f"SELECT COUNT(*) FROM {temp_table}").fetchone()[0]

        if total_records == 0:
            self.logger.log_warning(f"No records to check for {table_name}")
            self.duckdb.execute(f"DROP TABLE IF EXISTS {temp_table}")
            return {
                'clean_df': df,
                'null_records_df': None,
                'null_summary': {},
                'metrics': {'warning': 'Empty DataFrame'}
            }

        # Find records with nulls
        null_conditions = " OR ".join([f"{col} IS NULL" for col in non_null_columns])
        null_records_df = self.duckdb.execute(f"SELECT * FROM {temp_table} WHERE {null_conditions}").fetchdf()

        # Get clean records (no nulls in non-null columns)
        clean_records_query = f"SELECT * FROM {temp_table} WHERE NOT ({null_conditions})"
        clean_df = self.duckdb.execute(clean_records_query)

        # Calculate null percentages per column
        null_summary = {}
        columns_with_nulls = []
        rows_with_nulls = []  # Store rows for quarantine

        for col in non_null_columns:
            null_count_query = f"""
                SELECT COUNT(*) FROM {temp_table} 
                WHERE {col} IS NULL
            """
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
        if quarantine_nulls and len(null_records_df) > 0:
            # Convert null records to list of tuples for the ErrorBatchWriter
            for idx, row in null_records_df.iterrows():
                # Get the primary key value as event_id (use first available ID column)
                event_id = None
                for pk in primary_keys:
                    if pk in row and pd.notna(row[pk]):
                        event_id = str(row[pk])
                        break

                # If no primary key found, use row index
                if event_id is None:
                    event_id = f"null_row_{idx}"

                # Determine which required columns are null
                null_columns = []
                for col in required_cols:
                    if col in row and pd.isna(row[col]):
                        null_columns.append(col)

                error_column = ", ".join(null_columns) if null_columns else "multiple_required_columns"

                # Create row tuple for ErrorBatchWriter
                # Format: (event_id, raw_payload) - will be expanded by write_batch
                rows_with_nulls.append((
                    event_id,
                    row.to_dict()  # Store the entire row as payload
                ))

            # Quarantine the records with nulls
            try:
                self.error_writer.write_batch(
                    table_name=table_name,
                    batch_id=batch_id,
                    rows=rows_with_nulls,
                    error_type="NULL_REQUIRED_COLUMN",
                    error_column=error_column,
                    fk_table=None,  # Not applicable for null checks
                    is_retryable=False  # Nulls are not retryable without fixing source data
                )
                self.quality_metrics['quarantined_records'] = len(rows_with_nulls)
                self.logger.log_msg(
                    f"Quarantined {len(rows_with_nulls)} records with nulls in required columns "
                    f"for table {table_name} (batch: {batch_id})"
                )
            except Exception as e:
                self.logger.log_err(f"Failed to quarantine null records: {e}")

        # Update quality metrics
        self.quality_metrics['null_percentages'][table_name] = null_summary
        self.quality_metrics['failed_records'][table_name] = {
            'total_records': total_records,
            'null_records_count': len(null_records_df),
            'clean_records_count': total_records - len(null_records_df),
            'null_rate': len(null_records_df) / total_records if total_records > 0 else 0,
            'columns_with_nulls': columns_with_nulls,
            'batch_id': batch_id,
            'quarantined': len(null_records_df) if quarantine_nulls else 0
        }

        # Log results
        self._log_null_results(table_name, batch_id, null_summary, total_records, len(null_records_df))

        # Check threshold from config
        max_null_percentage = self.config.load_the_yaml().get('quality_thresholds', {}).get('max_null_percentage', 5.0)

        for col, stats in null_summary.items():
            if stats['null_percentage'] > max_null_percentage:
                if stats['is_required']:
                    self.logger.log_err(
                        f"❌ CRITICAL: High null percentage for {table_name}.{col}: "
                        f"{stats['null_percentage']:.2f}% (threshold: {max_null_percentage}%)"
                    )
                else:
                    self.logger.log_warning(
                        f"⚠️ High null percentage for {table_name}.{col}: "
                        f"{stats['null_percentage']:.2f}%"
                    )

        # Clean up
        self.duckdb.execute(f"DROP TABLE IF EXISTS {temp_table}")

        return {
            'clean_df': clean_df,
            'null_records_df': null_records_df if len(null_records_df) > 0 else None,
            'null_summary': null_summary,
            'metrics': self.quality_metrics['failed_records'][table_name]
        }

    def _log_null_results(self, table_name: str, batch_id: str, null_summary: Dict,
                          total: int, null_count: int):
        """Log null check results"""
        if null_count == 0:
            self.logger.log_msg(
                f"✅ Null check passed for {table_name} (batch: {batch_id}): {total} records, no nulls in required fields")
        else:
            self.logger.log_warning(
                f"⚠️ Null check for {table_name} (batch: {batch_id}): "
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

    def get_quality_report(self) -> Dict:
        """Return comprehensive quality report"""
        return {
            'checker_type': 'null_checker',
            'total_checks': self.quality_metrics['total_checks_performed'],
            'last_check': self.quality_metrics['check_timestamp'].isoformat() if self.quality_metrics[
                'check_timestamp'] else None,
            'null_percentages': self.quality_metrics['null_percentages'],
            'failed_records_summary': self.quality_metrics['failed_records'],
            'total_quarantined': self.quality_metrics['quarantined_records']
        }