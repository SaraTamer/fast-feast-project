import duckdb
import core.logger as logger
from processing.validators.base_validator import BaseValidator
from config.schema_loader import SchemaLoader
from config.type_mapping import duckdb_type_to_yaml
from db.RowSeparator import RowSeparator
from processing.error_batch_writer import ErrorBatchWriter


class DataTypeValidator(BaseValidator):
    def __init__(self, loader: SchemaLoader):
        self.loader = loader
        self.audit_logger = logger.AuditLogger()
        self.separator = RowSeparator()
        self.error_writer = ErrorBatchWriter()

    def validate(self, relation: duckdb.DuckDBPyRelation, table_name: str,
                 batch_id: str = None):
        """
        Returns:
            (True,  relation)       → all rows valid
            (True,  clean_relation) → some quarantined, rest clean
            (False, None)           → nothing left
        """
        yaml_types = self.loader.get_data_types(table_name)
        primary_key = self.loader.get_primary_key(table_name)

        # ── Step 1: Column-level check ──
        mismatched = self._find_mismatches(
            relation.columns, relation.dtypes, yaml_types, table_name
        )

        if not mismatched:
            self.audit_logger.log_msg(
                f"PASSED: '{table_name}' all types match"
            )
            return True, relation

        # ── Step 2: Row-level separation ──
        self.audit_logger.log_warning(
            f"'{table_name}': {len(mismatched)} type mismatches -> checking rows"
        )

        clean_relation, bad_rows_df = self.separator.separate(
            relation, mismatched
        )

        # ── Step 3: Quarantine bad rows ──
        if bad_rows_df is not None and len(bad_rows_df) > 0:
            rows = []

            for idx, row in bad_rows_df.iterrows():
                event_id = None

                # Try extract PK
                if primary_key and primary_key in row and row[primary_key] is not None:
                    event_id = str(row[primary_key])
                else:
                    event_id = f"type_{idx}"

                rows.append((
                    event_id,
                    row.to_dict()
                ))
                table_name=table_name,
                primary_key=primary_key,
                batch_id=batch_id,
                error_type="TYPE_MISMATCH",
                retryable=False,
            )

        # ── Step 4: Check remaining ──
        if clean_relation is None or clean_relation.count("*").fetchone()[0] == 0:
            self.audit_logger.log_err(
                f"FAILED: '{table_name}' — all rows quarantined"
            )
            return False, None

        clean_count = clean_relation.count("*").fetchone()[0]
        self.audit_logger.log_msg(
            f"PASSED: '{table_name}' — {clean_count} clean rows"
        )
        return True, clean_relation

    def _find_mismatches(self, columns, dtypes, yaml_types, table_name):
        mismatched = []

        for col_name, duckdb_type in zip(columns, dtypes):
            if col_name not in yaml_types:
                continue

            actual = duckdb_type_to_yaml(duckdb_type)
            expected = yaml_types[col_name].lower()

            if actual != expected:
                mismatched.append({
                    "column": col_name,
                    "expected_yaml": expected,
                    "actual_duckdb": str(duckdb_type),
                    "actual_yaml": actual,
                })
                self.audit_logger.log_warning(
                    f"  {table_name}.{col_name}: "
                    f"expected '{expected}', got '{duckdb_type}' (='{actual}')"
                )

        return mismatched