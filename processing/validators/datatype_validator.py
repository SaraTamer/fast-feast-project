import duckdb
import core.logger as logger
from processing.validators.base_validator import BaseValidator
from config.schema_loader import SchemaLoader
from config.type_mapping import duckdb_type_to_yaml
from db.RowSeparator import RowSeparator
from db.QuarantineWriter import QuarantineWriter


class DataTypeValidator(BaseValidator):
    def __init__(self, loader: SchemaLoader, quarantine_writer: QuarantineWriter = None):
        self.loader = loader
        self.audit_logger = logger.AuditLogger()
        self.separator = RowSeparator()
        self.quarantine = quarantine_writer or QuarantineWriter()

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
