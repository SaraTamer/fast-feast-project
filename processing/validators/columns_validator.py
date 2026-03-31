import duckdb
import core.logger as logger
from processing.validators.base_validator import BaseValidator
from config.schema_loader import SchemaLoader

class ColumnsValidator(BaseValidator):
    def __init__(self, loader: SchemaLoader):
        self.loader = loader
        self.audit_logger = logger.AuditLogger()

    def validate(self, relation: duckdb.DuckDBPyRelation, table_name: str) -> bool:
        # get column arrays from YAML and check the duckdb relation against them.
        required_cols = self.loader.get_required_cols(table_name)
        current_cols = relation.columns

        for col in required_cols:
            if col not in current_cols:
                self.audit_logger.log_err(f"CRITICAL: '{table_name}' is missing required field: '{col}'. Dropping file.")
                return False
                
        self.audit_logger.log_msg(f"SUCCESS: '{table_name}' passed Columns Validation.")
        return True
