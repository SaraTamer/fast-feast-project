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


        required_lower = [c.lower() for c in required_cols]
        actual_lower = [c.lower() for c in current_cols]


        # Count check
        if len(actual_lower) != len(required_lower):
            self.audit_logger.log_err(
                f"COLUMN COUNT mismatch in '{table_name}': "
                f"expected {len(required_lower)}, got {len(actual_lower)}"
            )
            return False
        
        # check for missing columns
        for col in required_lower:
            if col not in actual_lower:
                self.audit_logger.log_err(f"CRITICAL: '{table_name}' is missing required field: '{col}'. Dropping file.")
                return False
            
        # check for extra columns
        for col in actual_lower:
            if col not in required_lower:
                self.audit_logger.log_err(f"CRITICAL: '{table_name}' has extra column: '{col}'. Dropping file.")
                return False
                
        self.audit_logger.log_msg(f"SUCCESS: '{table_name}' passed Columns Validation.")
        return True
