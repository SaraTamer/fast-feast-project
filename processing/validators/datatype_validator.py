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

    def validate(self, relation: duckdb.DuckDBPyRelation, table_name: str) -> bool:
        # Cross-checks DuckDB's native data type inference against schema.yaml definitions.
        expected_types = self.loader.get_data_types(table_name)
        
        actual_types = relation.dtypes
        columns = relation.columns
        
        # Loop over every column and check its duckdb type against YAML
        for col_name, actual_type in zip(columns, actual_types):
            actual_type_str = str(actual_type).upper()
            
            if col_name in expected_types:
                target_type = expected_types[col_name].lower()
                
                # Check mapping rules
                if target_type == "int" and "INT" not in actual_type_str:
                    self.audit_logger.log_err(f"Something Wrong: {table_name}.{col_name} is {actual_type_str}, expected INT.")
                    return False
                    
                if target_type == "float" and "DOUBLE" not in actual_type_str and "FLOAT" not in actual_type_str:
                    self.audit_logger.log_err(f"Something Wrong: {table_name}.{col_name} is {actual_type_str}, expected FLOAT.")
                    return False
                    
        self.audit_logger.log_msg(f"SUCCESS: '{table_name}' passed Data Type Validation.")
        return True
