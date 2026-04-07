import os
import core.logger as logger
from processing.validators.base_validator import BaseValidator
from config.schema_loader import SchemaLoader

class FilenameValidator(BaseValidator):
    def __init__(self, loader: SchemaLoader):
        self.loader = loader
        self.audit_logger = logger.AuditLogger()

    def validate(self, file_path: str) -> str:
        # Check tables names and return false if invalid
        base_name = os.path.basename(file_path)
        table_name = base_name.split('.')[0].lower()
        
        if table_name not in self.loader.get_table_names():
            self.audit_logger.log_err(f"Something Wrong: Unrecognized table name '{table_name}' from file {file_path}")
            return False
            
        self.audit_logger.log_msg(f"SUCCESS: Recognized table: {table_name}")
        return table_name