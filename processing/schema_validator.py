import duckdb
import yaml
import os
from db.metadata_db import MetadataDB
import core.logger as logger

audit_logger = logger.AuditLogger()
class SchemaValidator:
    def __init__(self, schemas_path: str):
        self.db = MetadataDB()
        self.conn = self.db.conn

    def prepare_validation(self):
        pass

    def validate_filename(self, file_path: str):
        base_name = os.path.basename(file_path)
        table_name = base_name.split('.')[0].lower()
        if table_name not in self.db.get_file_names():
            audit_logger.log_err(f"Failed Validation {file_path}...")
            return False
        audit_logger.log_msg(f"Successfully Validate {file_path}...")
        return table_name

    def validate_schema(self, file_path: str, df: pd.DataFrame):
        table_name = self.validate_filename(file_path)
        
        if not table_name:
            return None, df 
        if not self.validate_required_col(df, table_name):
            audit_logger.log_err(f"CRITICAL: {table_name} schema validation failed (Missing Columns). File dropped.")
            return None, df 
        valid_df, invalid_df = self.validate_dataTypes(df, table_name)
        
        if not invalid_df.empty:
            audit_logger.log_err(f"SCHEMA WARNING: {len(invalid_df)} rows in {table_name} failed type validation and were quarantined.")
        
        audit_logger.log_msg(f"SUCCESS: {table_name} schema validation complete. {len(valid_df)} rows passed.")
        return valid_df, invalid_df

    def validate_required_col(self, df):
        pass

    def validate_dataTypes(self, df):
        pass
