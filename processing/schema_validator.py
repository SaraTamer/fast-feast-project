import duckdb
import core.logger as logger
from core.alerter import Alert
from config.schema_loader import SchemaLoader
from processing.validators.filename_validator import FilenameValidator
from processing.validators.columns_validator import ColumnsValidator
from processing.validators.datatype_validator import DataTypeValidator
from db.QuarantineWriter import QuarantineWriter

class SchemaValidator:
    def __init__(self, schema_loader: SchemaLoader, quarantine_writer: QuarantineWriter = None):
        self.audit_logger = logger.AuditLogger()
        self.loader = schema_loader
        self.alerter = Alert()
        
        # Workers
        self.filename_validator = FilenameValidator(schema_loader)
        self.columns_validator = ColumnsValidator(schema_loader)
        self.datatype_validator = DataTypeValidator(schema_loader, quarantine_writer)

    def validate_schema(self, file_path: str, relation: duckdb.DuckDBPyRelation):

        # Filename validation
        table_name = self.filename_validator.validate(file_path)
        if not table_name:
            self.audit_logger.log_err(f"Something Wrong: [{file_path}] failed filename validation.")
            self.alerter.alert_mail("Schema Validation Failed", f"Something Wrong: [{file_path}] failed filename validation.")
            return False, relation

        # Missing Columns
        if not self.columns_validator.validate(relation, table_name):
            self.audit_logger.log_err(f"Something Wrong: [{table_name}] failed column checks.")
            self.alerter.alert_mail("Schema Validation Failed", f"Something Wrong: [{table_name}] failed column checks.")
            return False, relation 

        # Data Types
        if not self.datatype_validator.validate(relation, table_name):
            self.audit_logger.log_err(f"Something Wrong: [{table_name}] data types mismatch.")
            self.alerter.alert_mail("Schema Validation Failed", f"Something Wrong: [{table_name}] data types mismatch.")
            return False, relation
        
        # If it survives all 3 workers
        self.audit_logger.log_msg(f"ALL CHECKS PASSED: {table_name} data is pure and ready!")
        return relation, False
