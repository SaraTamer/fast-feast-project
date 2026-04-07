import duckdb
import core.logger as logger
from config.schema_loader import SchemaLoader
from processing.validators.filename_validator import FilenameValidator
from processing.validators.columns_validator import ColumnsValidator
from processing.validators.datatype_validator import DataTypeValidator
from db.QuarantineWriter import QuarantineWriter

class SchemaValidator:
    def __init__(self, schema_loader: SchemaLoader, quarantine_writer: QuarantineWriter = None):
        self.audit_logger = logger.AuditLogger()
        self.loader = schema_loader
        
        # Workers
        self.filename_validator = FilenameValidator(schema_loader)
        self.columns_validator = ColumnsValidator(schema_loader)
        self.datatype_validator = DataTypeValidator(schema_loader, quarantine_writer)

    def validate_schema(self, file_path: str, relation: duckdb.DuckDBPyRelation,
                        batch_id: str = None):

        # Filename validation
        table_name = self.filename_validator.validate(file_path)
        if not table_name:
            self.audit_logger.log_err(f"Pipeline Halted: [{file_path}] failed filename validation.")
            return False, relation

        # Missing Columns
        if not self.columns_validator.validate(relation, table_name):
            self.audit_logger.log_err(f"Pipeline Halted: [{table_name}] failed column checks.")
            return False, relation 

        # Data Types
        # 3. Data types + quarantine
        is_valid, result = self.datatype_validator.validate(
            relation, table_name, batch_id
        )
        if not is_valid:
            self.audit_logger.log_err(f"Pipeline Halted: [{table_name}] data types mismatch.")
            return False, relation
        
        # If it survives all 3 workers
        self.audit_logger.log_msg(f"ALL CHECKS PASSED: {table_name} data is pure and ready!")
        return result, self.loader.get_columns_meta(table_name)
