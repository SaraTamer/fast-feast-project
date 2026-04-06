import os

from .base_ingester import Ingester
import duckdb as dd
import core.logger as logger

class CSVIngest(Ingester):
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.audit_logger= logger.AuditLogger()

    def ingest(self):
        self.audit_logger.log_msg(f"Ingesting data from {self.file_path}...")
        try:
            data = dd.read_csv(self.file_path)
            if self.empty(data):
                self.audit_logger.log_warning(f"{self.file_path} is empty")
                return {'data': data, 'is_empty': True}
            else:
                self.audit_logger.log_msg("Data ingested successfully!")
            return {'data':data, 'is_empty':False}
        except Exception as e:
            self.audit_logger.log_err(f"An error occurred while ingesting data: {e}")
            return {'data':None, 'is_empty':False}

    def get_table_name(self) -> str:
        """Extract table name from file path"""
        filename = os.path.basename(self.file_path)
        table_name = filename.split('.')[0]
        return table_name

