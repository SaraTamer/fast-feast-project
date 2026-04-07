import os

from .base_ingester import Ingester
import duckdb as dd
import core.logger as logger
from db.connections import DuckDBConnection

class CSVIngest(Ingester):
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.audit_logger= logger.AuditLogger()
        self.conn = DuckDBConnection().conn

    def ingest(self):
        self.audit_logger.log_msg(f"Ingesting data from {self.file_path}...")
        try:
            data = self.conn.read_csv(self.file_path)
            if self.empty(data):
                self.audit_logger.log_warning(f"{self.file_path} is empty")
                return {'data': data, 'is_empty': True}
            else:
                self.audit_logger.log_msg("Data ingested successfully!")
            return {'data':data, 'is_empty':False}
        except Exception as e:
            self.audit_logger.log_err(f"An error occurred while ingesting data: {e}")
            return {'data':None, 'is_empty':False}


