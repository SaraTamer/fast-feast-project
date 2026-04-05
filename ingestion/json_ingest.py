from .base_ingester import Ingester
import core.logger as logger
import duckdb as dd
class JSONIngest(Ingester):
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.audit_logger = logger.AuditLogger()

    def ingest(self):
        self.audit_logger.log_msg(f"ingesting data from {self.file_path}...")
        try:
            data = dd.read_json(self.file_path, ignore_errors=True)
            if self.empty(data):
                self.audit_logger.log_warning(f"{self.file_path} is empty")
            else:
                self.audit_logger.log_msg("Data ingested successfully!")
            return data
        except Exception as e:
            self.audit_logger.log_err(f"An error occurred while ingesting data: {e}")
            return None

