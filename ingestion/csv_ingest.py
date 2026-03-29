from .base_ingester import Ingester
import pandas as pd
import core.logger as logger

class CSVIngest(Ingester):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def ingest(self):
        audit_logger= logger.AuditLogger()
        audit_logger.log_msg(f"Ingesting data from {self.file_path}...")
        try:
            data = pd.read_csv(self.file_path)
            if data.empty:
                audit_logger.log_warning(f"{self.file_path} is empty")
            else:
                audit_logger.log_msg("Data ingested successfully!")
            return data
        except Exception as e:
            audit_logger.log_err(f"An error occurred while ingesting data: {e}")
            raise RuntimeError(f"Ingestion failed: {e}")