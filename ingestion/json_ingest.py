from .base_ingester import Ingester
import core.logger as logger
import json
import pandas as pd
class JSONIngest(Ingester):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def ingest(self):
        audit_logger = logger.AuditLogger()
        audit_logger.log_msg(f"ingesting data from {self.file_path}...")
        try:
            with open(self.file_path,'r') as f:
                data=json.load(f)
            if not data:
                audit_logger.log_warning(f"{self.file_path} is empty")
            else:
                audit_logger.log_msg("Data ingested successfully!")
            data=pd.json_normalize(data)
            return data
        except Exception as e:
            audit_logger.log_err(f"An error occurred while ingesting data: {e}")
            raise RuntimeError(f"Ingestion failed: {e}")

