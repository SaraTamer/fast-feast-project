from .base_ingester import Ingester
import pandas as pd

class CSVIngest(Ingester):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def ingest(self):
        logger= logger.AuditLogger()
        logger.log_msg(f"Ingesting data from {self.file_path}...")
        try:
            data = pd.read_csv(self.file_path)
            if data.empty:
                logger.log_err(f"{self.file_path} is empty")
            else:
                logger.log_msg("Data ingested successfully!")
            return data
        except Exception as e:
            logger.log_err(f"An error occurred while ingesting data: {e}")
