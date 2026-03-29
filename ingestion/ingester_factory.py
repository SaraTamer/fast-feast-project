from .csv_ingest import CSVIngest
from .json_ingest import JSONIngest
import os
import core.logger as logger

class FactoryIngester:
    READERS = {
        "csv": CSVIngest,
        "json": JSONIngest
    }

    def __init__(self, file_type: str):
        self.file_type = file_type.lower()
        self.audit_logger = logger.AuditLogger()

    def get_reader(self, file_path: str):
        if self.file_type not in self.READERS:
            self.audit_logger.log_err(f"Unsupported file type: {self.file_type}")
            return None
        if not os.path.exists(file_path):
            self.audit_logger.log_err(f"File not found: {file_path}")
            return None
        return self.READERS[self.file_type](file_path)