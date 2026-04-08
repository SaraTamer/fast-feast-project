from utils.utils import get_file_extension

from .csv_ingest import CSVIngest
from .json_ingest import JSONIngest
import os
import core.logger as logger

class FactoryIngester:
    READERS = {
        "csv": CSVIngest,
        "json": JSONIngest
    }

    def __init__(self, file_path: str, duck_db):
        file_type = get_file_extension(file_path)
        self.file_type = file_type.lower()
        self.audit_logger = logger.AuditLogger()
        self.file_path = file_path
        self.duck_db = duck_db

    def get_reader(self):
        if self.file_type not in self.READERS:
            self.audit_logger.log_err(f"Unsupported file type: {self.file_type}")
            return None
        if not os.path.exists(self.file_path):
            self.audit_logger.log_err(f"File not found: {self.file_path}")
            return None
        return self.READERS[self.file_type](self.file_path, duckdb=self.duck_db)
