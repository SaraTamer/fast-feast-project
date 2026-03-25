from .csv_ingest import CSVIngest
from .json_ingest import JSONIngest
import os
import core.logger as logger

class FactoryIngester:
     def __init__(self, file_type: str):
        self.file_type = file_type

     def get_reader(self, file_path: str):
        audit_logger = logger.AuditLogger()
        if self.file_type=="csv":
            return CSVIngest(file_path)
        elif self.file_type=="json":
            return JSONIngest(file_path)
        else:
            audit_logger.log_err(f"Unsupported file type: {self.file_type}")