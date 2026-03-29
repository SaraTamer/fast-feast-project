import logging
import os 
import sys
from config.config_loader import Config
from logging.handlers import RotatingFileHandler
class AuditLogger:
    def __init__(self):
        cfg=Config()
        log_file=cfg.log_file_path()
        err_file=cfg.err_file_path()
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        os.makedirs(os.path.dirname(err_file), exist_ok=True)

        
        self.logger =logging.getLogger("AuditLogger")
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:

            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)s |%(threadName)s | %(name)s | %(message)s"
            )
            file_handler=RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5)
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)

            error_handler=RotatingFileHandler(err_file, maxBytes=5*1024*1024, backupCount=5)
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(formatter)

            console_handler=logging.StreamHandler()
            console_handler.setFormatter(formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(error_handler)
            self.logger.addHandler(console_handler)
        
    def log_msg(self, msg: str):
        self.logger.info(msg)

    def log_err(self, msg: str):
        self.logger.error(msg)
    def log_warning(self, msg: str):
        self.logger.warning(msg)