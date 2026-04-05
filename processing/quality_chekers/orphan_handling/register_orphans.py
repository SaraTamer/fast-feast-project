import os
import uuid
import csv
from datetime import datetime

from core.logger import Logger
from db.connections import DatabaseManager
from config.config_loader import Config


class OrphansRegistrar:

    def __init__(self, stage="@%reconciliation_table"):
        self.db = DatabaseManager()
        self.logger = Logger()
        self.config = Config()

        self.snow = self.db.get_snowflake()
        self.stage = stage

        self.max_retries = self.config.load_the_yaml()['rules']['max_retries']

    def register_batch(self, table_name,event_id, rows):


        if not rows:
            return

        file_name = f"retry_batch_{uuid.uuid4()}.csv"
        file_path = f"/tmp/{file_name}"

        with open(file_path, "w", newline="") as f:
            writer = csv.writer(f)
            for event_id, payload in rows:
                writer.writerow([
                    table_name,
                    event_id,
                    0,
                    "PENDING",
                    payload,
                    datetime.utcnow(),
                    datetime.utcnow()
                ])

        cursor = self.snow.cursor()

        try:
            cursor.execute(
                f"PUT file://{file_path} {self.stage} AUTO_COMPRESS=TRUE"
            )
            cursor.execute(f"""
            COPY INTO reconciliation_table
            (
                table_name,
                event_id,
                retry_count,
                status,
                raw_payload,
                created_at,
                updated_at
            )
            FROM {self.stage}/{file_name}
            FILE_FORMAT = (
                TYPE = CSV
                FIELD_DELIMITER = ','
                SKIP_HEADER = 0
            )
            ON_ERROR='CONTINUE'
            """)

            self.logger.log_msg(
                f"{len(rows)} retry records inserted into reconciliation_table"
            )

        finally:
            os.remove(file_path)