import uuid
import csv
from datetime import datetime
from core.logger import AuditLogger
from db.connections import DuckDBConnection, SnowflakeConnection
import os


class ErrorBatchWriter:

    def __init__(self, stage="@%streaming_pipeline_errors_stage"):

        self.duckdb = DuckDBConnection()
        self.logger = AuditLogger()
        self.snowflake = SnowflakeConnection().conn
        self.stage = stage


    def write_batch(
        self,
        table_name,
        batch_id,
        rows,
        error_type,
        error_column,
        fk_table,
        is_retryable
    ):

        if not rows:
            return

        file_name = f"errors_{table_name}_{batch_id}.csv"
        file_path = f"/tmp/{file_name}"
        with open(file_path, "w", newline="") as f:

            writer = csv.writer(f)

            for row in rows:

                writer.writerow([
                    str(uuid.uuid4()),
                    row[0],                
                    table_name,
                    batch_id,
                    datetime.utcnow(),
                    str(row),
                    error_type,
                    error_column,
                    fk_table,
                    is_retryable
                ])


        cursor = self.snowflake.cursor()
        try:
            cursor.execute(
                f"PUT file://{file_path} {self.stage} AUTO_COMPRESS=TRUE"
            )


            # bulk copy
            cursor.execute(f"""
                COPY INTO streaming_pipeline_errors (
                    error_id,
                    event_id,
                    table_name,
                    batch_id,
                    created_at,
                    raw_payload,
                    error_type,
                    error_column,
                    fk_table,
                    retryable
                )
                FROM {self.stage}/{file_name}.gz
                FILE_FORMAT = (
                    TYPE = CSV
                    FIELD_DELIMITER = ','
                    SKIP_HEADER = 0
                )
                ON_ERROR='CONTINUE'
            """)

            self.logger.log_msg(
                f"{len(rows)} errors copied into Snowflake"
            )

        finally:

            os.remove(file_path)