import uuid
import csv
from datetime import datetime
from core.logger import AuditLogger
from db.connections import DuckDBConnection, SnowflakeConnection
import os
import tempfile
from db.warehouse_manager import WarehouseManager

class ErrorBatchWriter:

    def __init__(self, database="FASTFEASTDWH", schema="PUBLIC",
                 stage="streaming_pipeline_errors_stage"):

        self.duckdb = DuckDBConnection()
        self.logger = AuditLogger()
        self.snowflake = SnowflakeConnection().conn
        self.database = database
        self.schema = schema

        # Create fully qualified stage name
        self.stage_name = stage
        self.fully_qualified_stage = f"{self.database}.{self.schema}.{self.stage_name}"
        self.table_name = "streaming_pipeline_errors"
        self.stage_ref = f"@{self.fully_qualified_stage}"
        self.warehouse_manager = WarehouseManager(self.snowflake)




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

        # Use tempfile for cross-platform compatibility
        with tempfile.NamedTemporaryFile(
                mode='w',
                suffix='.csv',
                prefix=f'errors_{table_name}_{batch_id}_',
                delete=False,
                newline=''
        ) as temp_file:
            file_path = temp_file.name
            writer = csv.writer(temp_file)

            for row in rows:
                writer.writerow([
                    str(uuid.uuid4()),
                    row[0],
                    table_name,
                    batch_id,
                    datetime.utcnow().isoformat(),
                    str(row),
                    error_type,
                    error_column,
                    fk_table,
                    is_retryable
                ])
        with self.warehouse_manager.auto_manage():
            cursor = None
            try:
                cursor = self.snowflake.cursor()

                # Upload to Snowflake stage
                cursor.execute(f"USE DATABASE {self.database}")
                cursor.execute(f"USE SCHEMA {self.schema}")

                # Convert Windows path to forward slashes for Snowflake
                snowflake_path = str(file_path).replace('\\', '/')

                # PUT file to stage (using fully qualified stage name)
                put_command = f"PUT file://{snowflake_path} {self.stage_ref} AUTO_COMPRESS=TRUE"
                cursor.execute(put_command)

                # Get just the filename for the COPY command
                file_name = os.path.basename(file_path)

                # Bulk copy into Snowflake
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
                       FROM {self.stage_ref}/{file_name}.gz
                       FILE_FORMAT = (
                           TYPE = CSV
                           FIELD_DELIMITER = ','
                           SKIP_HEADER = 0
                       )
                       ON_ERROR='CONTINUE'
                   """)

                self.logger.log_msg(f"{len(rows)} errors copied into Snowflake")

            except Exception as e:
                self.logger.log_err(f"Failed to write error batch to Snowflake: {e}")
                raise

            finally:
                # Clean up temp file
                if os.path.exists(file_path):
                    os.remove(file_path)
                if cursor:
                    cursor.close()