# processing/error_batch_writer.py
import json
import uuid
from datetime import datetime
from core.logger import AuditLogger
from db.connections import SnowflakeConnection
from db.warehouse_manager import WarehouseManager


class ErrorBatchWriter:

    def __init__(self, database="FASTFEASTDWH", schema="PUBLIC"):
        self.logger = AuditLogger()
        self.snowflake = SnowflakeConnection().conn
        self.database = database
        self.schema = schema
        self.table_name = "streaming_pipeline_errors"
        self.warehouse_manager = WarehouseManager(self.snowflake)

    def _serialize_row(self, row):
        """Convert a row tuple to JSON-serializable format (same as OrphansRegistrar)."""
        serializable_row = []
        for item in row:
            if isinstance(item, datetime):
                serializable_row.append(item.isoformat())
            elif hasattr(item, '__dict__'):
                serializable_row.append(str(item))
            else:
                serializable_row.append(str(item) if item is not None else None)
        return serializable_row

    def write_batch(self, table_name, batch_id, rows, error_type,
                    error_column, fk_table, is_retryable):
        if not rows:
            return

        with self.warehouse_manager.auto_manage():
            cursor = self.snowflake.cursor()
            try:
                cursor.execute(f"USE DATABASE {self.database}")
                cursor.execute(f"USE SCHEMA {self.schema}")

                insert_count = 0
                for row in rows:
                    # Extract event_id from first column
                    event_id = row[0] if row and len(row) > 0 else None

                    # Serialize the row to JSON-serializable format
                    serializable_row = self._serialize_row(row)

                    # Create payload as JSON string (same as OrphansRegistrar)
                    payload = json.dumps({
                        'row': serializable_row,
                        'error_type': error_type,
                        'error_column': error_column,
                        'fk_table': fk_table,
                        'table_name': table_name
                    })

                    # Use %s placeholders with direct STRING insertion (no PARSE_JSON needed)
                    cursor.execute("""
                        INSERT INTO streaming_pipeline_errors 
                        (error_id, event_id, table_name, batch_id, created_at, 
                         raw_payload, error_type, error_column, fk_table, retryable)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        str(uuid.uuid4()),
                        event_id,
                        table_name,
                        batch_id,
                        datetime.utcnow(),
                        payload,  # Use the JSON string directly
                        error_type,
                        error_column,
                        fk_table,
                        is_retryable
                    ))
                    insert_count += 1

                self.snowflake.commit()
                self.logger.log_msg(f"{insert_count} errors inserted into Snowflake")

            except Exception as e:
                self.snowflake.rollback()
                self.logger.log_err(f"Failed to write error batch to Snowflake: {e}")
                raise
            finally:
                cursor.close()