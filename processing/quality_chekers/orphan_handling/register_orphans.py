import json
from datetime import datetime

from core.logger import AuditLogger
from db.connections import DuckDBConnection,SnowflakeConnection
from config.config_loader import Config
from db.warehouse_manager import WarehouseManager

class OrphansRegistrar:

    def __init__(self):
        self.duckdb = DuckDBConnection()
        self.logger = AuditLogger()
        self.config = Config()

        self.snow = SnowflakeConnection().conn
        self.warehouse_manager = WarehouseManager(self.snow, "COMPUTE_WH")

        self.max_retries = self.config.load_the_yaml()['rules']['max_retries']

    def register_batch(self, table_name, event_id, rows):
        if not rows:
            return

        # Use warehouse manager for Snowflake operations
        with self.warehouse_manager.auto_manage():
            cursor = self.snow.cursor()
            try:
                insert_count = 0
                # Convert Windows path to forward slashes
                for row_data in rows:  # row_data is a tuple of (r, fk_column, fk_value, dim_name)
                    # Extract the four elements
                    r, fk_column, fk_value, dim_name = row_data

                    # Extract event_id from the first column
                    evt_id = r[0] if r and len(r) > 0 else None

                    # Convert tuple to serializable format (convert datetime to string)
                    serializable_row = []
                    for item in r:
                        if isinstance(item, datetime):
                            serializable_row.append(item.isoformat())
                        else:
                            serializable_row.append(str(item) if item is not None else None)

                    # Create payload as JSON string
                    payload = json.dumps({
                        'row': serializable_row,
                        'fk_column': fk_column,
                        'fk_value': str(fk_value) if fk_value is not None else None,
                        'dim_name': dim_name,
                        'table_name': table_name
                    })

                    cursor.execute("""
                                    INSERT INTO reconciliation_table 
                                    (table_name, event_id, retry_count, status, raw_payload, created_at, updated_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """, (
                        table_name,
                        evt_id,
                        0,
                        "PENDING",
                        payload,
                        datetime.utcnow().isoformat(),
                        datetime.utcnow().isoformat()
                    ))
                    insert_count += 1

                self.logger.log_msg(f"{insert_count} retry records inserted into reconciliation_table")


            except Exception as e:
                self.logger.log_err(f"Failed to register retry batch: {e}")
                raise
            finally:
                cursor.close()