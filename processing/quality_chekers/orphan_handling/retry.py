import json
from config.config_loader import Config
from db.connections import SnowflakeConnection
from core.logger import AuditLogger
from .fact_insertion import FactReplayService
from processing.quality_chekers.orphan_handling.fact_insertion import FactReplayService
from db.warehouse_manager import WarehouseManager
class RetryService:

    def __init__(self, duckdb_conn):

        self.logger = AuditLogger()
        self.config = Config()

        self.snow = SnowflakeConnection().conn
        self.duck = duckdb_conn.conn
        self.replayer = FactReplayService()
        self.warehouse_manager = WarehouseManager(self.snow, "COMPUTE_WH")

    def retry(self, dim_name, table_name):

        with self.warehouse_manager.auto_manage():
            try:
                cursor = self.snow.cursor()
                rows = cursor.execute(
                    """
                   SELECT event_id, raw_payload, retry_count, updated_at
                    FROM reconciliation_table
                    WHERE table_name=%s
                    AND status IN ('PENDING','RETRYING')
                    """,
                    (table_name,)
                ).fetchall()

                if not rows:
                    self.logger.log_msg(f"No retry records for {table_name}")
                    return

                from datetime import datetime, timedelta
                
                for event_id, payload, retry_count, updated_at in rows:
                    data = json.loads(payload)
                    value = data.get("fk_value")
                    fk_column = data.get("fk_column")
                    dim_name = data.get("dim_name")
                    
                    if value is None:
                        self.logger.log_warning(f"{event_id} still missing {fk_column}")
                        continue

                    found = self.duck.execute(
                        f"""
                        SELECT 1
                        FROM {dim_name}
                        WHERE {fk_column}=?
                        LIMIT 1
                        """,
                        [value]
                    ).fetchone()

                    if found:
                        self.replayer.insert_fact(table_name, payload)
                        cursor.execute(
                        """
                        UPDATE reconciliation_table
                        SET status='RESOLVED',
                            updated_at=CURRENT_TIMESTAMP
                        WHERE event_id=%s
                        AND table_name=%s
                        """,
                        (event_id, table_name)
                    )
                        self.logger.log_msg(f"Event {event_id} replayed into {table_name}")
                        self.logger.log_msg(f"Event {event_id} resolved for {table_name}")
                    else:
                        # Only increment retry_count once per batch cycle
                        # If updated_at was just updated (within 10 seconds), skip incrementing
                        now = datetime.utcnow()
                        time_since_update = (now - updated_at).total_seconds() if updated_at else float('inf')
                        
                        if time_since_update > 2000:  # Only increment if not updated in last 10 seconds
                            retry_count += 1
                            status = "RETRYING"
                            if retry_count >= self.config.max_retries():
                                status = "DEAD"

                            cursor.execute(
                                """
                                UPDATE reconciliation_table
                                SET retry_count=%s,
                                    status=%s,
                                    updated_at=CURRENT_TIMESTAMP
                                WHERE event_id=%s
                                AND table_name=%s
                                """,
                                (retry_count, status, event_id, table_name)
                            )
                            self.logger.log_msg(f"{event_id} retry={retry_count} status={status}")
                        else:
                            # Record was already retried in this batch cycle, just log
                            self.logger.log_msg(f"{event_id} already retried in this cycle, skipping increment")
            except Exception as e:
                self.logger.log_err(f"Retry failed for {table_name}: {e}")