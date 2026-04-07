import json
import duckdb
from config.config_loader import Config
from db.connections import SnowflakeConnection
from db.connections import DuckDBConnection
from core.logger import AuditLogger
from .fact_insertion import FactReplayService
from processing.quality_chekers.orphan_handling.fact_insertion import FactReplayService
from db.warehouse_manager import WarehouseManager
class RetryService:

    def __init__(self):

        self.logger = AuditLogger()
        self.config = Config()

        self.snow = SnowflakeConnection().conn
        self.duck = DuckDBConnection().conn
        self.replayer = FactReplayService()
        self.warehouse_manager = WarehouseManager(self.snow, "COMPUTE_WH")

    def retry(self, dim_name, join_key, table_name):

        with self.warehouse_manager.auto_manage():
            try:
                cursor = self.snow.cursor()
                rows = cursor.execute(
                    """
                    SELECT event_id, raw_payload, retry_count
                    FROM reconciliation_table
                    WHERE table_name=%s
                    AND status IN ('PENDING','RETRYING')
                    """,
                    (table_name,)
                ).fetchall()

                if not rows:
                    self.logger.log_msg(f"No retry records for {table_name}")
                    return


                for event_id, payload, retry_count in rows:
                    data = json.loads(payload)
                    value = data.get("fk_value")
                    fk_column = data.get("fk_column")
                    dim_name = data.get("dim_name")
                    
                    if value is None:
                        self.logger.log_err(f"{event_id} missing {fk_column}")
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
            except Exception as e:
                self.logger.log_err(f"Retry failed for {table_name}: {e}")