import json
import duckdb
from config.config_loader import Config
from db.connections import DatabaseManager
from core.logger import Logger
from .fact_insertion import FactReplayService

class RetryService:

    def __init__(self):

        self.db = DatabaseManager()
        self.logger = Logger()
        self.config = Config()

        self.snow = self.db.get_snowflake()
        self.duck = self.db.get_duckdb()
        self.replayer = FactReplayService()


    def retry(self, dim_name, join_key, table_name):

        cursor = self.snow.cursor()
        rows = cursor.execute(
            """
            SELECT event_id, raw_payload, retry_count, max_retries
            FROM reconciliation_table
            WHERE table_name=%s
            AND status IN ('PENDING','RETRYING')
            """,
            (table_name,)
        ).fetchall()

        if not rows:
            self.logger.log_msg(f"No retry records for {table_name}")
            return


        for event_id, payload, retry_count, max_retries in rows:
            data = json.loads(payload)
            value = data.get(join_key)

            if value is None:
                self.logger.log_err(f"{event_id} missing {join_key}")
                continue

            found = self.duck.execute(
                f"""
                SELECT 1
                FROM {dim_name}
                WHERE {join_key}=?
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
            if retry_count >= max_retries:
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