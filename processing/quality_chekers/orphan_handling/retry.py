import duckdb
from config.config_loader import Config
from db.connections import DatabaseManager
from core.logger import Logger
import json

class RetryService:

    def __init__(self):
        self.db = DatabaseManager()
        self.logger = Logger()
        self.config = Config()

        self.snow = self.db.get_snowflake()
        self.duck = self.db.get_duckdb()

        self.max_retries = self.config.load_the_yaml()['rules']['max_retries']

    def retry(self, dim_df, join_key, table_name):

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
                return

            self.duck.register("dim_table", dim_df)
            for event_id, payload, retry_count in rows:

                value = json.loads(payload)[join_key]
                found = self.duck.execute(
                f"SELECT * FROM dim_table WHERE {join_key}=?",
                [value]
            ).fetchall()

                if found:
                    cursor.execute(
                        """
                        UPDATE reconciliation_table
                        SET status='RESOLVED', updated_at=CURRENT_TIMESTAMP
                        WHERE event_id=%s AND table_name=%s
                        """,
                        (event_id, table_name)
                    )
                    self.logger.log_msg(f"Event {event_id} in {table_name} resolved after retry")

                else:
                    retry_count += 1
                    status = "RETRYING"
                    if retry_count >= self.max_retries:
                        status = "DEAD"

                    cursor.execute(
                        """
                        UPDATE reconciliation_table
                        SET retry_count=%s, status=%s, updated_at=CURRENT_TIMESTAMP
                        WHERE event_id=%s AND table_name=%s
                        """,
                        (retry_count, status, event_id, table_name)
                    )
                    self.logger.log_msg(f"Event {event_id} in {table_name} retry count updated to {retry_count} with status {status}")