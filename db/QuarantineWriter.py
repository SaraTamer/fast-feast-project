import uuid
import json
from datetime import datetime
import core.logger as logger
from db.connections import SnowflakeConnection


class QuarantineWriter:
    """
    Writes bad rows to Snowflake quarantine tables.
    Falls back to local logging if no connection.
    """

    def __init__(self, snowflake_conn=None, max_retries: int = 5):
        self.sf_conn = snowflake_conn or SnowflakeConnection()
        self.max_retries = max_retries
        self.audit_logger = logger.AuditLogger()

    def quarantine(self, bad_rows_df, table_name, primary_key,
                   batch_id, error_type="TYPE_MISMATCH", retryable=False):
        """
        Write bad rows to:
          1. streaming_pipeline_errors
          2. reconciliation_table
        """
        if bad_rows_df is None or len(bad_rows_df) == 0:
            return

        if self.sf_conn is None:
            self._log_locally(bad_rows_df, table_name, primary_key)
            return

        self._write_to_snowflake(
            bad_rows_df, table_name, primary_key,
            batch_id, error_type, retryable
        )

    def _write_to_snowflake(self, bad_rows_df, table_name, primary_key,
                             batch_id, error_type, retryable):
        now = datetime.utcnow()
        cursor = self.sf_conn.conn.cursor()

        try:
            for _, row in bad_rows_df.iterrows():
                event_id = str(row.get(primary_key, "unknown"))
                failed_columns = row.get("__failed_columns", [])
                raw_payload = self._row_to_payload(row)

                print("1")

                # One error record per failed column
                for failed_col in failed_columns:
                    self._insert_error(
                        cursor, table_name, event_id, batch_id,
                        raw_payload, error_type, failed_col,
                        retryable, now
                    )

                print("2")

                # One reconciliation record per row
                self._upsert_reconciliation(
                    cursor, table_name, event_id,
                    raw_payload, now
                )
            print("3")

            self.sf_conn.conn.commit()
            self.audit_logger.log_msg(
                f"Quarantined {len(bad_rows_df)} rows for '{table_name}'"
            )

            print()

        except Exception as e:
            self.sf_conn.conn.rollback()
            self.audit_logger.log_err(f"Quarantine write failed: {e}")

        finally:
            cursor.close()

    def _insert_error(self, cursor, table_name, event_id, batch_id,
                      raw_payload, error_type, error_column, retryable, now):
        cursor.execute("""
            INSERT INTO streaming_pipeline_errors (
                error_id, event_id, table_name, batch_id,
                created_at, raw_payload,
                error_type, error_column, fk_table, retryable
            ) VALUES (
                %(error_id)s, %(event_id)s, %(table_name)s, %(batch_id)s,
                %(created_at)s, %(raw_payload)s,
                %(error_type)s, %(error_column)s, NULL, %(retryable)s
            )
        """, {
            "error_id": str(uuid.uuid4()),
            "event_id": event_id,
            "table_name": table_name,
            "batch_id": batch_id,
            "created_at": now,
            "raw_payload": raw_payload,
            "error_type": error_type,
            "error_column": error_column,
            "retryable": retryable,
        })


    # should be added to dag later
    def _upsert_reconciliation(self, cursor, table_name, event_id,
                                raw_payload, now):
        cursor.execute(f"""
            MERGE INTO reconciliation_table t
            USING (SELECT
                %(table_name)s AS table_name,
                %(event_id)s AS event_id
            ) s
            ON t.table_name = s.table_name AND t.event_id = s.event_id
            WHEN MATCHED THEN UPDATE SET
                retry_count = t.retry_count + 1,
                status = CASE
                    WHEN t.retry_count + 1 >= {self.max_retries} THEN 'DEAD'
                    ELSE 'RETRYING'
                END,
                raw_payload = %(raw_payload)s,
                last_seen_at = %(now)s,
                updated_at = %(now)s
            WHEN NOT MATCHED THEN INSERT (
                table_name, event_id, retry_count, status,
                raw_payload, last_seen_at, updated_at
            ) VALUES (
                %(table_name)s, %(event_id)s, 0, 'PENDING',
                %(raw_payload)s), %(now)s, %(now)s
            )
        """, {
            "table_name": table_name,
            "event_id": event_id,
            "raw_payload": raw_payload,
            "now": now,
        })

    def _row_to_payload(self, row) -> str:
        """Convert a DataFrame row to JSON string, excluding internal columns."""
        return {
            k: (v if isinstance(v, (int, float, bool)) or v is None else str(v))
            for k, v in row.items()
            if not k.startswith("__")
        }

    def _log_locally(self, bad_rows_df, table_name, primary_key):
        """Fallback when no Snowflake connection."""
        self.audit_logger.log_warning(
            f"No Snowflake conn — logging {len(bad_rows_df)} quarantined rows locally"
        )
        for _, row in bad_rows_df.head(10).iterrows():
            event_id = row.get(primary_key, "unknown")
            failed_cols = row.get("__failed_columns", [])
            self.audit_logger.log_err(
                f"  QUARANTINE: {table_name} | "
                f"id={event_id} | "
                f"failed={failed_cols}"
            )
        if len(bad_rows_df) > 10:
            self.audit_logger.log_err(
                f"  ... and {len(bad_rows_df) - 10} more"
            )