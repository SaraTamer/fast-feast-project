from contextlib import contextmanager
from core.logger import AuditLogger
import time

class WarehouseManager:
    """Manages Snowflake warehouse state automatically."""

    def __init__(self, connection, warehouse_name="COMPUTE_WH"):
        self.conn = connection
        self.warehouse_name = warehouse_name
        self.logger = AuditLogger()
        self.was_suspended = False

    def resume_if_needed(self):
        """Resume warehouse if it's suspended."""
        cursor = self.conn.cursor()
        try:
            # Use uppercase for warehouse name (Snowflake stores names in uppercase)
            warehouse_upper = self.warehouse_name.upper()
            cursor.execute(f"SHOW WAREHOUSES LIKE '{warehouse_upper}'")
            result = cursor.fetchone()

            if result:
                # Get column names to find state dynamically
                cursor.execute("SHOW WAREHOUSES")
                column_names = [desc[0] for desc in cursor.description]
                state_index = column_names.index('state') if 'state' in column_names else 6

                state = result[state_index]
                if state == 'SUSPENDED':
                    self.logger.log_msg(f"Resuming warehouse {warehouse_upper}...")
                    cursor.execute(f"ALTER WAREHOUSE {warehouse_upper} RESUME")
                    time.sleep(2)
                    self.was_suspended = True
                    self.logger.log_msg(f"Warehouse {warehouse_upper} resumed")
                else:
                    self.was_suspended = False
        except Exception as e:
            self.logger.log_err(f"Error checking warehouse state: {e}")
            # Try to resume anyway
            cursor.execute(f"ALTER WAREHOUSE {warehouse_upper} RESUME")
            time.sleep(2)
            self.was_suspended = True
        finally:
            cursor.close()

    def suspend_if_we_resumed(self):
        """Suspend warehouse only if we resumed it."""
        if self.was_suspended:
            cursor = self.conn.cursor()
            try:
                self.logger.log_msg(f"Suspending warehouse {self.warehouse_name}...")
                cursor.execute(f"ALTER WAREHOUSE {self.warehouse_name} SUSPEND")
                self.logger.log_msg(f"Warehouse {self.warehouse_name} suspended")
            finally:
                cursor.close()

    @contextmanager
    def auto_manage(self):
        """Context manager for automatic warehouse management."""
        try:
            self.resume_if_needed()
            yield
        finally:
            self.suspend_if_we_resumed()
