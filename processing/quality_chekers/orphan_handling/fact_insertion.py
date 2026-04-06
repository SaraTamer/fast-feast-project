import json
from db.connections import DatabaseManager
from core.logger import Logger


class FactReplayService:

    def __init__(self):

        self.db = DatabaseManager()
        self.logger = Logger()
        self.snow = self.db.get_snowflake()


    def insert_fact(self, table_name, payload):

        cursor = self.snow.cursor()
        data = json.loads(payload)
        columns = list(data.keys())
        values = list(data.values())

        columns_sql = ",".join(columns)
        placeholders = ",".join(["%s"] * len(values))

        query = f"""
        INSERT INTO {table_name} ({columns_sql})
        VALUES ({placeholders})
        """

        cursor.execute(query, values)

        self.logger.log_msg(
            f"Inserted replayed record into {table_name}"
        )