import os
import duckdb
import snowflake.connector
from dotenv import load_dotenv

# Load .env secrets
load_dotenv()

class DatabaseManager:
    _instance = None

    def __new__(cls):
        # Singleton check
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            
            # DuckDB Local Connection
            cls._instance.duck = duckdb.connect(database=':memory:', read_only=False)
            
            # Snowflake Connection
            cls._instance.snow = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE")
            )
            
        return cls._instance
