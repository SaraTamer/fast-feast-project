import os
import threading
import duckdb
import snowflake.connector
from dotenv import load_dotenv

# Load .env secrets
load_dotenv()

class DuckDBConnection:
    # Singltone with adding thread lock
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DuckDBConnection, cls).__new__(cls)
                    
                    # Ensure the data folder exists before connecting
                    os.makedirs('data', exist_ok=True)
                    
                    cls._instance.conn = duckdb.connect(
                        database='data/pipeline_state.duckdb', 
                        read_only=False
                    )
                    
        return cls._instance


class SnowflakeConnection:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(SnowflakeConnection, cls).__new__(cls)
                    
                    cls._instance.conn = snowflake.connector.connect(
                        user=os.getenv("SNOWFLAKE_USER"),
                        password=os.getenv("SNOWFLAKE_PASSWORD"),
                        account=os.getenv("SNOWFLAKE_ACCOUNT"),
                        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                        database=os.getenv("SNOWFLAKE_DATABASE")
                    )
                    
        return cls._instance
