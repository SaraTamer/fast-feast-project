import duckdb
import yaml
import os
class MetadataDB:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(MetadataDB, cls).__new__(cls)
            cls._instance.conn = duckdb.connect(':memory:')
            cls._instance.conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_files (
                    filename VARCHAR PRIMARY KEY,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            if args:
                with open(args[0], 'r') as file:
                    cls._instance.schemas = yaml.safe_load(file)['tables']
            else:
                cls._instance.schemas = {}

        return cls._instance

    def __init__(self):
        pass
 
    def get_required_cols(self, table_name: str):
        return self.schemas.get(table_name, {}).get('required_columns', [])

    def get_cols_name(self, table_name: str):
        return self.schemas.get(table_name, {}).get('columns', [])

    def get_dataTypes(self, table_name: str):
        return self.schemas.get(table_name, {}).get('data_types', [])
    
    def get_primary_key(self, table_name: str):
        return self.schemas.get(table_name, {}).get('primary_key', None)

    def get_file_names(self, ):
        return list(self.schemas.keys())
    def is_file_processed(self, filename: str):
        result = self.conn.execute(
            "SELECT 1 FROM processed_files WHERE filename = ?", (filename,)
        ).fetchone()
        return result is not None
    def log_file_processed(self, filename: str):
        self.conn.execute(
            "INSERT OR IGNORE INTO processed_files (filename) VALUES (?)", (filename,)
        )

    def execute_query(self, query: str, params=None):
        if params:
            return self.conn.execute(query, params)
        return self.conn.execute(query)