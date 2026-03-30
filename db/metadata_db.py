from db.connections import DuckDBConnection

class MetadataTracker:
    def __init__(self):
        self.conn = DuckDBConnection().conn 

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                filename VARCHAR PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def is_file_processed(self, filename: str):
        result = self.conn.execute(
            "SELECT 1 FROM processed_files WHERE filename = ?", (filename,)
        ).fetchone()
        return result is not None

    def log_file_processed(self, filename: str):
        self.conn.execute(
            "INSERT OR IGNORE INTO processed_files (filename) VALUES (?)", (filename,)
        )