import hashlib
import os


class MetadataTracker:
    def __init__(self, duckdb_conn, hash_algorithm='sha256'):
        self.conn = duckdb_conn.conn
        self.hash_algorithm = hash_algorithm

        # Updated schema to store file_hash instead of filename
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                file_hash VARCHAR PRIMARY KEY,
                filename VARCHAR,
                file_size INTEGER,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate hash of file content"""
        hash_func = hashlib.new(self.hash_algorithm)

        try:
            with open(file_path, 'rb') as f:
                # Read in chunks to handle large files efficiently
                for chunk in iter(lambda: f.read(65536), b''):
                    hash_func.update(chunk)
            return hash_func.hexdigest()
        except Exception as e:
            print(f"Error hashing file {file_path}: {e}")
            return None

    def is_file_processed(self, file_path: str) -> bool:
        """Check if file content has been processed before"""
        file_hash = self._calculate_file_hash(file_path)
        if not file_hash:
            return False

        result = self.conn.execute(
            "SELECT 1 FROM processed_files WHERE file_hash = ?", (file_hash,)
        ).fetchone()
        return result is not None

    def log_file_processed(self, file_path: str):
        """Mark file as processed by storing its hash"""
        file_hash = self._calculate_file_hash(file_path)
        if not file_hash:
            return

        # Get file size for metadata
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else None

        self.conn.execute(
            """INSERT OR IGNORE INTO processed_files (file_hash, filename, file_size) 
               VALUES (?, ?, ?)""",
            (file_hash, file_path, file_size)
        )

    def clear_all(self):
        """Clear all processed files from the tracking table."""
        result = self.conn.execute("SELECT COUNT(*) FROM processed_files").fetchone()
        count = result[0] if result else 0

        self.conn.execute("DELETE FROM processed_files")
        self.conn.execute("VACUUM")  # Optional: reclaim disk space

        print(f"Cleared {count} records from processed_files tracking")

    def reset(self):
        """Alias for clear_all() - resets all tracking data."""
        self.clear_all()
