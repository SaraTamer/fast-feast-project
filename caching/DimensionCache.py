from db.connections import DuckDBConnection
from config.schema_loader import SchemaLoader
from core.logger import AuditLogger

class DimensionCache:

    def __init__(self):
        self.duck = DuckDBConnection().conn
        self.cached_dimensions = set()
        self.logger = AuditLogger()

    def cache_dimension(self, table_name, relation):
        try:
            # Register the relation in DuckDB
            self.duck.register(table_name, relation)
            self.cached_dimensions.add(table_name)

        except Exception as e:
            self.logger.log_err(f"Failed to cache dimension '{table_name}': {str(e)}")
            raise

    def get_cached_dimension(self, table_name, relation):
        if table_name not in self.cached_dimensions:
            self.logger.log_warning(f"Dimension '{table_name}' not found in cache")
            return None

        try:
            # Query the cached table and return as relation
            relation = self.duck.table(table_name)
            return relation
        except Exception as e:
            self.logger.log_err(f"Failed to retrieve dimension '{table_name}': {str(e)}")
            return None

    def get_all_cached_dimensions(self) -> list:
        """Get list of all cached dimension names."""
        return list(self.cached_dimensions)