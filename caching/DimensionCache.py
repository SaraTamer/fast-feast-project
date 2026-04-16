from config.schema_loader import SchemaLoader
from core.logger import AuditLogger


class DimensionCache:

    def __init__(self, duck_db_connection):
        self.duck = duck_db_connection.conn
        self.cached_dimensions = set()
        self.logger = AuditLogger()
        self.schema_loader = SchemaLoader('config/schema.yaml')

    def cache_dimension(self, table_name, relation):
        try:
            # Register the relation in DuckDB
            self.duck.register(table_name, relation)
            self.cached_dimensions.add(table_name)
            self.logger.log_msg(f"Dimension '{table_name}' cached successfully")

        except Exception as e:
            self.logger.log_err(f"Failed to cache dimension '{table_name}': {str(e)}")
            raise

    def get_cached_dimension(self, table_name):
        """Get a cached dimension by name."""
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

    def get_join_key(self, table_name: str):
        """Get the primary key (join key) for a table."""
        return self.schema_loader.get_primary_key(table_name)
    def clear_cache(self):
        """Clear all cached dimensions from memory and DuckDB."""
        self.logger.log_msg(f"Clearing dimension cache. Current cache: {self.cached_dimensions}")

        for dim_name in list(self.cached_dimensions):
            try:
                # Unregister from DuckDB
                self.duck.unregister(dim_name)
                self.logger.log_msg(f"Unregistered '{dim_name}' from DuckDB")
            except Exception as e:
                self.logger.log_warning(f"Failed to unregister '{dim_name}': {e}")

        # Clear the set
        self.cached_dimensions.clear()
        self.logger.log_msg("Dimension cache cleared successfully")
