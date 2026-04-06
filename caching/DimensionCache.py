from db.connections import DuckDBConnection
from config.schema_loader import SchemaLoader
from core.logger import AuditLogger
class DimensionCache:

    def __init__(self):
        self.duck = DuckDBConnection()

    def cache_dimension(self, table_name, relation):
        self.duck.register(table_name, relation)