from abc import ABC, abstractmethod
import duckdb
from config.schema_loader import SchemaLoader

class BaseValidator(ABC):
    @abstractmethod
    def validate(self, relation: duckdb.DuckDBPyRelation, table_name: str) -> bool:
        pass
