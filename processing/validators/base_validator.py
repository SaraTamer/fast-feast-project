from abc import ABC, abstractmethod
import duckdb
from config.schema_loader import SchemaLoader

class BaseValidator(ABC):
    @abstractmethod
    def validate(self, **kwargs) -> bool:
        pass
