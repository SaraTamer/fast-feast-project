from abc import ABC, abstractmethod
import duckdb

class BaseTransformer(ABC):

    @abstractmethod
    def transform(self, relation: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
        pass
