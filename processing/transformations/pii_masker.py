import duckdb
from .base import BaseTransformer

class PIIMasker(BaseTransformer):

    def __init__(self):
        # Dictionary of tables and the columns that need masking
        self.masking_rules = {
            "customers": ["email", "phone"],
            "drivers": ["driver_phone", "national_id"],
            "agents": ["agent_email", "agent_phone"]
        }

    def transform(self, table_name: str, relation: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
        if table_name not in self.masking_rules:
            return relation
            
        columns = self.masking_rules[table_name]
        # implementing SQL hashing for the listed columns (using duckdb)
        return relation
