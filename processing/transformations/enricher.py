import duckdb
from .base import BaseTransformer

class Enricher(BaseTransformer):
    # Handking joins

    def __init__(self, duckdb_conn):
        self.conn = duckdb_conn

    def transform(self, table_name: str, relation: duckdb.DuckDBPyRelation, **kwargs) -> duckdb.DuckDBPyRelation:
        # Implementing SQL joins for each table
        if table_name == 'customers':
            return self._enrich_customers(relation)
        elif table_name == 'regions':
            return self._enrich_regions(relation)
        elif table_name == 'agents':
            return self._enrich_agents(relation)
        elif table_name == 'reasons':
            return self._enrich_reasons(relation)
        
        return relation

    def _enrich_customers(self, relation):
        if not self._check_table_exists('segments'):
            return relation
        
        return self.conn.sql("""
            SELECT 
                c.*, 
                s.segment_name, 
                s.discount_pct 
            FROM relation AS c
            LEFT JOIN segments AS s ON c.segment_id = s.segment_id
        """)

    def _enrich_regions(self, relation):
        if not self._check_table_exists('cities'):
            return relation

        return self.conn.sql("""
            SELECT 
                r.*, 
                c.city_name, 
                c.country, 
                c.timezone
            FROM relation AS r
            LEFT JOIN cities AS c ON r.city_id = c.city_id
        """)

    def _enrich_agents(self, relation):
        if not self._check_table_exists('teams'):
            return relation

        return self.conn.sql("""
            SELECT 
                a.*, 
                t.team_name
            FROM relation AS a
            LEFT JOIN teams AS t ON a.team_id = t.team_id
        """)

    def _enrich_reasons(self, relation):
        if not self._check_table_exists('reason_categories'):
            return relation

        return self.conn.sql("""
            SELECT 
                r.*, 
                rc.reason_category_name
            FROM relation AS r
            LEFT JOIN reason_categories AS rc ON r.reason_category_id = rc.reason_category_id
        """)

    def _check_table_exists(self, table_name):
        # checker to check if the table is registered in DuckDB.
        try:
            self.conn.table(table_name)
            return True
        except:
            return False
