import duckdb
from .base import BaseTransformer

class MetricsEngine(BaseTransformer):

    def __init__(self):
        self.sla_first_min = 1
        self.sla_resolve_min = 15

    def transform(self, table_name: str, relation: duckdb.DuckDBPyRelation, **kwargs) -> duckdb.DuckDBPyRelation:
        # Add universal Date IDs for the Gold layer
        relation = self._add_date_ids(table_name, relation)

        # Add business calculations
        if table_name == 'orders':
            return self._transform_orders(relation)
        elif table_name == 'tickets':
            return self._transform_tickets(relation)
        
        return relation

    def _transform_tickets(self, relation):
        # Calculate SLA latency, breach flags, and revenue impact.
        # Check if we have SLA limit columns from the Enricher, else fallback
        return relation.project("""
            *,
            -- Latency in minutes
            (epoch(first_response_at) - epoch(created_at)) / 60 as first_response_min,
            (epoch(resolved_at) - epoch(created_at)) / 60 as resolution_min,
            
            -- SLA breach flags (using dynamic limits if they were joined)
            CASE 
                WHEN (epoch(first_response_at) - epoch(created_at)) / 60 > coalesce(sla_first_response_min, 1) THEN TRUE 
                ELSE FALSE 
            END as is_sla_first_breached,
            
            CASE 
                WHEN (epoch(resolved_at) - epoch(created_at)) / 60 > coalesce(sla_resolution_min, 15) THEN TRUE 
                ELSE FALSE 
            END as is_sla_resolution_breached,
            
            -- Reopen Rate flag
            CASE WHEN status = 'Reopened' THEN TRUE ELSE FALSE END as is_reopened,
            
            -- Revenue Impact
            coalesce(refund_amount, 0) as revenue_impact
        """)

    def _transform_orders(self, relation):
        # Calculate net revenue and delivery durations.
        return relation.project("""
            *,
            -- Net Revenue calculation
            (order_amount + delivery_fee - discount_amount) as net_revenue,
            
            -- Delivery duration (if delivered_at is not null)
            CASE 
                WHEN delivered_at IS NOT NULL THEN (epoch(delivered_at) - epoch(order_created_at)) / 60 
                ELSE NULL 
            END as delivery_duration_min
        """)

    def _add_date_ids(self, table_name, relation):
        # Convert timestamps to YYYYMMDD integer surrogate keys for DimDate.
        cols = relation.columns
        transform_sql = "SELECT *"
        
        # Mapping timestamps to target Date IDs
        date_mapping = {
            "order_created_at": "order_date_id",
            "delivered_at": "delivered_date_id",
            "created_at": "created_date_id",
            "first_response_at": "first_response_date_id",
            "resolved_at": "resolved_date_id"
        }
        
        for ts_col, id_col in date_mapping.items():
            if ts_col in cols:
                transform_sql += f", CAST(strftime({ts_col}, '%Y%m%d') AS INTEGER) AS {id_col}"
        
        return relation.query("temp_date", transform_sql)
