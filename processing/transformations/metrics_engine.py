import duckdb
from .base import BaseTransformer
from core.logger import AuditLogger


class MetricsEngine(BaseTransformer):

    def __init__(self, duckdb):
        self.sla_first_min = 1
        self.sla_resolve_min = 15
        self.logger = AuditLogger()
        self.duckdb = duckdb

    def transform(self, table_name: str, relation: duckdb.DuckDBPyRelation, **kwargs) -> duckdb.DuckDBPyRelation:
        """Add metrics calculations to the relation."""
        if relation is None:
            return None

        self.logger.log_msg(f"Calculating metrics for {table_name}")

        # Convert to DataFrame first to avoid connection issues
        df = relation.df()

        # Register as temp table in the connection
        temp_table = f"_temp_metrics_{table_name}"
        duckdb.register(temp_table,df)

        try:
            if table_name == 'orders':
                result = self._transform_orders_sql(temp_table, duckdb)
            elif table_name == 'tickets':
                result = self._transform_tickets_sql(temp_table, duckdb)
            else:
                result = relation

            return result
        finally:
            try:
                duckdb.unregister(temp_table)
            except:
                pass

    def _transform_tickets_sql(self, temp_table, conn):
        """Transform tickets using SQL."""
        sql = f"""
            SELECT 
                *,
                -- Latency in minutes
                (epoch(first_response_at) - epoch(created_at)) / 60 as first_response_min,
                (epoch(resolved_at) - epoch(created_at)) / 60 as resolution_min,

                -- SLA breach flags
                CASE 
                    WHEN (epoch(first_response_at) - epoch(created_at)) / 60 > COALESCE(sla_first_response_min, 1) THEN TRUE 
                    ELSE FALSE 
                END as is_sla_first_breached,

                CASE 
                    WHEN (epoch(resolved_at) - epoch(created_at)) / 60 > COALESCE(sla_resolution_min, 15) THEN TRUE 
                    ELSE FALSE 
                END as is_sla_resolution_breached,

                -- Reopen Rate flag
                CASE WHEN status = 'Reopened' THEN TRUE ELSE FALSE END as is_reopened,

                -- Revenue Impact
                COALESCE(refund_amount, 0) as revenue_impact,

                -- Date IDs
                CAST(strftime(created_at, '%Y%m%d') AS INTEGER) as created_date_id,
                CAST(strftime(first_response_at, '%Y%m%d') AS INTEGER) as first_response_date_id,
                CAST(strftime(resolved_at, '%Y%m%d') AS INTEGER) as resolved_date_id
            FROM {temp_table}
        """

        result_df = conn.execute(sql).fetchdf()
        return conn.from_df(result_df)

    def _transform_orders_sql(self, temp_table, conn):
        """Transform orders using SQL."""
        sql = f"""
            SELECT 
                *,
                -- Net Revenue calculation
                (order_amount + delivery_fee - discount_amount) as net_revenue,

                -- Delivery duration
                CASE 
                    WHEN delivered_at IS NOT NULL THEN (epoch(delivered_at) - epoch(order_created_at)) / 60 
                    ELSE NULL 
                END as delivery_duration_min,

                -- Date IDs
                CAST(strftime(order_created_at, '%Y%m%d') AS INTEGER) as order_date_id,
                CAST(strftime(delivered_at, '%Y%m%d') AS INTEGER) as delivered_date_id
            FROM {temp_table}
        """

        result_df = conn.execute(sql).fetchdf()
        return conn.from_df(result_df)