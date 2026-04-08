import duckdb
import pandas as pd
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

        # Convert to DataFrame
        df = relation.df()

        if len(df) == 0:
            self.logger.log_warning(f"Empty relation for {table_name}")
            return relation

        if table_name == 'orders':
            df = self._transform_orders(df)
        elif table_name == 'tickets':
            df = self._transform_tickets(df)
        else:
            return relation

        # Convert back to DuckDB relation
        return self.duckdb.from_df(df)

    def _transform_tickets(self, df):
        """Transform tickets using pandas."""
        # Convert to datetime
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['first_response_at'] = pd.to_datetime(df['first_response_at'])
        df['resolved_at'] = pd.to_datetime(df['resolved_at'])

        # Calculate latency in minutes
        df['first_response_min'] = (df['first_response_at'] - df['created_at']).dt.total_seconds() / 60
        df['resolution_min'] = (df['resolved_at'] - df['created_at']).dt.total_seconds() / 60

        # Get SLA values (if they exist from join, otherwise use defaults)
        sla_first = df.get('sla_first_response_min', self.sla_first_min)
        sla_resolve = df.get('sla_resolution_min', self.sla_resolve_min)

        # Calculate SLA breach flags
        df['is_sla_first_breached'] = df['first_response_min'] > sla_first
        df['is_sla_resolution_breached'] = df['resolution_min'] > sla_resolve

        # Reopen flag
        df['is_reopened'] = df['status'] == 'Reopened'

        # Revenue impact
        df['revenue_impact'] = df['refund_amount'].fillna(0)

        # Date IDs
        df['created_date_id'] = df['created_at'].dt.strftime('%Y%m%d').astype(int)
        df['first_response_date_id'] = df['first_response_at'].dt.strftime('%Y%m%d').astype(int)
        df['resolved_date_id'] = df['resolved_at'].dt.strftime('%Y%m%d').astype(int)

        self.logger.log_msg(f"Transformed {len(df)} tickets")
        return df

    def _transform_orders(self, df):
        """Transform orders using pandas."""
        # Convert to datetime
        df['order_created_at'] = pd.to_datetime(df['order_created_at'])

        # Net revenue
        df['net_revenue'] = df['order_amount'] + df['delivery_fee'] - df['discount_amount']

        # Delivery duration
        if 'delivered_at' in df.columns:
            df['delivered_at'] = pd.to_datetime(df['delivered_at'])
            mask = df['delivered_at'].notna()
            df.loc[mask, 'delivery_duration_min'] = (df.loc[mask, 'delivered_at'] - df.loc[
                mask, 'order_created_at']).dt.total_seconds() / 60

        # Date IDs
        df['order_date_id'] = df['order_created_at'].dt.strftime('%Y%m%d').astype(int)
        if 'delivered_at' in df.columns:
            df['delivered_date_id'] = df['delivered_at'].dt.strftime('%Y%m%d').astype(int)

        self.logger.log_msg(f"Transformed {len(df)} orders")
        return df