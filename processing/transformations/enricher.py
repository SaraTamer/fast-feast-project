import duckdb
from .base import BaseTransformer
from core.logger import AuditLogger


class Enricher(BaseTransformer):
    """Handles dimension joins."""

    def __init__(self, duckdb_conn):
        self.conn = duckdb_conn
        self.logger = AuditLogger()
        self.counter = 0

    def transform(self, table_name: str, relation: duckdb.DuckDBPyRelation, **kwargs) -> duckdb.DuckDBPyRelation:
        """Implement SQL joins for each table."""
        if relation is None:
            self.logger.log_warning(f"Enricher: relation is None for {table_name}")
            return None

        self.logger.log_msg(f"Enriching table: {table_name}")

        # Register the relation temporarily
        self.counter += 1
        temp_name = f"_temp_enrich_{table_name}_{self.counter}"

        self.logger.log_msg(f"Registering relation as {temp_name}")
        self.conn.register(temp_name, relation)

        try:
            if table_name == 'customers':
                result = self._enrich_customers(temp_name)
            elif table_name == 'regions':
                result = self._enrich_regions(temp_name)
            elif table_name == 'agents':
                result = self._enrich_agents(temp_name)
            elif table_name == 'reasons':
                result = self._enrich_reasons(temp_name)
            elif table_name == 'tickets':
                result = self._enrich_tickets(temp_name)
            else:
                self.logger.log_msg(f"No enrichment defined for {table_name}, returning original")
                df = self.conn.table(temp_name).df()
                result = self.conn.from_df(df)

            self.logger.log_msg(f"Successfully enriched {table_name}")
            return result

        except Exception as e:
            self.logger.log_err(f"Failed to enrich {table_name}: {e}")
            try:
                df = self.conn.table(temp_name).df()
                return self.conn.from_df(df)
            except:
                return relation
        finally:
            try:
                self.conn.unregister(temp_name)
                self.logger.log_msg(f"Unregistered {temp_name}")
            except Exception as e:
                self.logger.log_warning(f"Failed to unregister {temp_name}: {e}")

    def _check_table_exists(self, table_name):
        """Check if a table is registered in DuckDB (silent fail)."""
        try:
            self.conn.table(table_name)
            return True
        except Exception:
            # Silently return False without logging warning
            return False

    def _check_table_exists_with_log(self, table_name):
        """Check if a table exists with debug logging (only in debug mode)."""
        try:
            self.conn.table(table_name)
            self.logger.log_msg(f"Table {table_name} exists")
            return True
        except Exception:
            # Log at debug level instead of warning
            self.logger.log_msg(f"Table {table_name} not found, skipping enrichment")
            return False

    def _enrich_tickets(self, temp_name):
        """Join tickets with priorities and channels."""
        self.logger.log_msg(f"Enriching tickets using {temp_name}")

        has_priorities = self._check_table_exists('priorities')
        has_channels = self._check_table_exists('channels')

        if not has_priorities and not has_channels:
            self.logger.log_msg("No enrichment tables available for tickets")
            df = self.conn.table(temp_name).df()
            return self.conn.from_df(df)

        select_parts = [f"{temp_name}.*"]
        if has_priorities:
            select_parts.append("p.sla_first_response_min")
            select_parts.append("p.sla_resolution_min")
        if has_channels:
            select_parts.append("c.channel_name")

        from_clause = f"FROM {temp_name}"
        if has_priorities:
            from_clause += " LEFT JOIN priorities AS p ON {}.priority_id = p.priority_id".format(temp_name)
        if has_channels:
            from_clause += " LEFT JOIN channels AS c ON {}.channel_id = c.channel_id".format(temp_name)

        full_sql = f"SELECT {', '.join(select_parts)} {from_clause}"
        self.logger.log_msg(f"Executing SQL: {full_sql[:200]}...")

        df = self.conn.execute(full_sql).fetchdf()
        self.logger.log_msg(f"Enriched tickets, got {len(df)} rows")

        return self.conn.from_df(df)

    def _enrich_customers(self, temp_name):
        """Enrich customers with segments."""
        self.logger.log_msg(f"Enriching customers using {temp_name}")

        if not self._check_table_exists('segments'):
            self.logger.log_msg("Segments table not found, returning original")
            df = self.conn.table(temp_name).df()
            return self.conn.from_df(df)

        sql = f"""
            SELECT 
                c.*, 
                s.segment_name, 
                s.discount_pct 
            FROM {temp_name} AS c
            LEFT JOIN segments AS s ON c.segment_id = s.segment_id
        """
        self.logger.log_msg(f"Executing SQL: {sql[:200]}...")

        df = self.conn.execute(sql).fetchdf()
        self.logger.log_msg(f"Enriched customers, got {len(df)} rows")

        return self.conn.from_df(df)

    def _enrich_regions(self, temp_name):
        """Enrich regions with cities."""
        self.logger.log_msg(f"Enriching regions using {temp_name}")

        if not self._check_table_exists('cities'):
            self.logger.log_msg("Cities table not found, returning original")
            df = self.conn.table(temp_name).df()
            return self.conn.from_df(df)

        sql = f"""
            SELECT 
                r.*, 
                c.city_name, 
                c.country, 
                c.timezone
            FROM {temp_name} AS r
            LEFT JOIN cities AS c ON r.city_id = c.city_id
        """
        self.logger.log_msg(f"Executing SQL: {sql[:200]}...")

        df = self.conn.execute(sql).fetchdf()
        self.logger.log_msg(f"Enriched regions, got {len(df)} rows")

        return self.conn.from_df(df)

    def _enrich_agents(self, temp_name):
        """Enrich agents with teams."""
        self.logger.log_msg(f"Enriching agents using {temp_name}")

        if not self._check_table_exists('teams'):
            # Log at info level instead of warning
            self.logger.log_msg("Teams table not found, returning original")
            df = self.conn.table(temp_name).df()
            return self.conn.from_df(df)

        sql = f"""
            SELECT 
                a.*, 
                t.team_name
            FROM {temp_name} AS a
            LEFT JOIN teams AS t ON a.team_id = t.team_id
        """
        self.logger.log_msg(f"Executing SQL: {sql[:200]}...")

        try:
            df = self.conn.execute(sql).fetchdf()
            self.logger.log_msg(f"Enriched agents, got {len(df)} rows")
            return self.conn.from_df(df)
        except Exception as e:
            self.logger.log_err(f"Failed to enrich agents: {e}")
            df = self.conn.table(temp_name).df()
            return self.conn.from_df(df)

    def _enrich_reasons(self, temp_name):
        """Enrich reasons with reason categories."""
        self.logger.log_msg(f"Enriching reasons using {temp_name}")

        if not self._check_table_exists('reason_categories'):
            # Log at info level instead of warning
            self.logger.log_msg("Reason_categories table not found, returning original")
            df = self.conn.table(temp_name).df()
            return self.conn.from_df(df)

        sql = f"""
            SELECT 
                r.*, 
                rc.reason_category_name
            FROM {temp_name} AS r
            LEFT JOIN reason_categories AS rc ON r.reason_category_id = rc.reason_category_id
        """
        self.logger.log_msg(f"Executing SQL: {sql[:200]}...")

        df = self.conn.execute(sql).fetchdf()
        self.logger.log_msg(f"Enriched reasons, got {len(df)} rows")

        return self.conn.from_df(df)