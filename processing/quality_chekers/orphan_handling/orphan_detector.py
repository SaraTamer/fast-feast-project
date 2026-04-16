from time import sleep
from core.logger import AuditLogger
from config.required_cols_loader import RequiredColsLoader
from config.schema_loader import SchemaLoader
from config.config_loader import Config
from processing.error_batch_writer import ErrorBatchWriter
from .register_orphans import OrphansRegistrar


class OrphanChecker:

    def __init__(self, duckdb_conn):

        self.duckdb = duckdb_conn.conn
        self.logger = AuditLogger()
        self.config = Config()
        self.req_cols_schema = RequiredColsLoader(self.config.req_cols_path())
        self.schema = SchemaLoader(self.config.schemas_path())

        self.writer = ErrorBatchWriter()
        self.register = OrphansRegistrar(duckdb_conn)


    def detect_orphans(self, table_name, fact_df, dims_names, batch_id):

        self.duckdb.register("fact_table", fact_df)
        foreign_keys = self.req_cols_schema.get_foreign_keys(table_name)
        primary_key = self.schema.get_primary_key(table_name)

        all_orphans = []
        stream_dim = ['orders', 'tickets', 'ticket_events']

        # Start with all records
        clean_relation = fact_df
        self.duckdb.register("clean_table", clean_relation)

        for fk in foreign_keys:

            fk_column = fk["column"]
            ref = fk["references"]

            if '.' in ref:
                referenced_table = ref.split('.')[0]
                if referenced_table in stream_dim:
                    continue  # Skip this foreign key
            dim_name, dim_column = ref.split(".")

            if dim_name not in dims_names:
                self.logger.log_err(f"Dimension {dim_name} not found in cache")
                continue

            query = f"""
                SELECT f.*
                FROM clean_table f
                LEFT JOIN {dim_name} d
                ON f.{fk_column} = d.{dim_column}
                WHERE d.{dim_column} IS NULL
            """
            rows = self.duckdb.execute(query).fetchall()
            if not rows:
                continue

            self.logger.log_warning(f"{len(rows)} orphans detected in {table_name} referencing {dim_name}")
            
            # Get column names to find fk_column index
            result = self.duckdb.execute(f"SELECT * FROM fact_table LIMIT 1")
            column_names = [desc[0] for desc in result.description]
            fk_col_index = column_names.index(fk_column) if fk_column in column_names else 0
            
            for r in rows:
                fk_value = r[fk_col_index]
                all_orphans.append((r, fk_column, fk_value, dim_name))

            self.writer.write_batch(
                table_name=table_name,
                batch_id=batch_id,
                rows=rows,
                error_type="FK_MISSING",
                error_column=fk_column,
                fk_table=dim_name,
                is_retryable=True
            )

            # Track orphans for reconciliation
            for r in rows:
                all_orphans.append((r, fk_column, dim_name))

            # Remove orphans from clean_relation
            # Create list of IDs to exclude (assuming first column is primary key)
            orphan_ids = [str(r[0]) for r in rows if r and len(r) > 0]

            if orphan_ids:
                # Convert list to comma-separated string for SQL
                ids_str = ', '.join([f"'{id}'" for id in orphan_ids])

                # Create new clean table without orphans
                clean_query = f"""
                    CREATE OR REPLACE TEMP TABLE result_table AS
                    SELECT * FROM clean_table
                    WHERE CAST({primary_key} AS VARCHAR) NOT IN ({ids_str})
                """
                self.duckdb.execute(clean_query)

                # Update the registered relation
                clean_relation = self.duckdb.table("result_table")

                self.logger.log_msg(
                    f"Removed {len(orphan_ids)} orphan records, {clean_relation.count()} records remain")

        # Register orphans for retry
        if all_orphans:
            self.logger.log_msg(f"Orphan batch sent to {self.config.get_errors_table_name}")
            self.register.register_batch(table_name, primary_key, all_orphans)
        else:
            self.logger.log_msg(f"No orphans detected in {table_name}")

        # Return the clean relation (without orphans)
        return clean_relation