from time import sleep
from core.logger import AuditLogger
from db.connections import DuckDBConnection
from config.required_cols_loader import RequiredColsLoader
from config.schema_loader import SchemaLoader
from config.config_loader import Config
from processing.error_batch_writer import ErrorBatchWriter
from .register_orphans import OrphansRegistrar


class OrphanChecker:

    def __init__(self):

        self.duckdb = DuckDBConnection().conn
        self.logger = AuditLogger()
        self.config = Config()
        self.req_cols_schema = RequiredColsLoader(self.config.req_cols_path())
        self.schema = SchemaLoader(self.config.schemas_path())

        self.writer = ErrorBatchWriter()
        self.register = OrphansRegistrar()


    def detect_orphans(self, table_name, fact_df, dims_names, batch_id):

        self.duckdb.register("fact_table", fact_df)
        foreign_keys = self.req_cols_schema.get_foreign_keys(table_name)
        primary_key = self.schema.get_primary_key(table_name)

        all_orphans = []

        for fk in foreign_keys:

            fk_column = fk["column"]
            ref = fk["references"]

            dim_name, dim_column = ref.split(".")

            if dim_name not in dims_names:
                self.logger.log_err(f"Dimension {dim_name} not found in cache")
                continue

            query = f"""
                SELECT f.*
                FROM fact_table f
                LEFT JOIN {dim_name} d
                ON f.{fk_column} = d.{dim_column}
                WHERE d.{dim_column} IS NULL
            """
            rows = self.duckdb.execute(query).fetchall()
            if not rows:
                continue
            
            self.logger.log_warning(f"{len(rows)} orphans detected in {table_name} referencing {dim_name}")

            self.writer.write_batch(
                table_name=table_name,
                batch_id=batch_id,
                rows=rows,
                error_type="FK_MISSING",
                error_column=fk_column,
                fk_table=dim_name,
                is_retryable=True
            )


            
            for r in rows:
                all_orphans.append((r, fk_column, dim_name))


        if not all_orphans:
            self.logger.log_msg(f"No orphans detected in {table_name}")
            return


        self.logger.log_msg(
            f"Orphan batch sent to {self.config.get_errors_table_name}"
        )

        self.register.register_orphans(
            table_name,
            primary_key,
            all_orphans
        )