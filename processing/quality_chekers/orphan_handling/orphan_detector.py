from time import sleep

import duckdb
from core.logger import AuditLogger
from db.connections import DuckDBConnection, SnowflakeConnection
from config.schema_loader import SchemaLoader
from config.config_loader import Config
from processing.error_batch_writer import ErrorBatchWriter
from .register_orphans import OrphansRegistrar

class OrphanChecker:

    def __init__(self):
        self.duckdb = DuckDBConnection()
        self.logger = AuditLogger()
        self.config = Config()
        self.schema = SchemaLoader(self.config.schemas_path())
        self.writer= ErrorBatchWriter()
        self.register= OrphansRegistrar()

    def detect_orphans(self, table_name, fact_df, dim_tables,batch_id):
        self.duckdb.register("fact_table", fact_df)

        foreign_keys = self.schema.get_foreign_keys(table_name)
        primary_key=self.schema.get_primary_key(table_name)
        for fk in foreign_keys:
            fk_column=fk["column"]
            ref=fk["references"]
            dim_name, dim_column=ref.split(".")
            dim_df=dim_tables.get(dim_name)

            if not dim_df:
                self.logger.log_error(f"Dimension {dim_name} are not provided")
                continue
            orphans = []
            self.duckdb.register(dim_name, dim_df)
            query = f"""
                    SELECT * FROM fact_table
                    LEFT JOIN {dim_name} ON {fk_column}={dim_column}
                    WHERE {dim_column} is null
                  """
            rows=self.duckdb.execute(query).fetchall()
            if rows: 
                self.logger.log_warning(f"{len(rows)} orphans detected in {table_name} from {dim_name}")
                for r in rows: 
                    orphans.append({ "row": r, "fk_column": fk_column, "fk_table": dim_name})
            
        self.logger.log_msg(f"Waiting for {self.config.orphans_wait_time} before insert orphans in {self.config.get_errors_table_name}")
        sleep(self.config.orphans_wait_time)
        self.writer.write_batch(table_name,batch_id,'Orphans',fk_column,dim_name,True)
        self.logger.log_msg(f"Orphan_detector sent a batch to {self.config.get_errors_table_name} to be written in snowflake")
        self.register.register_orphans(table_name, primary_key, orphans)