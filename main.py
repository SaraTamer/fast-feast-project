import time
import threading
import uuid
from watchers.stream_watcher import StreamWatcher
from watchers.batch_watcher import BatchWatcher
from config.config_loader import Config
from ingestion.ingester_factory import FactoryIngester
from db.metadata_db import MetadataTracker
from config.schema_loader import SchemaLoader
from processing.schema_validator import SchemaValidator
from processing.quality_chekers.orphan_handling.orphan_detector import OrphanChecker
from processing.error_batch_writer import ErrorBatchWriter
from core.logger import AuditLogger

class PipelinePhases:
    def __init__(self, metadata_tracker, validator):
        self.metadata_tracker = metadata_tracker
        self.validator = validator
        self.audit_logger = AuditLogger()
        self.orphan_checker = OrphanChecker()
        self.error_writer = ErrorBatchWriter()

    def pipeline_trigger(self, file_path):

        if self.metadata_tracker.is_file_processed(file_path):
            self.audit_logger.log_msg(f"Skipping {file_path} (already processed)")
            return

        ingester = FactoryIngester(file_path).get_reader()

        try:
            if ingester:
                relation = ingester.ingest()
                if relation is not None:
                    valid_relation, table_name = self.validator.validate_schema(file_path, relation)
                    if valid_relation:
                        self.audit_logger.log_msg("Data validated, sample:")
                        self.audit_logger.log_msg(valid_relation.limit(5))

                    batch_id = str(uuid.uuid4())
                    dim_tables = self.validator.load_dimensions()
            
                    self.orphan_checker.detect_orphans(
                        table_name,
                        valid_relation,
                        dim_tables,
                        batch_id
                    )

                    self.metadata_tracker.log_file_processed(file_path)

                else:

                    self.audit_logger.log_err(
                        f"{file_path} failed Schema Validation. Dropping file."
                    )

        except Exception as e:
            self.audit_logger.log_err(
            f"An error occurred while processing the file: {e}"
        )

class Pipeline:
    def __init__(self, app_config, phases):
        self.app_config = app_config
        self.phases = phases
        self.audit_logger = AuditLogger()
        
        # get paths from config
        self.stream_path = self.app_config.stream_input_path()
        self.batch_path = self.app_config.batch_input_path()

        # create watchers
        self.stream_watch = StreamWatcher(self.stream_path, self.phases.pipeline_trigger)
        self.batch_watch = BatchWatcher(self.batch_path, self.phases.pipeline_trigger)
        
        self.t1 = None
        self.t2 = None

    def start(self):
        self.audit_logger.log_msg("starting both Watchers with Multi-threading\n")
        
        # create two parallel threads
        self.t1 = threading.Thread(target=self.stream_watch.watch_dog)
        self.t2 = threading.Thread(target=self.batch_watch.watch_dog)

        # start them at the same time
        self.t1.start()
        self.t2.start()

    def stop(self):
        self.audit_logger.log_msg("\nCtrl+C caught — shutting down...")
        
        # Stop watchers properly
        self.stream_watch.stop()
        self.batch_watch.stop()

        # Wait for threads to exit
        if self.t1: self.t1.join()
        if self.t2: self.t2.join()
        
        self.audit_logger.log_msg("Shutdown complete.")


class MainApp:
    def __init__(self):
        # Initialize dependencies
        self.app_config = Config()
        self.schema_loader = SchemaLoader(self.app_config.schemas_path())
        self.metadata_tracker = MetadataTracker()
        self.validator = SchemaValidator(self.schema_loader)

        # Initialize core components
        self.phases = PipelinePhases(self.metadata_tracker, self.validator)
        self.pipeline = Pipeline(self.app_config, self.phases)

    def run(self):
        self.pipeline.start()
        try:
            while True:
                time.sleep(1) # Keep main thread alive
        except KeyboardInterrupt:
            self.pipeline.stop()


if __name__ == "__main__":
    app = MainApp()
    app.run()
