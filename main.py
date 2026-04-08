import threading
import time

from pipelines.batch_pipeline import BatchPipeline
from pipelines.stream_pipeline import StreamPipeline

from watchers.batch_watcher import BatchWatcher
from watchers.stream_watcher import StreamWatcher

from core.logger import AuditLogger

from config.config_loader import Config
from config.schema_loader import SchemaLoader
from processing.schema_validator import SchemaValidator
from processing.formats import FormatChecker      
from db.connections import DuckDBConnection
from db.metadata_db import MetadataTracker
from caching.DimensionCache import DimensionCache
from db.dwh_loader import DWHLoader

class PipelineApp:

    def __init__(self):

        self.logger = AuditLogger()
        self.duck_db_connection = DuckDBConnection()
        config = Config()
        schema_loader = SchemaLoader(config.schemas_path())
        validator = SchemaValidator(schema_loader)
        format_checker  = FormatChecker()
        metadata = MetadataTracker(self.duck_db_connection)
        self.dim_cache = DimensionCache(self.duck_db_connection)
        self.dwh_loader = DWHLoader()
        self.batch_pipeline = BatchPipeline(
            metadata,
            validator,
            format_checker,
            self.dim_cache,
            self.dwh_loader,
            self.duck_db_connection
        )

        self.stream_pipeline = StreamPipeline(
            metadata,
            validator,
            format_checker,
            self.dim_cache,
            self.dwh_loader,
            self.duck_db_connection
        )

        self.batch_watcher = BatchWatcher(
            config.batch_input_path(),
            self.batch_pipeline.process_file
        )

        self.stream_watcher = StreamWatcher(
            config.stream_input_path(),
            self.stream_pipeline.process_event
        )

        self.t1 = None
        self.t2 = None

    def start(self):

        self.logger.log_msg("Starting Batch + Stream pipelines")

        self.t1 = threading.Thread(target=self.batch_watcher.watch_dog)
        self.t2 = threading.Thread(target=self.stream_watcher.watch_dog)

        self.t1.start()
        self.t2.start()


    def stop(self):
        print("\nCtrl+C caught — shutting down...")

        # Stop watchers properly
        self.stream_watcher.stop()
        self.batch_watcher.stop()

        # Wait for threads to exit
        if self.t1: self.t1.join()
        if self.t2: self.t2.join()

        print("Shutdown complete.")

class MainApp:
    def __init__(self):

        # Initialize core components
        self.pipeline = PipelineApp()

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