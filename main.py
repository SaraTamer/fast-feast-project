import time
import threading
from watchers.stream_watcher import StreamWatcher
from watchers.batch_watcher import BatchWatcher
from config.config_loader import Config
from ingestion.ingester_factory import FactoryIngester
from db.metadata_db import MetadataTracker
from config.schema_loader import SchemaLoader
from processing.schema_validator import SchemaValidator

class PipelinePhases:
    def __init__(self, metadata_tracker, validator):
        self.metadata_tracker = metadata_tracker
        self.validator = validator
        self.processing_files = set()
        self.lock = threading.Lock()        # thread-safe since you use 2 threads; where we will lock on shared variables when we use them for each thread

    def pipeline_trigger(self, file_path):
        with self.lock:
            if file_path in self.processing_files:
                print(f"Skipping {file_path} (currently being processed)")
                return
            if self.metadata_tracker.is_file_processed(file_path):
                print(f"Skipping {file_path} (already processed)")
                return
            # Mark as in-progress
            self.processing_files.add(file_path)

        file_type = file_path.split('.')[-1]
        ingester = FactoryIngester(file_type).get_reader(file_path)
        try:
            if ingester:
                relation = ingester.ingest()
                if relation is not None:
                    valid_relation, _ = self.validator.validate_schema(file_path, relation)
                    if valid_relation:
                        print(f"\nData validated, this is a sample:")
                        print(valid_relation.limit(5))
                        self.metadata_tracker.log_file_processed(file_path)
                    else:
                        print(f"{file_path} failed Schema Validation. Dropping file.")
        except Exception as e:
            print(f"An error occurred while processing the file: {e}")
        finally:
            # Always remove from set — success or failure
            with self.lock:
                self.processing_files.discard(file_path)
                print(f"Released {file_path} — ready to accept again")

class Pipeline:
    def __init__(self, app_config, phases):
        self.app_config = app_config
        self.phases = phases
        
        # get paths from config
        self.stream_path = self.app_config.stream_input_path()
        self.batch_path = self.app_config.batch_input_path()

        # create watchers
        self.stream_watch = StreamWatcher(self.stream_path, self.phases.pipeline_trigger)
        self.batch_watch = BatchWatcher(self.batch_path, self.phases.pipeline_trigger)
        
        self.t1 = None
        self.t2 = None

    def start(self):
        print("starting both Watchers with Multi-threading\n")
        
        # create two parallel threads
        self.t1 = threading.Thread(target=self.stream_watch.watch_dog)
        self.t2 = threading.Thread(target=self.batch_watch.watch_dog)

        # start them at the same time
        self.t1.start()
        self.t2.start()

    def stop(self):
        print("\nCtrl+C caught — shutting down...")
        
        # Stop watchers properly
        self.stream_watch.stop()
        self.batch_watch.stop()

        # Wait for threads to exit
        if self.t1: self.t1.join()
        if self.t2: self.t2.join()
        
        print("Shutdown complete.")


class MainApp:
    def __init__(self):
        # Initialize dependencies
        self.app_config = Config()
        self.schema_loader = SchemaLoader(self.app_config.schema_path())
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