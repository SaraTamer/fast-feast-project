import os
import threading
from watchers.stream_watcher import StreamWatcher
from watchers.batch_watcher import BatchWatcher
from config.config_loader import Config
from ingestion.ingester_factory import FactoryIngester
from db.metadata_db import MetadataTracker
from config.schema_loader import SchemaLoader
from processing.schema_validator import SchemaValidator

app_config = Config()
schema_loader = SchemaLoader(app_config.schema_path())
metadata_tracker = MetadataTracker()
validator = SchemaValidator(schema_loader)

def pipeline_trigger(file_path):

    if metadata_tracker.is_file_processed(file_path):
        print(f"Skipping {file_path} (already processed)")
        return

    file_type = file_path.split('.')[-1] 
    ingester = FactoryIngester(file_type).get_reader(file_path)
    try: 
        if ingester:
            relation = ingester.ingest()
            if relation is not None:
                valid_relation, _ = validator.validate_schema(file_path, relation)
                if valid_relation:
                    print(f"\nData validated, this is a sample:")
                    print(valid_relation.limit(5))
                    metadata_tracker.log_file_processed(file_path)
                else:
                    print(f"{file_path} failed Schema Validation. Dropping file.")
    except Exception as e:
        print(f"An error occurred while processing the file: {e}")


class Main:
    def __init__(self):
        
        # get the paths from config.yaml  using config_loader.py
        stream_path = app_config.stream_input_path()
        batch_path = app_config.batch_input_path()

        # create the watchers
        self.stream_watch = StreamWatcher(stream_path, pipeline_trigger)
        self.batch_watch = BatchWatcher(batch_path, pipeline_trigger)

    def run_pipeline(self):
        print("starting both Watchers with Multi-threading\n")
        
        # create two parallel threads
        t1 = threading.Thread(target=self.stream_watch.watch_dog)
        t2 = threading.Thread(target=self.batch_watch.watch_dog)

        # start them at the same time
        t1.start()
        t2.start()


if __name__ == "__main__":
    app = Main()
    app.run_pipeline()
