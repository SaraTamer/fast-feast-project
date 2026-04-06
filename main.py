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

from db.metadata_db import MetadataTracker


class PipelineApp:

    def __init__(self):

        self.logger = AuditLogger()
        config = Config()
        schema_loader = SchemaLoader(config.schemas_path())
        validator = SchemaValidator(schema_loader)
        metadata = MetadataTracker()
        self.dim_cache = {}
        self.batch_pipeline = BatchPipeline(
            metadata,
            validator,
            self.dim_cache
        )

        self.stream_pipeline = StreamPipeline(
            validator,
            self.dim_cache
        )

        self.batch_watcher = BatchWatcher(
            config.batch_input_path(),
            self.batch_pipeline.process_file
        )

        self.stream_watcher = StreamWatcher(
            config.stream_input_path(),
            self.stream_pipeline.process_event
        )

    def start(self):

        self.logger.log_msg("Starting Batch + Stream pipelines")

        t1 = threading.Thread(target=self.batch_watcher.watch_dog)
        t2 = threading.Thread(target=self.stream_watcher.watch_dog)

        t1.start()
        t2.start()

        while True:
            time.sleep(1)


if __name__ == "__main__":
    app = PipelineApp()
    app.start()