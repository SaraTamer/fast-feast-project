import time
from watchdog.observers import Observer
from .base_watcher import FileWatcher, BaseEventHandler

class BatchWatcher(FileWatcher):
    def __init__(self, directory: str, ingestion_callback):
        self.directory = directory
        self.ingestion_callback = ingestion_callback
        self.observer = Observer()

    def watch_dog(self):
        # we pass the prefix "BATCH"
        event_handler = BaseEventHandler(self.ingestion_callback, prefix="BATCH")
        
        self.observer.schedule(event_handler, self.directory, recursive=True)
        self.observer.start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
            
        self.observer.join()
