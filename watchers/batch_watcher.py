import time
from watchdog.observers import Observer
from .base_watcher import FileWatcher, BaseEventHandler

class BatchWatcher(FileWatcher):
    def __init__(self, directory: str, ingestion_callback):
        self.directory = directory
        self.ingestion_callback = ingestion_callback
        self.observer = Observer()
        self._running = True

    def watch_dog(self):
        # we pass the prefix "BATCH"
        event_handler = BaseEventHandler(self.ingestion_callback, prefix="BATCH")
        
        self.observer.schedule(event_handler, self.directory, recursive=True)
        self.observer.start()
        
        while self._running:
                time.sleep(1)
                print("batch_thread")
            
        self.observer.join()

    def stop(self):
        self._running = False
        self.observer.stop()

