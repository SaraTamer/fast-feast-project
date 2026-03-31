import time
import os
from abc import ABC, abstractmethod
from watchdog.events import FileSystemEventHandler

class BaseEventHandler(FileSystemEventHandler):

    def __init__(self, callback_function, prefix="WATCHER"):
        self.callback_function = callback_function
        self.prefix = prefix
    # detecting when a new file is created in the directory
    def on_created(self, event):
        if event.is_directory:
            return
            
        file_path = event.src_path
        if not (file_path.endswith('.csv') or file_path.endswith('.json')):
            return

        # a simple logic for checking if the file is fully written or not
        historical_size = -1
        while True:
            try:
                current_size = os.path.getsize(file_path)
            except OSError:
                time.sleep(0.5)
                continue
                
            if current_size == historical_size and current_size > 0:
                break
                
            historical_size = current_size
            time.sleep(0.5)

        print(f"detected new [{self.prefix}] file: {file_path} and ready to be sent to ingestion")
        self.callback_function(file_path)


class FileWatcher(ABC):
    @abstractmethod
    def watch_dog(self):
        pass
