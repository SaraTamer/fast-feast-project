import threading
import time
from watchers.stream_watcher import StreamWatcher
from watchers.batch_watcher import BatchWatcher
from config.config_loader import Config
from ingestion.ingester_factory import FactoryIngester
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

        finally:
            # Always remove from set — success or failure
            with self.lock:
                self.processing_files.discard(file_path)
                print(f"Released {file_path} — ready to accept again")

        
        # get the paths from config.yaml  using config_loader.py
        stream_path = self.app_config.stream_input_path()
        batch_path = self.app_config.batch_input_path()
        schema_path = self.app_config.schema_path()

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

        try:
            while True:
                time.sleep(1)   # keep main thread alive
                print("main_thread")
        except KeyboardInterrupt:
            print("\nCtrl+C caught — shutting down...")

            # Stop watchers properly
            self.stream_watch.stop()
            self.batch_watch.stop()

            # Wait for threads to exit
            t1.join()
            t2.join()


            print("Shutdown complete.")



if __name__ == "__main__":
    app = Main()
    app.run_pipeline()
