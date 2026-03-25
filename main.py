import threading
from watchers.stream_watcher import StreamWatcher
from watchers.batch_watcher import BatchWatcher
from config.config_loader import Config
from ingestion.ingester_factory import FactoryIngester


def test_pipeline_trigger(file_path):

    file_type = file_path.split('.')[-1] 
    ingester = FactoryIngester(file_type).get_reader(file_path)
    try: 
        if ingester:
            df=ingester.ingest()
            if df is not None:
                print(df.head())
    except Exception as e:
        print(f"An error occurred while processing the file: {e}")
    
class Main:
    def __init__(self):
        self.app_config = Config()
        
        # get the paths from config.yaml  using config_loader.py
        stream_path = self.app_config.stream_input_path()
        batch_path = self.app_config.batch_input_path()
        
        # create the watchers
        self.stream_watch = StreamWatcher(stream_path, test_pipeline_trigger)
        self.batch_watch = BatchWatcher(batch_path, test_pipeline_trigger)

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
