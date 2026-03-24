from .csv_ingest import CSVIngest
from .json_ingest import JSONIngest

class FactoryIngester:
    def __init__(self, file_type: str):
        pass

    def get_reader(self, file_path: str):
        print("We are Sara and Shahd, we got the file to be ready for ingestion!!")
