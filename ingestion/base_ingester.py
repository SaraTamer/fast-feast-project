from abc import ABC, abstractmethod

class Ingester(ABC):
    @abstractmethod
    def ingest(self):
        pass
