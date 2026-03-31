from abc import ABC, abstractmethod

class Ingester(ABC):
    @staticmethod
    def empty(rel):
        return rel.count("*").fetchone()[0] == 0
    @abstractmethod
    def ingest(self):
        pass
