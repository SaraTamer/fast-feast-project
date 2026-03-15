from abc import ABC, abstractmethod

class FileWatcher(ABC):
    @abstractmethod
    def watch_dog(self):
        pass
