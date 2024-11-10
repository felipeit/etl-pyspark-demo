from abc import ABC, abstractmethod
from typing import Any
from dataenforce import Dataset

class Loader(ABC):
    
    @abstractmethod
    def save(self, df:Dataset[Any, ...]) -> None:...