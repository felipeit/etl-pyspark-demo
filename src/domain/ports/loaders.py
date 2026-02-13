from abc import ABC, abstractmethod
from typing import Any

class Loader(ABC):
    
    @abstractmethod
    def save(self, df: Any = None) -> None:...