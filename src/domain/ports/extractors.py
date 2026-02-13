from abc import ABC, abstractmethod
from typing import Any, Optional

from src.infra.data.ports.engine import Engine


class Extract(ABC):
    def __init__(self, engine: Optional[Engine] = None) -> None:
        """Engine may be provided by adapter; default is None for lazy/DI creation."""
        self.engine = engine
    
    @abstractmethod
    def read(self, file_path: str, header: Optional[bool] = None, sep: str = ';') -> Any:...

    @abstractmethod
    def run(self, *args, **kwargs) -> Any:...