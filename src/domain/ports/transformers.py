from abc import ABC, abstractmethod
from typing import Any


class Transformer(ABC):
    @abstractmethod
    def run(self, df: Any) -> Any:
        pass