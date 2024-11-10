from abc import ABC, abstractmethod
from typing import Any
from dataenforce import Dataset


class Transformer(ABC):
    @abstractmethod
    def run(self, df:Dataset[Any, ...]) -> Dataset[Any, ...]:
        pass