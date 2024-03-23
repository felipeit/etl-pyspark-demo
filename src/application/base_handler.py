from abc import ABC, abstractmethod
from dataclasses import dataclass

from src.domain.extractor import Extractor
from src.domain.loader import Loader
from src.domain.transformer import Transformer


@dataclass
class Input:
    extractor: Extractor
    transformer: Transformer
    loader: Loader

@dataclass
class Output:
    message: str


class ETL(ABC):
    @abstractmethod
    def process(self, input: Input) -> Output:
        pass