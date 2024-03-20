from abc import ABC, abstractmethod
from importlib.abc import Loader
from typing import Any, Dict, List

from src.domain.extractor import CSVExtractor, Extractor
from src.domain.loader import DatabaseLoader
from src.domain.transformer import JSONTransformer, Transformer


class ETLProcessor:
    def __init__(self, extractor: Extractor, transformer: Transformer, loader: Loader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def process(self) -> None:
        data = self.extractor.extract()
        transformed_data = self.transformer.transform(data)
        self.loader.load(transformed_data)

# Exemplo de uso

if __name__ == "__main__":
    extractor = CSVExtractor("data.csv")
    transformer = JSONTransformer()
    loader = DatabaseLoader("connection_string")

    etl_processor = ETLProcessor(extractor, transformer, loader)
    etl_processor.process()
