from typing import Any
from dataenforce import Dataset
from src.domain.ports.extractors import Extract
from src.infra.data.ports.engine import Engine


class CSVExtractor(Extract):
    def __init__(self) -> None:
        self.engine.get_session()

    def run(self, file_path:str) -> Dataset[Any, ...]:
        return self.read(file_path)
    