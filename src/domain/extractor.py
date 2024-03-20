from abc import ABC, abstractmethod
from typing import Any, Dict, List


class Extractor(ABC()):
    @abstractmethod()
    def extract(self) -> List[Dict[str, Any]]:
        pass

class CSVExtractor(Extractor):
    def __init__(self, filename: str):
        self.filename = filename

    def extract(self) -> List[Dict[str, Any]]:
        # Lógica de extração de dados de um arquivo CSV
        pass