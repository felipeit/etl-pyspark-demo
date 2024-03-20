from abc import ABC, abstractmethod
from typing import Any, Dict, List


class Loader(ABC()):
    @abstractmethod
    def load(self, data: List[Dict[str, Any]]) -> None:
        pass


class DatabaseLoader(Loader):
    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    def load(self, data: List[Dict[str, Any]]) -> None:
        # Lógica de carregamento de dados em um banco de dados
        pass