from abc import ABC, abstractmethod
from typing import Any, Dict, List


class Transformer(ABC):
    @abstractmethod
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass

class JSONTransformer(Transformer):
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Lógica de transformação de dados JSON
        pass
