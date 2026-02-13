from typing import Any
from src.domain.ports.transformers import Transformer


class JSONTransformer(Transformer):
    def run(self, df: Any) -> Any:
        df = df.dropna()
        df = df.drop_duplicates()
        return df
    
class AnyFormatTransformer(Transformer):
    def run(self, df: Any) -> Any: 
        return df