from typing import Any
from src.domain.ports.transformers import Transformer
from dataenforce import Dataset



class JSONTransformer(Transformer):
    def run(self, df:  Dataset[Any, ...]) ->  Dataset[Any, ...]:
        df = df.dropna()
        df = df.drop_duplicates()
        return df
    
class AnyFormatTransformer(Transformer):
    def run(self, df:  Dataset[Any, ...]) ->  Dataset[Any, ...]: 
        return df