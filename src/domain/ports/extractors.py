from abc import ABC, abstractmethod

from src.infra.data.ports.engine import Engine


class Extract(ABC):
    def __init__(self, engine: Engine = Engine()) -> None:
        self.engine = engine
    
    @abstractmethod
    def read(self, file_path:str , header:bool = True, sep:str=';') -> None:...

    @abstractmethod
    def run(self) -> None:...