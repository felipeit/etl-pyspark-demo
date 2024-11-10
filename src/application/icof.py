from abc import ABC
from typing import Any


class HandlerChainOfResponsability(ABC):
    def __init__(self) -> None:
        self._pipeline = []
    
    def set_next(self, next_step: Any) -> None:
        if next_step not in  self._pipeline: 
           self._pipeline.append(next_step)   
    
    def run(self) -> None:
        try:
            for pl in self._pipeline:
                pl.run()
        except Exception as err:
            pass