
from abc import ABC, abstractmethod


class Engine(ABC):

    @abstractmethod
    def get_session(self) -> None:...
