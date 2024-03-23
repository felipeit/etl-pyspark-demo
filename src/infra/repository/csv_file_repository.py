
from dataclasses import dataclass
from typing import Any


@dataclass
class CSVData:
    title: str
    data: Any


class inMemoryCSVRepository:
    def __init__(self, data: CSVData) -> None:
        self.__title = data.title
        self.__file = data.data