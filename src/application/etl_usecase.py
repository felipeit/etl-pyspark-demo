from typing import Any
from src.application.icof import HandlerChainOfResponsability
from src.domain.adapters.extractor import CSVExtractor
from src.domain.adapters.loader import DatabaseLoader
from src.domain.adapters.transformer import JSONTransformer
from src.domain.ports.loaders import Loader
from src.infra.data.ports.engine import Engine
from src.infra.data.adapters.spark_conn import SparkAdapters
from src.infra.repository.csv_file_repository import CrytoRepository


class ETLUsecase(HandlerChainOfResponsability):
    def __init__(self, engine: Engine = SparkAdapters(), repo: Loader = CrytoRepository()) -> None:
        self._engine = engine
        self._repo = repo

    def execute(self) -> Any:
        self.set_next(CSVExtractor(engine=self._engine))
        self.set_next(JSONTransformer())
        self.set_next(DatabaseLoader(repo=self._repo))
        self.run()