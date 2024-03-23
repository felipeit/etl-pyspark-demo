from abc import ABC, abstractmethod
from pyspark.sql.dataframe import DataFrame

from src.infra.repository.csv_file_repository import inMemoryCSVRepository


class Transformer(ABC):
    @abstractmethod
    def transform(self, data: DataFrame) -> inMemoryCSVRepository:
        pass

class JSONTransformer(Transformer):
    def transform(self, dataframe: DataFrame) -> inMemoryCSVRepository:
        try:
            data = dataframe.toJSON().collect()
            repo = inMemoryCSVRepository(title="csv-test-random", data=data)
        except Exception as err:
            pass
        return repo
