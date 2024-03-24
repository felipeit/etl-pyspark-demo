from abc import ABC, abstractmethod
from pyspark.sql.dataframe import DataFrame

from src.infra.repository.csv_file_repository import CSVData, inMemoryCSVRepository


class Transformer(ABC):
    @abstractmethod
    def transform(self, data: DataFrame) -> inMemoryCSVRepository:
        pass

class JSONTransformer(Transformer):
    def transform(self, dataframe: DataFrame) -> inMemoryCSVRepository:
        data = dataframe.toJSON().collect()
        dto = CSVData(title="csv-test-random", data=data)
        repo = inMemoryCSVRepository(dto)
        return repo
