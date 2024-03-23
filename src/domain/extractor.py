from abc import ABC, abstractmethod
from typing import Any, Dict, List
from pyspark.sql.dataframe import DataFrame

from src.infra.data.spark_conn import SparkSingleton

class Extractor(ABC):
    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        pass


class CSVExtractor(Extractor):
    def __init__(self, spark: SparkSingleton = SparkSingleton()) -> None:
        self.__spark = spark.get_spark_session()

    def extract(self, filename) -> DataFrame:
        return self.__spark.read.csv(filename, header=True)