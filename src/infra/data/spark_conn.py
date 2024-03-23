from typing import Any
from pyspark.sql import SparkSession


class SparkSingleton:
    _instance = None

    def __new__(cls) -> 'SparkSingleton':
        if not hasattr(cls, 'instance'):
            cls.instance = super(SparkSingleton, cls).__new__(cls)
        return cls.instance

    def get_spark_session(self) -> SparkSession:
        return SparkSession.builder \
            .appName("elt-pyspark-demo") \
            .getOrCreate()