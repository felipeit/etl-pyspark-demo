from typing import Any
from pyspark.sql import SparkSession

from src.infra.data.ports.engine import Engine


class SparkAdapter(Engine):
    _instance = None

    def __new__(cls) -> 'SparkAdapter':
        if not hasattr(cls, 'instance'):
            cls.instance = super(SparkAdapter, cls).__new__(cls)
        return cls.instance

    def get_session(self) -> SparkSession:
        return SparkSession.builder \
            .appName("elt-pyspark-demo") \
            .getOrCreate()