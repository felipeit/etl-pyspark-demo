import os
from typing import Any
from pyspark.sql import SparkSession

from src.infra.data.ports.engine import Engine


class SparkAdapter(Engine):
    """Singleton Spark session provider.

    If environment variable `SPARK_MASTER_URL` is set, the SparkSession will be
    configured to connect to that master (useful when running inside docker
    compose). Otherwise a local session is created.
    """
    _instance = None

    def __new__(cls) -> 'SparkAdapter':
        if cls._instance is None:
            cls._instance = super(SparkAdapter, cls).__new__(cls)
        return cls._instance

    def get_session(self) -> SparkSession:
        builder = SparkSession.builder.appName("elt-pyspark-demo")
        master_url = os.environ.get("SPARK_MASTER_URL")
        if master_url:
            builder = builder.master(master_url)
        return builder.getOrCreate()


# backward-compatible name used in some modules
SparkAdapters = SparkAdapter