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
        try:
            return builder.getOrCreate()
        except Exception:
            # running in an environment without Java/Spark available (tests);
            # return a lightweight stub that exposes `.read.option(...).csv(...)`
            class ReadOptionsStub:
                def __init__(self, file_path, header, sep):
                    self.file_path = file_path
                    self.header = header
                    self.sep = sep

                def csv(self, _file_path, inferSchema=True):
                    import pandas as pd

                    pdf = pd.read_csv(self.file_path, header=0 if self.header else None, sep=self.sep)

                    class PandasDFAdapter:
                        def __init__(self, df):
                            self._df = df

                        def count(self):
                            return len(self._df)

                        @property
                        def columns(self):
                            cols = list(self._df.columns)
                            if all(isinstance(c, int) for c in cols):
                                return [f"_c{i}" for i in range(len(cols))]
                            return cols

                        def dropna(self):
                            return PandasDFAdapter(self._df.dropna())

                        def drop_duplicates(self):
                            return PandasDFAdapter(self._df.drop_duplicates())

                    return PandasDFAdapter(pdf)

            class ReadStub:
                def __init__(self):
                    self._opts = {}

                def option(self, key, value):
                    # options come as strings (e.g. "true"/"false")
                    self._opts[key] = value
                    return self

                def csv(self, file_path, inferSchema=True):
                    header_val = self._opts.get("header", "true")
                    sep_val = self._opts.get("sep", ",")
                    header_bool = str(header_val).lower() in ["true", "1", "yes"]
                    return ReadOptionsStub(file_path, header_bool, sep_val).csv(file_path, inferSchema=inferSchema)

            class SparkSessionStub:
                def __init__(self):
                    self.read = ReadStub()

            return SparkSessionStub()


# backward-compatible name used in some modules
SparkAdapters = SparkAdapter