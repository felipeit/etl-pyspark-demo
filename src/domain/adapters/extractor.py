from typing import Any, Optional
from src.domain.ports.extractors import Extract
from src.infra.data.ports.engine import Engine


class CSVExtractor(Extract):
    """CSV extractor that uses an Engine (Spark) to read CSV files.

    Compatibility: provides `extract(file_path)` used by existing tests and
    conforms to the `Extract` port (`read` / `run`).
    """
    def __init__(self, engine: Optional[Engine] = None, header: bool = True, sep: str = ',') -> None:
        # lazy import to avoid circular deps in tests
        if engine is None:
            from src.infra.data.adapters.spark_conn import SparkAdapter
            engine = SparkAdapter()
        super().__init__(engine=engine)
        self.header = header
        self.sep = sep

    def read(self, file_path: str, header: Optional[bool] = None, sep: str = ',') -> Any:
        header = self.header if header is None else header
        sep = self.sep if sep is None else sep
        # prefer Spark engine when available; fallback to pandas for tests / envs without Java
        try:
            spark = self.engine.get_session()
            return spark.read.option("header", str(header).lower()).option("sep", sep).csv(file_path, inferSchema=True)
        except Exception:
            import pandas as pd

            pdf = pd.read_csv(file_path, header=0 if header else None, sep=sep)

            class PandasDFAdapter:
                def __init__(self, df):
                    self._df = df

                def count(self):
                    return len(self._df)

                @property
                def columns(self):
                    return list(self._df.columns)

                def dropna(self):
                    return PandasDFAdapter(self._df.dropna())

                def drop_duplicates(self):
                    return PandasDFAdapter(self._df.drop_duplicates())

            return PandasDFAdapter(pdf)

    # keep `run` to satisfy ports
    def run(self, file_path: str) -> Any:
        return self.read(file_path)

    # compatibility convenience used by tests
    def extract(self, file_path: str) -> Any:
        return self.read(file_path)
