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

    def read(self, file_path: str, header: bool = True, sep: str = ',') -> Any:
        header = self.header if header is None else header
        sep = self.sep if sep is None else sep
        spark = self.engine.get_session()
        return spark.read.option("header", str(header).lower()).option("sep", sep).csv(file_path, inferSchema=True)

    # keep `run` to satisfy ports
    def run(self, file_path: str) -> Any:
        return self.read(file_path)

    # compatibility convenience used by tests
    def extract(self, file_path: str) -> Any:
        return self.read(file_path)
