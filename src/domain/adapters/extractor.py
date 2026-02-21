from typing import Any, Optional
import csv

from src.domain.ports.extractors import Extract
from src.infra.data.ports.engine import Engine


class CSVExtractor(Extract):
    """CSV extractor that uses an Engine (Spark) to read CSV files."""

    def __init__(self, engine: Optional[Engine] = None, header: bool = True, sep: str = ',') -> None:
        if engine is None:
            from src.infra.data.adapters.spark_conn import SparkAdapter

            engine = SparkAdapter()
        super().__init__(engine=engine)
        self.header = header
        self.sep = sep

    def read(self, file_path: str, header: Optional[bool] = None, sep: str = ',') -> Any:
        header = self.header if header is None else header
        sep = self.sep if sep is None else sep
        try:
            spark = self.engine.get_session()
            return spark.read.option("header", str(header).lower()).option("sep", sep).csv(file_path, inferSchema=True)
        except Exception:
            try:
                import pandas as pd

                pdf = pd.read_csv(file_path, header=0 if header else None, sep=sep)
                rows = pdf.to_dict(orient="records")
                columns = list(pdf.columns)
            except Exception:
                with open(file_path, newline="", encoding="utf-8") as fp:
                    reader = csv.reader(fp, delimiter=sep)
                    raw = list(reader)
                if header:
                    columns = raw[0]
                    rows = [dict(zip(columns, row)) for row in raw[1:]]
                else:
                    columns = [f"_c{i}" for i in range(len(raw[0]))]
                    rows = [dict(zip(columns, row)) for row in raw]

            class DataFrameAdapter:
                def __init__(self, data_rows, data_columns):
                    self._rows = data_rows
                    self._columns = data_columns

                def count(self):
                    return len(self._rows)

                @property
                def columns(self):
                    cols = list(self._columns)
                    if all(isinstance(c, int) for c in cols):
                        return [f"_c{i}" for i in range(len(cols))]
                    return cols

                def dropna(self):
                    filtered = [r for r in self._rows if all(v not in (None, "") for v in r.values())]
                    return DataFrameAdapter(filtered, self._columns)

                def drop_duplicates(self):
                    seen = set()
                    deduped = []
                    for row in self._rows:
                        marker = tuple(row.get(col) for col in self._columns)
                        if marker not in seen:
                            seen.add(marker)
                            deduped.append(row)
                    return DataFrameAdapter(deduped, self._columns)

                def __getitem__(self, column):
                    class SeriesAdapter:
                        def __init__(self, values):
                            self._values = values

                        def isnull(self):
                            return SeriesAdapter([v in (None, "") for v in self._values])

                        def sum(self):
                            return sum(self._values)

                    return SeriesAdapter([row.get(column) for row in self._rows])

                def duplicated(self, subset, keep=False):
                    counts = {}
                    for row in self._rows:
                        key = tuple(row.get(col) for col in subset)
                        counts[key] = counts.get(key, 0) + 1

                    class SeriesAdapter:
                        def __init__(self, values):
                            self._values = values

                        def sum(self):
                            return sum(self._values)

                    return SeriesAdapter([
                        counts[tuple(row.get(col) for col in subset)] > 1 for row in self._rows
                    ])

            return DataFrameAdapter(rows, columns)

    def run(self, file_path: str) -> Any:
        return self.read(file_path)

    def extract(self, file_path: str) -> Any:
        return self.read(file_path)
