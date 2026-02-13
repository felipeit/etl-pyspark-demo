from typing import Any
from src.application.base_handler import Input


class ETLProcessorCSV:
    def __init__(self, filename: str) -> None:
        self.filename = filename

    def execute(self, input: Input) -> Any:
        df = input.extractor.extract(self.filename)
        df = input.transformer.run(df)
        input.loader.save(df)
        return df
