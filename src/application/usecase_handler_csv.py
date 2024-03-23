from src.application.base_handler import ETL, Input, Output


class ETLProcessorCSV(ETL):
    def __init__(self, filename) -> None:
        self.__filename = filename

    def process(self, input: Input) -> Output:
        dataframe = input.extractor.extract(self.__filename)
        transformed_data = input.transformer.transform(dataframe)
        input.loader.load(transformed_data)
        return "works"
        