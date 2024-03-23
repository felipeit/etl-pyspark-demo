from application.usecase_handler_csv import ETLProcessorCSV
from src.application.base_handler import Input
from src.domain.extractor import CSVExtractor
from src.domain.loader import DatabaseLoader
from src.domain.transformer import JSONTransformer

input = Input(
    extractor=CSVExtractor(), 
    transformer=JSONTransformer(), 
    loader=DatabaseLoader()
)

etl_processor = ETLProcessorCSV(file="teste.csv")
etl_processor.process(input)
