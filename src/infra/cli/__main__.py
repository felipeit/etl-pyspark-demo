from src.application.etl_usecase import ETLProcessorCSV
from src.application.base_handler import Input
from src.domain.extractor import CSVExtractor
from src.domain.loader import DatabaseLoader
from src.domain.transformer import JSONTransformer

input = Input(
    extractor=CSVExtractor(), 
    transformer=JSONTransformer(), 
    loader=DatabaseLoader()
)

etl_processor = ETLProcessorCSV(filename="teste.csv")
etl_processor.execute(input)
