from src.domain.extractor import CSVExtractor, Extractor


async def test_verify_if_instance_from_extractor() -> None:
    output = CSVExtractor()
    assert isinstance(output, Extractor)

async def test_verify_csvextractor_process_a_csv_file_with_success() -> None:
    pass