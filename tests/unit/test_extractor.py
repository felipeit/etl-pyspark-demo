import pytest
from src.domain.extractor import CSVExtractor, Extractor


async def test_verify_if_instance_from_extractor() -> None:
    # Arrange
    output = CSVExtractor()
    
    # Act & Assert
    assert isinstance(output, Extractor)

async def test_extract_reads_csv_with_header(csv_test) -> None:
    # Arrange
    csv_extractor = CSVExtractor()

    # Act
    df = csv_extractor.extract(csv_test)

    # Assert
    assert df.count() == 100
    assert df.columns == ['First Name', 'Last Name', 'Email Address', 'Phone Number','Date Of Birth','Current Loyalty Point Total']

async def test_extract_reads_csv_without_header(csv_test) -> None:
    # Arrange
    csv_extractor = CSVExtractor(header=False)

    # Act
    df = csv_extractor.extract(csv_test)

    # Assert
    assert df.count() == 101
    assert df.columns == ["_c0", "_c1", "_c2", "_c3", "_c4", "_c5"]

async def test_extract_raises_exception_for_nonexistent_file(csv_test) -> None:
    # Arrange
    csv_extractor = CSVExtractor()

    # Act & Assert
    with pytest.raises(Exception):
        csv_extractor.extract("random.csv")

