import pytest
from unittest.mock import Mock
from src.domain.loader import DatabaseLoader, Loader

from tests.conftest import MockDbSQLAlchemy, MockRepository

async def test_verify_if_instance_from_loader() -> None:
    # Arrange
    output = DatabaseLoader()
    # Act & Assert
    assert isinstance(output, Loader)

@pytest.mark.skip
async def test_method_process_of_loader_called_on_save(database_loader) -> None:
    # Arrange
    mock_db = Mock(MockDbSQLAlchemy())
    mock_db.save = Mock(return_value=...)
    mock_repo = Mock(MockRepository())
    database_loader.db = mock_db

    # Act
    database_loader.load(mock_repo)

    # Assert
    assert mock_db.save.assert_called_once()
