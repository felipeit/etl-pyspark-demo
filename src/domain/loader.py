from abc import ABC, abstractmethod
from src.infra.orm.db_sqlalchemy import DbSQLAlchemy
from src.infra.repository.csv_file_repository import inMemoryCSVRepository


class Loader(ABC):
    @abstractmethod
    def load(self, repo: inMemoryCSVRepository) -> None:
        pass


class DatabaseLoader(Loader):
    def load(self, repo: inMemoryCSVRepository) -> None:
        db = DbSQLAlchemy()
        db.save()