from typing import Optional
from src.domain.ports.loaders import Loader


class DatabaseLoader(Loader):
    def __init__(self, repo: Optional[Loader] = None) -> None:
        # lazy import default implementation to avoid circular imports in tests
        if repo is None:
            from src.infra.repository.csv_file_repository import inMemoryCSVRepository

            repo = inMemoryCSVRepository()
        self._repo = repo

    def save(self, df=None) -> None:
        """Persist dataframe via provided repository."""
        self._repo.save(df)

    # backward-compatible convenience
    def run(self, df=None) -> None:
        return self.save(df)