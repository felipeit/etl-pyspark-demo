from src.domain.ports.loaders import Loader
from typing import Any


# Minimal in-memory / CSV repository stubs used by tests and examples.
class CSVData(dict):
    pass


class inMemoryCSVRepository(Loader):
    def __init__(self) -> None:
        self._storage: list[Any] = []

    def save(self, df: Any = None) -> None:
        # store a lightweight reference (tests only rely on callability)
        self._storage.append(df)


class CryptoRepository(Loader):
    def save(self, df: Any = None) -> None:
        # real implementation would persist to DB/datalake
        return None


# backwards-compatible names (there are several typos/usages across the repo)
CrytoRepository = CryptoRepository
inMemoryCSVRepository = inMemoryCSVRepository
CSVData = CSVData