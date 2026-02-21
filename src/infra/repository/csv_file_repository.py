from src.domain.ports.loaders import Loader
from typing import Any


class CSVData(dict):
    pass


class inMemoryCSVRepository(Loader):
    def __init__(self) -> None:
        self._storage: list[Any] = []
        self._quarantine: list[dict[str, Any]] = []
        self._processed_keys: set[tuple[str, str]] = set()

    @property
    def storage(self) -> list[Any]:
        return self._storage

    @property
    def quarantine(self) -> list[dict[str, Any]]:
        return self._quarantine

    def save(self, df: Any = None, *, run_id: str | None = None, source_fingerprint: str | None = None) -> bool:
        if run_id and source_fingerprint:
            key = (run_id, source_fingerprint)
            if key in self._processed_keys:
                return False
            self._processed_keys.add(key)

        self._storage.append(df)
        return True

    def save_quarantine(
        self,
        df: Any = None,
        *,
        run_id: str | None = None,
        source_fingerprint: str | None = None,
        reason: str | None = None,
    ) -> None:
        self._quarantine.append(
            {
                "run_id": run_id,
                "source_fingerprint": source_fingerprint,
                "reason": reason,
                "payload": df,
            }
        )


class CryptoRepository(Loader):
    def save(self, df: Any = None, *, run_id: str | None = None, source_fingerprint: str | None = None) -> bool:
        return True

    def save_quarantine(
        self,
        df: Any = None,
        *,
        run_id: str | None = None,
        source_fingerprint: str | None = None,
        reason: str | None = None,
    ) -> None:
        return None


CrytoRepository = CryptoRepository
inMemoryCSVRepository = inMemoryCSVRepository
CSVData = CSVData
