from abc import ABC, abstractmethod
from typing import Any


class Loader(ABC):
    @abstractmethod
    def save(self, df: Any = None, *, run_id: str | None = None, source_fingerprint: str | None = None) -> bool: ...

    def save_quarantine(
        self,
        df: Any = None,
        *,
        run_id: str | None = None,
        source_fingerprint: str | None = None,
        reason: str | None = None,
    ) -> None:
        raise NotImplementedError
