from typing import Optional
from src.domain.ports.loaders import Loader


class DatabaseLoader(Loader):
    def __init__(self, repo: Optional[Loader] = None) -> None:
        if repo is None:
            from src.infra.repository.csv_file_repository import inMemoryCSVRepository

            repo = inMemoryCSVRepository()
        self._repo = repo

    def save(self, df=None, *, run_id: str | None = None, source_fingerprint: str | None = None) -> bool:
        """Persist dataframe via provided repository with idempotency metadata."""
        return self._repo.save(df, run_id=run_id, source_fingerprint=source_fingerprint)

    def save_quarantine(
        self,
        df=None,
        *,
        run_id: str | None = None,
        source_fingerprint: str | None = None,
        reason: str | None = None,
    ) -> None:
        self._repo.save_quarantine(
            df,
            run_id=run_id,
            source_fingerprint=source_fingerprint,
            reason=reason,
        )

    def run(self, df=None) -> None:
        return self.save(df)
