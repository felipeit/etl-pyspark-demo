class DbSQLAlchemy:
    """Minimal compatibility shim used by tests (in-memory stub).

    Concrete implementations should implement `save()`.
    """
    def save(self) -> None:  # pragma: no cover - trivial
        raise NotImplementedError
