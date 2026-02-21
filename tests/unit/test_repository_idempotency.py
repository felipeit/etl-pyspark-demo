from src.infra.repository.csv_file_repository import inMemoryCSVRepository


def test_repository_idempotency_by_run_id_and_fingerprint() -> None:
    repo = inMemoryCSVRepository()

    first = repo.save({"payload": 1}, run_id="run-1", source_fingerprint="fp-1")
    second = repo.save({"payload": 1}, run_id="run-1", source_fingerprint="fp-1")

    assert first is True
    assert second is False
    assert len(repo.storage) == 1
