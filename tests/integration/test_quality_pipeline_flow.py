import csv

from src.application.usecase_handler_csv import ETLProcessorCSV
from src.domain.adapters.extractor import CSVExtractor
from src.domain.adapters.loader import DatabaseLoader
from src.domain.adapters.transformer import AnyFormatTransformer
from src.infra.repository.csv_file_repository import inMemoryCSVRepository


class DummyEngine:
    def get_session(self):
        raise RuntimeError("Spark session unavailable in tests")


class InputStub:
    def __init__(self, extractor, transformer, loader) -> None:
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader


def _write_csv(path, rows):
    columns = ["id", "name", "email"]
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def test_etl_batch_approved_goes_to_final_storage(tmp_path) -> None:
    csv_path = tmp_path / "approved.csv"
    _write_csv(
        csv_path,
        [
            {"id": 1, "name": "alice", "email": "a@mail.com"},
            {"id": 2, "name": "bob", "email": "b@mail.com"},
        ],
    )

    repo = inMemoryCSVRepository()
    input_data = InputStub(CSVExtractor(engine=DummyEngine()), AnyFormatTransformer(), DatabaseLoader(repo=repo))

    result = ETLProcessorCSV(str(csv_path)).execute(input_data)

    assert result.context.approved is True
    assert result.persisted is True
    assert len(repo.storage) == 1
    assert repo.quarantine == []


def test_etl_batch_rejected_goes_to_quarantine(tmp_path) -> None:
    csv_path = tmp_path / "rejected.csv"
    _write_csv(
        csv_path,
        [
            {"id": 1, "name": "alice", "email": ""},
            {"id": 1, "name": "alice-dup", "email": ""},
        ],
    )

    repo = inMemoryCSVRepository()
    input_data = InputStub(CSVExtractor(engine=DummyEngine()), AnyFormatTransformer(), DatabaseLoader(repo=repo))

    result = ETLProcessorCSV(str(csv_path)).execute(input_data)

    assert result.context.approved is False
    assert result.persisted is False
    assert repo.storage == []
    assert len(repo.quarantine) == 1
    assert repo.quarantine[0]["reason"] == "quality_gate_failed"
