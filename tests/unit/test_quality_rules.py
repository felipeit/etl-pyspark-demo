from src.domain.quality.gates import DataQualityGate
from src.domain.quality.rules import DuplicateKeyRule, MandatorySchemaRule, NullThresholdRule


class SeriesStub:
    def __init__(self, values):
        self.values = values

    def isnull(self):
        return SeriesStub([v is None for v in self.values])

    def sum(self):
        return sum(self.values)


class DataFrameStub:
    def __init__(self, rows):
        self.rows = rows
        self.columns = list(rows[0].keys()) if rows else []

    def count(self):
        return len(self.rows)

    def __getitem__(self, column):
        return SeriesStub([row.get(column) for row in self.rows])

    def duplicated(self, subset, keep=False):
        seen = {}
        for row in self.rows:
            key = tuple(row[col] for col in subset)
            seen[key] = seen.get(key, 0) + 1
        return SeriesStub([seen[tuple(row[col] for col in subset)] > 1 for row in self.rows])


def test_quality_rules_approved_batch() -> None:
    df = DataFrameStub(
        [
            {"id": 1, "name": "alice", "email": "a@mail.com"},
            {"id": 2, "name": "bob", "email": "b@mail.com"},
        ]
    )

    gate = DataQualityGate(
        rules=[
            MandatorySchemaRule(required_columns=["id", "name", "email"]),
            NullThresholdRule(limits_by_column={"email": 0.2}),
            DuplicateKeyRule(key_columns=["id"]),
        ]
    )

    result = gate.evaluate(df)

    assert result.approved is True
    assert result.violations == []


def test_quality_rules_rejected_batch() -> None:
    df = DataFrameStub(
        [
            {"id": 1, "name": "alice", "email": None},
            {"id": 1, "name": "alice-dup", "email": None},
        ]
    )

    gate = DataQualityGate(
        rules=[
            MandatorySchemaRule(required_columns=["id", "name", "email", "country"]),
            NullThresholdRule(limits_by_column={"email": 0.2}),
            DuplicateKeyRule(key_columns=["id"]),
        ]
    )

    result = gate.evaluate(df)

    assert result.approved is False
    assert len(result.violations) == 3
    rule_names = {v.rule_name for v in result.violations}
    assert rule_names == {"mandatory_schema", "null_threshold", "duplicate_key"}
