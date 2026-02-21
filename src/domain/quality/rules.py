from abc import ABC, abstractmethod
from typing import Any

from src.domain.quality.result import QualityViolation


def _count_rows(df: Any) -> int:
    return int(df.count())


def _get_columns(df: Any) -> list[str]:
    return list(df.columns)


def _null_count(df: Any, column: str) -> int:
    if hasattr(df, "filter") and hasattr(df, "where"):
        from pyspark.sql.functions import col

        return int(df.filter(col(column).isNull()).count())

    return int(df[column].isnull().sum())


def _duplicate_key_count(df: Any, key_columns: list[str]) -> int:
    if hasattr(df, "groupBy"):
        from pyspark.sql.functions import col

        grouped = df.groupBy(*key_columns).count().where(col("count") > 1)
        return int(grouped.count())

    duplicated = df.duplicated(subset=key_columns, keep=False)
    return int(duplicated.sum())


class QualityRule(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def validate(self, df: Any) -> list[QualityViolation]: ...


class MandatorySchemaRule(QualityRule):
    def __init__(self, required_columns: list[str]) -> None:
        self.required_columns = required_columns

    @property
    def name(self) -> str:
        return "mandatory_schema"

    def validate(self, df: Any) -> list[QualityViolation]:
        available = set(_get_columns(df))
        missing = [col for col in self.required_columns if col not in available]
        if not missing:
            return []
        return [
            QualityViolation(
                rule_name=self.name,
                message=f"Missing required columns: {', '.join(missing)}",
            )
        ]


class NullThresholdRule(QualityRule):
    def __init__(self, limits_by_column: dict[str, float]) -> None:
        self.limits_by_column = limits_by_column

    @property
    def name(self) -> str:
        return "null_threshold"

    def validate(self, df: Any) -> list[QualityViolation]:
        total = _count_rows(df)
        if total == 0:
            return []

        violations: list[QualityViolation] = []
        for column, threshold in self.limits_by_column.items():
            null_ratio = _null_count(df, column) / total
            if null_ratio > threshold:
                violations.append(
                    QualityViolation(
                        rule_name=self.name,
                        message=(
                            f"Column '{column}' null ratio {null_ratio:.2%} exceeds {threshold:.2%}"
                        ),
                    )
                )
        return violations


class DuplicateKeyRule(QualityRule):
    def __init__(self, key_columns: list[str]) -> None:
        self.key_columns = key_columns

    @property
    def name(self) -> str:
        return "duplicate_key"

    def validate(self, df: Any) -> list[QualityViolation]:
        duplicated = _duplicate_key_count(df, self.key_columns)
        if duplicated == 0:
            return []
        return [
            QualityViolation(
                rule_name=self.name,
                message=f"Found {duplicated} rows with duplicated business key: {self.key_columns}",
            )
        ]
