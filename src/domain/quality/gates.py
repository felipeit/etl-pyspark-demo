from typing import Any

from src.domain.quality.result import QualityGateResult
from src.domain.quality.rules import QualityRule


class DataQualityGate:
    def __init__(self, rules: list[QualityRule]) -> None:
        self._rules = rules

    def evaluate(self, df: Any) -> QualityGateResult:
        result = QualityGateResult()
        for rule in self._rules:
            for violation in rule.validate(df):
                result.add_violation(rule_name=violation.rule_name, message=violation.message)
        return result
