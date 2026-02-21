from dataclasses import dataclass, field


@dataclass(frozen=True)
class QualityViolation:
    rule_name: str
    message: str


@dataclass
class QualityGateResult:
    violations: list[QualityViolation] = field(default_factory=list)

    @property
    def approved(self) -> bool:
        return len(self.violations) == 0

    def add_violation(self, rule_name: str, message: str) -> None:
        self.violations.append(QualityViolation(rule_name=rule_name, message=message))
