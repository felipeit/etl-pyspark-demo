from dataclasses import dataclass, field
from datetime import datetime, timezone

from src.domain.quality.result import QualityGateResult


@dataclass
class PipelineContext:
    run_id: str
    source_fingerprint: str
    logical_timestamp: str
    quality_result: QualityGateResult | None = None
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def approved(self) -> bool:
        return self.quality_result.approved if self.quality_result else False


def default_logical_timestamp() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
