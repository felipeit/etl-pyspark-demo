from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from src.application.pipeline_context import PipelineContext, default_logical_timestamp
from src.domain.quality.fingerprint import build_source_fingerprint
from src.domain.quality.gates import DataQualityGate
from src.domain.quality.rules import DuplicateKeyRule, MandatorySchemaRule, NullThresholdRule


@dataclass
class ETLResult:
    dataframe: Any
    context: PipelineContext
    persisted: bool


class ETLProcessorCSV:
    def __init__(self, filename: str) -> None:
        self.filename = filename

    def execute(self, input: Any) -> ETLResult:
        df = input.extractor.extract(self.filename)
        df = input.transformer.run(df)

        logical_timestamp = default_logical_timestamp()
        run_id = f"run-{uuid4()}"
        source_fingerprint = build_source_fingerprint(self.filename, logical_timestamp)

        quality_gate = DataQualityGate(
            rules=[
                MandatorySchemaRule(required_columns=list(df.columns)),
                NullThresholdRule(limits_by_column={column: 0.2 for column in list(df.columns)}),
                DuplicateKeyRule(key_columns=[list(df.columns)[0]]),
            ]
        )

        quality_result = quality_gate.evaluate(df)
        context = PipelineContext(
            run_id=run_id,
            source_fingerprint=source_fingerprint,
            logical_timestamp=logical_timestamp,
            quality_result=quality_result,
        )

        if quality_result.approved:
            persisted = input.loader.save(
                df,
                run_id=context.run_id,
                source_fingerprint=context.source_fingerprint,
            )
        else:
            input.loader.save_quarantine(
                df,
                run_id=context.run_id,
                source_fingerprint=context.source_fingerprint,
                reason="quality_gate_failed",
            )
            persisted = False

        return ETLResult(dataframe=df, context=context, persisted=persisted)
