""""Evaluation contracts and orchestration for GOLD BOT."""

from backend.evaluation.contracts import (
    CostKnowledge,
    EvaluationDirection,
    EvaluationOutcomeContract,
    EvaluationOutcomeStatus,
    EvaluationPeriodContract,
    EvaluationRunContract,
    EvaluationSignalContract,
    ExecutionPriceSide,
    FilterDecision,
    OutcomeMeasurementBasis,
    SpreadAccounting,
    configuration_snapshot_sha256,
    validate_evaluation_period_set,
    validate_evaluation_relationships,
)
from backend.evaluation.service import (
    EvaluationService,
    HorizonSampleReadiness,
)

__all__ = [
    "CostKnowledge",
    "EvaluationDirection",
    "EvaluationOutcomeContract",
    "EvaluationOutcomeStatus",
    "EvaluationPeriodContract",
    "EvaluationRunContract",
    "EvaluationService",
    "EvaluationSignalContract",
    "ExecutionPriceSide",
    "FilterDecision",
    "HorizonSampleReadiness",
    "OutcomeMeasurementBasis",
    "SpreadAccounting",
    "configuration_snapshot_sha256",
    "validate_evaluation_period_set",
    "validate_evaluation_relationships",
]
