"""Evaluation data contracts and integrity helpers for GOLD BOT."""

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

__all__ = [
    "CostKnowledge",
    "EvaluationDirection",
    "EvaluationOutcomeContract",
    "EvaluationOutcomeStatus",
    "EvaluationPeriodContract",
    "EvaluationRunContract",
    "EvaluationSignalContract",
    "ExecutionPriceSide",
    "FilterDecision",
    "OutcomeMeasurementBasis",
    "SpreadAccounting",
    "configuration_snapshot_sha256",
    "validate_evaluation_period_set",
    "validate_evaluation_relationships",
]
