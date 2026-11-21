from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from backend.evaluation.contracts import EvaluationSignalContract
from backend.market_data.contracts import MarketCandleDataContract


class EvaluationCandleRepository(Protocol):
    """Read-only interface required from persisted market-data storage."""

    def get_candle_contract(
        self, candle_id: int
    ) -> MarketCandleDataContract | None:
        ...


class EvaluationMarketDataError(ValueError):
    """Raised when signal source candles fail evaluation provenance checks."""


@dataclass(frozen=True, slots=True)
class EvaluationMarketDataEvidence:
    """Verified source candles resolved for one immutable evaluation signal."""

    signal_id: str
    candles: tuple[MarketCandleDataContract, ...]
    source_candle_ids: tuple[int, ...]
    source_batch_ids: tuple[int, ...]
    latest_available_at: datetime


class EvaluationMarketDataReader:
    """Resolve signal inputs from persisted candles without fetching or repairing data.

    This component is intentionally read-only. It verifies source identity,
    temporal availability, closed/verified candle state, clock evidence, and
    the ingestion validator's persisted analysis-eligibility decision.
    """

    def __init__(self, repository: EvaluationCandleRepository) -> None:
        if not callable(getattr(repository, "get_candle_contract", None)):
            raise TypeError("repository must provide get_candle_contract().")
        self._repository = repository

    def resolve_signal_inputs(
        self,
        signal: EvaluationSignalContract,
    ) -> EvaluationMarketDataEvidence:
        if not isinstance(signal, EvaluationSignalContract):
            raise TypeError("signal must be an EvaluationSignalContract.")

        candles: list[MarketCandleDataContract] = []
        actual_batch_ids: set[int] = set()

        for candle_id in signal.source_candle_ids:
            candle = self._repository.get_candle_contract(candle_id)
            if candle is None:
                raise EvaluationMarketDataError(
                    f"Source candle {candle_id} does not exist."
                )
            if not isinstance(candle, MarketCandleDataContract):
                raise EvaluationMarketDataError(
                    f"Source candle {candle_id} did not return a valid data contract."
                )
            if candle.candle_id != candle_id:
                raise EvaluationMarketDataError(
                    f"Source candle identity mismatch for {candle_id}."
                )
            if candle.instrument != signal.instrument:
                raise EvaluationMarketDataError(
                    f"Source candle {candle_id} belongs to another instrument."
                )
            if candle.event_time > signal.signal_at:
                raise EvaluationMarketDataError(
                    f"Source candle {candle_id} occurs after the signal."
                )
            if candle.available_at > signal.input_available_at:
                raise EvaluationMarketDataError(
                    f"Source candle {candle_id} became available after signal inputs were frozen."
                )
            if not candle.is_closed or candle.candle_quality != "VERIFIED":
                raise EvaluationMarketDataError(
                    f"Source candle {candle_id} is not a verified closed candle."
                )
            if candle.clock_verified is not True:
                raise EvaluationMarketDataError(
                    f"Source candle {candle_id} has no verified source clock."
                )
            if candle.analysis_eligible is not True:
                raise EvaluationMarketDataError(
                    f"Source candle {candle_id} was not approved for analysis."
                )
            report = candle.validation_report
            if not isinstance(report, dict) or report.get("analysis_eligible") is not True:
                raise EvaluationMarketDataError(
                    f"Source candle {candle_id} lacks positive persisted validation evidence."
                )

            actual_batch_ids.add(candle.batch_id)
            candles.append(candle)

        expected_batch_ids = set(signal.source_batch_ids)
        if actual_batch_ids != expected_batch_ids:
            raise EvaluationMarketDataError(
                "Signal source_batch_ids do not exactly match the batches of its source candles."
            )

        latest_available_at = max(item.available_at for item in candles)
        if latest_available_at > signal.input_available_at:
            # Kept as a final aggregate guard in addition to the per-candle check.
            raise EvaluationMarketDataError(
                "Resolved source data was not fully available at the signal input cutoff."
            )

        return EvaluationMarketDataEvidence(
            signal_id=str(signal.signal_id),
            candles=tuple(candles),
            source_candle_ids=tuple(item.candle_id for item in candles),
            source_batch_ids=tuple(sorted(actual_batch_ids)),
            latest_available_at=latest_available_at,
        )
