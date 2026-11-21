from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    JSON,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class DatabaseBase(DeclarativeBase):
    """Base class for persistent GOLD BOT database models."""


class MarketDataBatchRecord(DatabaseBase):
    """Metadata describing one received market-data batch."""

    __tablename__ = "market_data_batches"
    __table_args__ = (
        CheckConstraint("candle_count > 0", name="ck_batch_candle_count_positive"),
        Index("ix_market_data_batches_symbol_timeframe", "symbol", "timeframe"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(8), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    candle_count: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    clock_verified: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    clock_account_login: Mapped[int | None] = mapped_column(Integer, nullable=True)
    clock_broker_server: Mapped[str | None] = mapped_column(String(128), nullable=True)
    clock_terminal_build: Mapped[int | None] = mapped_column(Integer, nullable=True)
    clock_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    clock_ntp_offset_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    clock_ntp_uncertainty_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    clock_ntp_sample_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    clock_ntp_servers: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    clock_candle_offset_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    clock_candle_uncertainty_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    clock_calibration_sample_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    validation_report: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    candles: Mapped[list[MarketCandleRecord]] = relationship(
        back_populates="batch", cascade="all, delete-orphan", passive_deletes=True
    )


class MarketCandleRecord(DatabaseBase):
    """Persisted OHLC candle with source and quality metadata."""

    __tablename__ = "market_candles"
    __table_args__ = (
        UniqueConstraint("symbol", "timeframe", "timestamp", name="uq_market_candles_identity"),
        CheckConstraint("open_price > 0", name="ck_candle_open_positive"),
        CheckConstraint("high_price > 0", name="ck_candle_high_positive"),
        CheckConstraint("low_price > 0", name="ck_candle_low_positive"),
        CheckConstraint("close_price > 0", name="ck_candle_close_positive"),
        CheckConstraint("high_price >= low_price", name="ck_candle_high_gte_low"),
        CheckConstraint("high_price >= open_price AND high_price >= close_price", name="ck_candle_high_covers_prices"),
        CheckConstraint("low_price <= open_price AND low_price <= close_price", name="ck_candle_low_covers_prices"),
        CheckConstraint("tick_volume >= 0", name="ck_candle_tick_volume_nonnegative"),
        CheckConstraint("real_volume IS NULL OR real_volume >= 0", name="ck_candle_real_volume_nonnegative"),
        Index("ix_market_candles_symbol_timeframe_timestamp", "symbol", "timeframe", "timestamp"),
        Index("ix_market_candles_batch_id", "batch_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    batch_id: Mapped[int] = mapped_column(ForeignKey("market_data_batches.id", ondelete="CASCADE"), nullable=False)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(8), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    open_price: Mapped[float] = mapped_column(Float, nullable=False)
    high_price: Mapped[float] = mapped_column(Float, nullable=False)
    low_price: Mapped[float] = mapped_column(Float, nullable=False)
    close_price: Mapped[float] = mapped_column(Float, nullable=False)
    tick_volume: Mapped[int] = mapped_column(Integer, nullable=False)
    real_volume: Mapped[int | None] = mapped_column(Integer, nullable=True)
    volume_source: Mapped[str] = mapped_column(String(16), nullable=False)
    is_closed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    quality: Mapped[str] = mapped_column(String(16), nullable=False)

    batch: Mapped[MarketDataBatchRecord] = relationship(back_populates="candles")


# This is a storage envelope, not a claim about the broker's tick size or digits.
# Final instrument-specific precision remains a runtime/data-contract concern.
EVALUATION_NUMERIC_TYPE = Numeric(precision=38, scale=18, asdecimal=True)


class EvaluationRunRecord(DatabaseBase):
    """Immutable reproducibility definition for one evaluation run."""

    __tablename__ = "evaluation_runs"
    __table_args__ = (
        CheckConstraint("data_start_at < data_end_at", name="ck_eval_run_positive_range"),
        CheckConstraint("length(configuration_sha256) = 64", name="ck_eval_run_sha256_length"),
        CheckConstraint("minimum_completed_outcomes_per_horizon > 0", name="ck_eval_run_min_total_positive"),
        CheckConstraint("minimum_outcomes_per_period > 0", name="ck_eval_run_min_period_positive"),
        Index("ix_evaluation_runs_instrument_created", "instrument", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    contract_version: Mapped[str] = mapped_column(String(16), nullable=False)
    instrument: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    data_start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    data_end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    code_revision: Mapped[str] = mapped_column(String(128), nullable=False)
    configuration_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    configuration_snapshot: Mapped[dict] = mapped_column(JSON, nullable=False)
    sampling_rule: Mapped[str] = mapped_column(String(64), nullable=False)
    period_partition: Mapped[str] = mapped_column(String(64), nullable=False)
    horizons: Mapped[list[int]] = mapped_column(JSON, nullable=False)
    minimum_completed_outcomes_per_horizon: Mapped[int] = mapped_column(Integer, nullable=False)
    minimum_outcomes_per_period: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)

    periods: Mapped[list[EvaluationPeriodRecord]] = relationship(
        back_populates="run", cascade="all, delete-orphan", passive_deletes=True
    )
    signals: Mapped[list[EvaluationSignalRecord]] = relationship(
        back_populates="run", cascade="all, delete-orphan", passive_deletes=True
    )


class EvaluationPeriodRecord(DatabaseBase):
    """One chronological partition belonging to an evaluation run."""

    __tablename__ = "evaluation_periods"
    __table_args__ = (
        UniqueConstraint("run_id", "period_index", name="uq_evaluation_period_run_index"),
        UniqueConstraint("id", "run_id", name="uq_evaluation_period_id_run"),
        CheckConstraint("period_index IN (1, 2, 3)", name="ck_evaluation_period_index"),
        CheckConstraint("start_at < end_at", name="ck_evaluation_period_positive_range"),
        Index("ix_evaluation_periods_run_range", "run_id", "start_at", "end_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("evaluation_runs.id", ondelete="CASCADE"), nullable=False)
    contract_version: Mapped[str] = mapped_column(String(16), nullable=False)
    period_index: Mapped[int] = mapped_column(Integer, nullable=False)
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    partition_method: Mapped[str] = mapped_column(String(64), nullable=False)

    run: Mapped[EvaluationRunRecord] = relationship(back_populates="periods")
    signals: Mapped[list[EvaluationSignalRecord]] = relationship(back_populates="period")


class EvaluationSignalRecord(DatabaseBase):
    """Reference signal and filter decision; blocked signals are retained."""

    __tablename__ = "evaluation_signals"
    __table_args__ = (
        ForeignKeyConstraint(
            ["period_id", "run_id"],
            ["evaluation_periods.id", "evaluation_periods.run_id"],
            ondelete="CASCADE",
            name="fk_evaluation_signal_period_run",
        ),
        UniqueConstraint("id", "run_id", "period_id", name="uq_evaluation_signal_identity_scope"),
        CheckConstraint("length(instrument) > 0", name="ck_evaluation_signal_instrument_nonempty"),
        CheckConstraint("filter_decision IN ('ALLOWED', 'BLOCKED', 'NOT_APPLICABLE')", name="ck_evaluation_signal_filter_decision"),
        Index("ix_evaluation_signals_run_time", "run_id", "signal_at"),
        Index("ix_evaluation_signals_period_time", "period_id", "signal_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("evaluation_runs.id", ondelete="CASCADE"), nullable=False)
    period_id: Mapped[str] = mapped_column(String(36), nullable=False)
    contract_version: Mapped[str] = mapped_column(String(16), nullable=False)
    instrument: Mapped[str] = mapped_column(String(64), nullable=False)
    signal_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    input_available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    direction: Mapped[str] = mapped_column(String(8), nullable=False)
    reference_eligible: Mapped[bool] = mapped_column(Boolean, nullable=False)
    filter_decision: Mapped[str] = mapped_column(String(24), nullable=False)
    filter_reason: Mapped[str | None] = mapped_column(String(512), nullable=True)
    source_candle_ids: Mapped[list[int]] = mapped_column(JSON, nullable=False)
    source_batch_ids: Mapped[list[int]] = mapped_column(JSON, nullable=False)
    input_snapshot: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    run: Mapped[EvaluationRunRecord] = relationship(back_populates="signals")
    period: Mapped[EvaluationPeriodRecord] = relationship(back_populates="signals")
    outcomes: Mapped[list[EvaluationOutcomeRecord]] = relationship(
        back_populates="signal", cascade="all, delete-orphan", passive_deletes=True
    )


class EvaluationOutcomeRecord(DatabaseBase):
    """Counterfactual result for one signal and one independent horizon."""

    __tablename__ = "evaluation_outcomes"
    __table_args__ = (
        ForeignKeyConstraint(
            ["signal_id", "run_id", "period_id"],
            ["evaluation_signals.id", "evaluation_signals.run_id", "evaluation_signals.period_id"],
            ondelete="CASCADE",
            name="fk_evaluation_outcome_signal_scope",
        ),
        UniqueConstraint("signal_id", "horizon_minutes", name="uq_evaluation_outcome_signal_horizon"),
        CheckConstraint("horizon_minutes IN (5, 15, 30, 60)", name="ck_evaluation_outcome_horizon"),
        CheckConstraint("status IN ('COMPLETED', 'NEEDS_MORE_DATA', 'INVALID')", name="ck_evaluation_outcome_status"),
        CheckConstraint("direction IN ('BUY', 'SELL')", name="ck_evaluation_outcome_direction"),
        CheckConstraint("entry_price IS NULL OR entry_price > 0", name="ck_evaluation_outcome_entry_positive"),
        CheckConstraint("exit_price IS NULL OR exit_price > 0", name="ck_evaluation_outcome_exit_positive"),
        CheckConstraint("spread_cost IS NULL OR spread_cost >= 0", name="ck_evaluation_outcome_spread_nonnegative"),
        CheckConstraint("slippage_cost IS NULL OR slippage_cost >= 0", name="ck_evaluation_outcome_slippage_nonnegative"),
        CheckConstraint("commission_cost IS NULL OR commission_cost >= 0", name="ck_evaluation_outcome_commission_nonnegative"),
        CheckConstraint(
            "(status = 'COMPLETED' AND entry_at IS NOT NULL AND exit_at IS NOT NULL AND entry_price IS NOT NULL AND exit_price IS NOT NULL AND gross_pnl IS NOT NULL AND exit_at > entry_at) OR "
            "(status <> 'COMPLETED' AND entry_at IS NULL AND exit_at IS NULL AND entry_price IS NULL AND exit_price IS NULL AND gross_pnl IS NULL AND spread_cost IS NULL AND slippage_cost IS NULL AND commission_cost IS NULL AND swap_adjustment IS NULL AND net_pnl IS NULL AND costs_complete = 0)",
            name="ck_evaluation_outcome_completion_fields",
        ),
        CheckConstraint(
            "costs_complete = 0 OR (status = 'COMPLETED' AND measurement_basis = 'BID_ASK_QUOTES' AND net_pnl IS NOT NULL)",
            name="ck_evaluation_outcome_complete_costs",
        ),
        Index("ix_evaluation_outcomes_run_horizon_status", "run_id", "horizon_minutes", "status"),
        Index("ix_evaluation_outcomes_period_horizon", "period_id", "horizon_minutes"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(36), nullable=False)
    signal_id: Mapped[str] = mapped_column(String(36), nullable=False)
    period_id: Mapped[str] = mapped_column(String(36), nullable=False)
    contract_version: Mapped[str] = mapped_column(String(16), nullable=False)
    horizon_minutes: Mapped[int] = mapped_column(Integer, nullable=False)
    outcome_type: Mapped[str] = mapped_column(String(24), nullable=False)
    direction: Mapped[str] = mapped_column(String(8), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False)
    measurement_basis: Mapped[str] = mapped_column(String(32), nullable=False)
    entry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    exit_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    entry_price: Mapped[Decimal | None] = mapped_column(EVALUATION_NUMERIC_TYPE, nullable=True)
    exit_price: Mapped[Decimal | None] = mapped_column(EVALUATION_NUMERIC_TYPE, nullable=True)
    entry_price_side: Mapped[str] = mapped_column(String(8), nullable=False)
    exit_price_side: Mapped[str] = mapped_column(String(8), nullable=False)
    gross_pnl: Mapped[Decimal | None] = mapped_column(EVALUATION_NUMERIC_TYPE, nullable=True)
    spread_accounting: Mapped[str] = mapped_column(String(40), nullable=False)
    spread_cost: Mapped[Decimal | None] = mapped_column(EVALUATION_NUMERIC_TYPE, nullable=True)
    slippage_cost: Mapped[Decimal | None] = mapped_column(EVALUATION_NUMERIC_TYPE, nullable=True)
    slippage_knowledge: Mapped[str] = mapped_column(String(24), nullable=False)
    commission_cost: Mapped[Decimal | None] = mapped_column(EVALUATION_NUMERIC_TYPE, nullable=True)
    commission_knowledge: Mapped[str] = mapped_column(String(24), nullable=False)
    swap_adjustment: Mapped[Decimal | None] = mapped_column(EVALUATION_NUMERIC_TYPE, nullable=True)
    swap_knowledge: Mapped[str] = mapped_column(String(24), nullable=False)
    costs_complete: Mapped[bool] = mapped_column(Boolean, nullable=False)
    net_pnl: Mapped[Decimal | None] = mapped_column(EVALUATION_NUMERIC_TYPE, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    signal: Mapped[EvaluationSignalRecord] = relationship(back_populates="outcomes")
