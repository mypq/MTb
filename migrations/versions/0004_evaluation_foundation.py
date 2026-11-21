"""Create the Evaluation Foundation tables.

Revision ID: 0004_evaluation_foundation
Revises: 0003_validation_provenance
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0004_evaluation_foundation"
down_revision: Union[str, None] = "0003_validation_provenance"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _decimal_type():
    # SQLite stores these values as TEXT to preserve Decimal precision exactly.
    # The application TypeDecorator supplies the same behavior at ORM runtime.
    return sa.String(length=80)


def upgrade() -> None:
    op.create_table(
        "evaluation_runs",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("contract_version", sa.String(16), nullable=False),
        sa.Column("instrument", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("data_start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("data_end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("code_revision", sa.String(128), nullable=False),
        sa.Column("configuration_sha256", sa.String(64), nullable=False),
        sa.Column("configuration_snapshot", sa.JSON(), nullable=False),
        sa.Column("sampling_rule", sa.String(64), nullable=False),
        sa.Column("period_partition", sa.String(64), nullable=False),
        sa.Column("horizons", sa.JSON(), nullable=False),
        sa.Column("minimum_completed_outcomes_per_horizon", sa.Integer(), nullable=False),
        sa.Column("minimum_outcomes_per_period", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.CheckConstraint("data_start_at < data_end_at", name="ck_eval_run_positive_range"),
        sa.CheckConstraint("length(configuration_sha256) = 64", name="ck_eval_run_sha256_length"),
        sa.CheckConstraint("minimum_completed_outcomes_per_horizon > 0", name="ck_eval_run_min_total_positive"),
        sa.CheckConstraint("minimum_outcomes_per_period > 0", name="ck_eval_run_min_period_positive"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_evaluation_runs_instrument_created", "evaluation_runs", ["instrument", "created_at"])

    op.create_table(
        "evaluation_periods",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("run_id", sa.String(36), nullable=False),
        sa.Column("contract_version", sa.String(16), nullable=False),
        sa.Column("period_index", sa.Integer(), nullable=False),
        sa.Column("start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("partition_method", sa.String(64), nullable=False),
        sa.CheckConstraint("period_index IN (1, 2, 3)", name="ck_evaluation_period_index"),
        sa.CheckConstraint("start_at < end_at", name="ck_evaluation_period_positive_range"),
        sa.ForeignKeyConstraint(["run_id"], ["evaluation_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "period_index", name="uq_evaluation_period_run_index"),
        sa.UniqueConstraint("id", "run_id", name="uq_evaluation_period_id_run"),
    )
    op.create_index("ix_evaluation_periods_run_range", "evaluation_periods", ["run_id", "start_at", "end_at"])

    op.create_table(
        "evaluation_signals",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("run_id", sa.String(36), nullable=False),
        sa.Column("period_id", sa.String(36), nullable=False),
        sa.Column("contract_version", sa.String(16), nullable=False),
        sa.Column("instrument", sa.String(64), nullable=False),
        sa.Column("signal_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("input_available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("direction", sa.String(8), nullable=False),
        sa.Column("reference_eligible", sa.Boolean(), nullable=False),
        sa.Column("filter_decision", sa.String(24), nullable=False),
        sa.Column("filter_reason", sa.String(512), nullable=True),
        sa.Column("source_candle_ids", sa.JSON(), nullable=False),
        sa.Column("source_batch_ids", sa.JSON(), nullable=False),
        sa.Column("input_snapshot", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("length(instrument) > 0", name="ck_evaluation_signal_instrument_nonempty"),
        sa.CheckConstraint("direction IN ('BUY', 'SELL')", name="ck_evaluation_signal_direction"),
        sa.CheckConstraint("input_available_at <= signal_at", name="ck_evaluation_signal_no_lookahead"),
        sa.CheckConstraint("filter_decision IN ('ALLOWED', 'BLOCKED', 'NOT_APPLICABLE')", name="ck_evaluation_signal_filter_decision"),
        sa.CheckConstraint("filter_decision <> 'BLOCKED' OR (filter_reason IS NOT NULL AND length(trim(filter_reason)) > 0)", name="ck_evaluation_signal_block_reason"),
        sa.ForeignKeyConstraint(["run_id"], ["evaluation_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["period_id", "run_id"],
            ["evaluation_periods.id", "evaluation_periods.run_id"],
            ondelete="CASCADE",
            name="fk_evaluation_signal_period_run",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id", "run_id", "period_id", name="uq_evaluation_signal_identity_scope"),
    )
    op.create_index("ix_evaluation_signals_run_time", "evaluation_signals", ["run_id", "signal_at"])
    op.create_index("ix_evaluation_signals_period_time", "evaluation_signals", ["period_id", "signal_at"])

    op.create_table(
        "evaluation_outcomes",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("run_id", sa.String(36), nullable=False),
        sa.Column("signal_id", sa.String(36), nullable=False),
        sa.Column("period_id", sa.String(36), nullable=False),
        sa.Column("contract_version", sa.String(16), nullable=False),
        sa.Column("horizon_minutes", sa.Integer(), nullable=False),
        sa.Column("outcome_type", sa.String(24), nullable=False),
        sa.Column("direction", sa.String(8), nullable=False),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("measurement_basis", sa.String(32), nullable=False),
        sa.Column("entry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("exit_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("entry_price", _decimal_type(), nullable=True),
        sa.Column("exit_price", _decimal_type(), nullable=True),
        sa.Column("entry_price_side", sa.String(8), nullable=False),
        sa.Column("exit_price_side", sa.String(8), nullable=False),
        sa.Column("gross_pnl", _decimal_type(), nullable=True),
        sa.Column("spread_accounting", sa.String(40), nullable=False),
        sa.Column("spread_cost", _decimal_type(), nullable=True),
        sa.Column("slippage_cost", _decimal_type(), nullable=True),
        sa.Column("slippage_knowledge", sa.String(24), nullable=False),
        sa.Column("commission_cost", _decimal_type(), nullable=True),
        sa.Column("commission_knowledge", sa.String(24), nullable=False),
        sa.Column("swap_adjustment", _decimal_type(), nullable=True),
        sa.Column("swap_knowledge", sa.String(24), nullable=False),
        sa.Column("costs_complete", sa.Boolean(), nullable=False),
        sa.Column("net_pnl", _decimal_type(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("horizon_minutes IN (5, 15, 30, 60)", name="ck_evaluation_outcome_horizon"),
        sa.CheckConstraint("outcome_type = 'COUNTERFACTUAL'", name="ck_evaluation_outcome_type"),
        sa.CheckConstraint("status IN ('COMPLETED', 'NEEDS_MORE_DATA', 'INVALID')", name="ck_evaluation_outcome_status"),
        sa.CheckConstraint("direction IN ('BUY', 'SELL')", name="ck_evaluation_outcome_direction"),
        sa.CheckConstraint("measurement_basis IN ('BID_ASK_QUOTES', 'OHLC_APPROXIMATION', 'UNAVAILABLE')", name="ck_evaluation_outcome_measurement_basis"),
        sa.CheckConstraint("entry_price_side IN ('BID', 'ASK', 'UNKNOWN')", name="ck_evaluation_outcome_entry_side"),
        sa.CheckConstraint("exit_price_side IN ('BID', 'ASK', 'UNKNOWN')", name="ck_evaluation_outcome_exit_side"),
        sa.CheckConstraint("spread_accounting IN ('EMBEDDED_IN_BID_ASK_PRICES', 'SEPARATE_COST', 'UNKNOWN')", name="ck_evaluation_outcome_spread_accounting"),
        sa.CheckConstraint("slippage_knowledge IN ('MEASURED', 'NOT_APPLICABLE', 'UNKNOWN')", name="ck_evaluation_outcome_slippage_knowledge"),
        sa.CheckConstraint("commission_knowledge IN ('MEASURED', 'NOT_APPLICABLE', 'UNKNOWN')", name="ck_evaluation_outcome_commission_knowledge"),
        sa.CheckConstraint("swap_knowledge IN ('MEASURED', 'NOT_APPLICABLE', 'UNKNOWN')", name="ck_evaluation_outcome_swap_knowledge"),
        sa.CheckConstraint("entry_price IS NULL OR CAST(entry_price AS NUMERIC) > 0", name="ck_evaluation_outcome_entry_positive"),
        sa.CheckConstraint("exit_price IS NULL OR CAST(exit_price AS NUMERIC) > 0", name="ck_evaluation_outcome_exit_positive"),
        sa.CheckConstraint("spread_cost IS NULL OR CAST(spread_cost AS NUMERIC) >= 0", name="ck_evaluation_outcome_spread_nonnegative"),
        sa.CheckConstraint("slippage_cost IS NULL OR CAST(slippage_cost AS NUMERIC) >= 0", name="ck_evaluation_outcome_slippage_nonnegative"),
        sa.CheckConstraint("commission_cost IS NULL OR CAST(commission_cost AS NUMERIC) >= 0", name="ck_evaluation_outcome_commission_nonnegative"),
        sa.CheckConstraint("(slippage_knowledge = 'MEASURED' AND slippage_cost IS NOT NULL) OR (slippage_knowledge <> 'MEASURED' AND slippage_cost IS NULL)", name="ck_evaluation_outcome_slippage_knowledge_amount"),
        sa.CheckConstraint("(commission_knowledge = 'MEASURED' AND commission_cost IS NOT NULL) OR (commission_knowledge <> 'MEASURED' AND commission_cost IS NULL)", name="ck_evaluation_outcome_commission_knowledge_amount"),
        sa.CheckConstraint("(swap_knowledge = 'MEASURED' AND swap_adjustment IS NOT NULL) OR (swap_knowledge <> 'MEASURED' AND swap_adjustment IS NULL)", name="ck_evaluation_outcome_swap_knowledge_amount"),
        sa.CheckConstraint(
            "(measurement_basis = 'BID_ASK_QUOTES' AND ((direction = 'BUY' AND entry_price_side = 'ASK' AND exit_price_side = 'BID') OR (direction = 'SELL' AND entry_price_side = 'BID' AND exit_price_side = 'ASK')) AND spread_accounting = 'EMBEDDED_IN_BID_ASK_PRICES' AND spread_cost IS NULL) OR "
            "(measurement_basis = 'OHLC_APPROXIMATION' AND entry_price_side = 'UNKNOWN' AND exit_price_side = 'UNKNOWN' AND spread_accounting = 'UNKNOWN' AND spread_cost IS NULL) OR measurement_basis = 'UNAVAILABLE'",
            name="ck_evaluation_outcome_measurement_consistency",
        ),
        sa.CheckConstraint(
            "(status = 'COMPLETED' AND measurement_basis <> 'UNAVAILABLE' AND entry_at IS NOT NULL AND exit_at IS NOT NULL AND entry_price IS NOT NULL AND exit_price IS NOT NULL AND gross_pnl IS NOT NULL AND exit_at > entry_at) OR "
            "(status <> 'COMPLETED' AND entry_at IS NULL AND exit_at IS NULL AND entry_price IS NULL AND exit_price IS NULL AND gross_pnl IS NULL AND spread_cost IS NULL AND slippage_cost IS NULL AND commission_cost IS NULL AND swap_adjustment IS NULL AND net_pnl IS NULL AND costs_complete = false)",
            name="ck_evaluation_outcome_completion_fields",
        ),
        sa.CheckConstraint(
            "costs_complete = false OR (status = 'COMPLETED' AND measurement_basis = 'BID_ASK_QUOTES' AND net_pnl IS NOT NULL AND slippage_knowledge <> 'UNKNOWN' AND commission_knowledge <> 'UNKNOWN' AND swap_knowledge <> 'UNKNOWN')",
            name="ck_evaluation_outcome_complete_costs",
        ),
        sa.CheckConstraint("costs_complete = true OR net_pnl IS NULL", name="ck_evaluation_outcome_net_requires_complete_costs"),
        sa.ForeignKeyConstraint(
            ["signal_id", "run_id", "period_id"],
            ["evaluation_signals.id", "evaluation_signals.run_id", "evaluation_signals.period_id"],
            ondelete="CASCADE",
            name="fk_evaluation_outcome_signal_scope",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("signal_id", "horizon_minutes", name="uq_evaluation_outcome_signal_horizon"),
    )
    op.create_index("ix_evaluation_outcomes_run_horizon_status", "evaluation_outcomes", ["run_id", "horizon_minutes", "status"])
    op.create_index("ix_evaluation_outcomes_period_horizon", "evaluation_outcomes", ["period_id", "horizon_minutes"])


def downgrade() -> None:
    op.drop_index("ix_evaluation_outcomes_period_horizon", table_name="evaluation_outcomes")
    op.drop_index("ix_evaluation_outcomes_run_horizon_status", table_name="evaluation_outcomes")
    op.drop_table("evaluation_outcomes")

    op.drop_index("ix_evaluation_signals_period_time", table_name="evaluation_signals")
    op.drop_index("ix_evaluation_signals_run_time", table_name="evaluation_signals")
    op.drop_table("evaluation_signals")

    op.drop_index("ix_evaluation_periods_run_range", table_name="evaluation_periods")
    op.drop_table("evaluation_periods")

    op.drop_index("ix_evaluation_runs_instrument_created", table_name="evaluation_runs")
    op.drop_table("evaluation_runs")
