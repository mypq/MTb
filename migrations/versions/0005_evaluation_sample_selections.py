"""Persist per-horizon evaluation sample selection decisions.

Revision ID: 0005_evaluation_sample_selections
Revises: 0004_evaluation_foundation
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0005_evaluation_sample_selections"
down_revision: Union[str, None] = "0004_evaluation_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "evaluation_sample_selections",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("signal_id", sa.String(length=36), nullable=False),
        sa.Column("period_id", sa.String(length=36), nullable=False),
        sa.Column("horizon_minutes", sa.Integer(), nullable=False),
        sa.Column("candidate_order", sa.Integer(), nullable=False),
        sa.Column("selected", sa.Boolean(), nullable=False),
        sa.Column("decision_reason", sa.String(length=32), nullable=False),
        sa.CheckConstraint(
            "horizon_minutes IN (5, 15, 30, 60)",
            name="ck_eval_sample_selection_horizon",
        ),
        sa.CheckConstraint(
            "candidate_order > 0",
            name="ck_eval_sample_selection_order_positive",
        ),
        sa.CheckConstraint(
            "decision_reason IN ('SELECTED', 'INELIGIBLE_REFERENCE', 'OVERLAPPING_HORIZON')",
            name="ck_eval_sample_selection_reason",
        ),
        sa.CheckConstraint(
            "(selected = true AND decision_reason = 'SELECTED') OR "
            "(selected = false AND decision_reason IN ('INELIGIBLE_REFERENCE', 'OVERLAPPING_HORIZON'))",
            name="ck_eval_sample_selection_decision_consistency",
        ),
        sa.ForeignKeyConstraint(
            ["signal_id", "run_id", "period_id"],
            [
                "evaluation_signals.id",
                "evaluation_signals.run_id",
                "evaluation_signals.period_id",
            ],
            ondelete="CASCADE",
            name="fk_evaluation_sample_selection_signal_scope",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id",
            "horizon_minutes",
            "signal_id",
            name="uq_eval_sample_selection_run_horizon_signal",
        ),
        sa.UniqueConstraint(
            "run_id",
            "horizon_minutes",
            "candidate_order",
            name="uq_eval_sample_selection_candidate_order",
        ),
    )
    op.create_index(
        "ix_eval_sample_selection_run_horizon_selected_order",
        "evaluation_sample_selections",
        ["run_id", "horizon_minutes", "selected", "candidate_order"],
    )
    op.create_index(
        "ix_eval_sample_selection_period_horizon",
        "evaluation_sample_selections",
        ["period_id", "horizon_minutes"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_eval_sample_selection_period_horizon",
        table_name="evaluation_sample_selections",
    )
    op.drop_index(
        "ix_eval_sample_selection_run_horizon_selected_order",
        table_name="evaluation_sample_selections",
    )
    op.drop_table("evaluation_sample_selections")
