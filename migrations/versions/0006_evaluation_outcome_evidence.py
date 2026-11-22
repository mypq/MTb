"""Persist immutable M1 source evidence for every calculated evaluation outcome.

Revision ID: 0006_evaluation_outcome_evidence
Revises: 0005_evaluation_sample_selections
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0006_evaluation_outcome_evidence"
down_revision: Union[str, None] = "0005_evaluation_sample_selections"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "evaluation_outcome_evidence",
        sa.Column("outcome_id", sa.String(length=36), nullable=False),
        sa.Column("instrument", sa.String(length=64), nullable=False),
        sa.Column("timeframe", sa.String(length=8), nullable=False),
        sa.Column("requested_start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("requested_end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("available_at_cutoff", sa.DateTime(timezone=True), nullable=False),
        sa.Column("latest_available_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_candles", sa.JSON(), nullable=False),
        sa.Column("observed_gaps", sa.JSON(), nullable=False),
        sa.Column("duplicate_event_times", sa.JSON(), nullable=False),
        sa.Column("is_contiguous", sa.Boolean(), nullable=False),
        sa.Column("evidence_schema_version", sa.Integer(), nullable=False, server_default="1"),
        sa.CheckConstraint(
            "length(instrument) > 0",
            name="ck_eval_outcome_evidence_instrument",
        ),
        sa.CheckConstraint(
            "timeframe = 'M1'",
            name="ck_eval_outcome_evidence_timeframe",
        ),
        sa.CheckConstraint(
            "requested_start_at < requested_end_at",
            name="ck_eval_outcome_evidence_range",
        ),
        sa.ForeignKeyConstraint(
            ["outcome_id"],
            ["evaluation_outcomes.id"],
            ondelete="CASCADE",
            name="fk_eval_outcome_evidence_outcome",
        ),
        sa.PrimaryKeyConstraint("outcome_id"),
    )
    op.create_index(
        "ix_eval_outcome_evidence_cutoff",
        "evaluation_outcome_evidence",
        ["available_at_cutoff"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_eval_outcome_evidence_cutoff",
        table_name="evaluation_outcome_evidence",
    )
    op.drop_table("evaluation_outcome_evidence")
