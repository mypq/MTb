
"""Add historical clock evidence to market data batches.

Revision ID: 0002_clock_evidence
Revises: 0001_market_data
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0002_clock_evidence"
down_revision: Union[str, None] = "0001_market_data"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "market_data_batches",
        sa.Column("clock_verified", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "market_data_batches",
        sa.Column("clock_account_login", sa.Integer(), nullable=True),
    )
    op.add_column(
        "market_data_batches",
        sa.Column("clock_broker_server", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "market_data_batches",
        sa.Column("clock_terminal_build", sa.Integer(), nullable=True),
    )
    op.add_column(
        "market_data_batches",
        sa.Column("clock_checked_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "market_data_batches",
        sa.Column("clock_ntp_offset_seconds", sa.Float(), nullable=True),
    )
    op.add_column(
        "market_data_batches",
        sa.Column("clock_ntp_uncertainty_seconds", sa.Float(), nullable=True),
    )
    op.add_column(
        "market_data_batches",
        sa.Column("clock_ntp_sample_count", sa.Integer(), nullable=True),
    )
    op.add_column(
        "market_data_batches",
        sa.Column("clock_ntp_servers", sa.JSON(), nullable=True),
    )
    op.add_column(
        "market_data_batches",
        sa.Column("clock_candle_offset_seconds", sa.Float(), nullable=True),
    )
    op.add_column(
        "market_data_batches",
        sa.Column("clock_candle_uncertainty_seconds", sa.Float(), nullable=True),
    )
    op.add_column(
        "market_data_batches",
        sa.Column("clock_calibration_sample_count", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column(
        "market_data_batches",
        "clock_calibration_sample_count",
    )
    op.drop_column(
        "market_data_batches",
        "clock_candle_uncertainty_seconds",
    )
    op.drop_column(
        "market_data_batches",
        "clock_candle_offset_seconds",
    )
    op.drop_column(
        "market_data_batches",
        "clock_ntp_servers",
    )
    op.drop_column(
        "market_data_batches",
        "clock_ntp_sample_count",
    )
    op.drop_column(
        "market_data_batches",
        "clock_ntp_uncertainty_seconds",
    )
    op.drop_column(
        "market_data_batches",
        "clock_ntp_offset_seconds",
    )
    op.drop_column(
        "market_data_batches",
        "clock_checked_at",
    )
    op.drop_column(
        "market_data_batches",
        "clock_terminal_build",
    )
    op.drop_column(
        "market_data_batches",
        "clock_broker_server",
    )
    op.drop_column(
        "market_data_batches",
        "clock_account_login",
    )
    op.drop_column(
        "market_data_batches",
        "clock_verified",
    )
