"""Create initial market data tables.

Revision ID: 0001_market_data
Revises:
Create Date: 2026-11-14
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0001_market_data"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "market_data_batches",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("symbol", sa.String(length=64), nullable=False),
        sa.Column("timeframe", sa.String(length=8), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("candle_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "candle_count > 0",
            name="ck_batch_candle_count_positive",
        ),
    )

    op.create_index(
        "ix_market_data_batches_symbol_timeframe",
        "market_data_batches",
        ["symbol", "timeframe"],
        unique=False,
    )

    op.create_table(
        "market_candles",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("batch_id", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.String(length=64), nullable=False),
        sa.Column("timeframe", sa.String(length=8), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open_price", sa.Float(), nullable=False),
        sa.Column("high_price", sa.Float(), nullable=False),
        sa.Column("low_price", sa.Float(), nullable=False),
        sa.Column("close_price", sa.Float(), nullable=False),
        sa.Column("tick_volume", sa.Integer(), nullable=False),
        sa.Column("real_volume", sa.Integer(), nullable=True),
        sa.Column("volume_source", sa.String(length=16), nullable=False),
        sa.Column("is_closed", sa.Boolean(), nullable=False),
        sa.Column("quality", sa.String(length=16), nullable=False),
        sa.ForeignKeyConstraint(
            ["batch_id"],
            ["market_data_batches.id"],
            name="fk_market_candles_batch_id_market_data_batches",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "symbol",
            "timeframe",
            "timestamp",
            name="uq_market_candles_identity",
        ),
        sa.CheckConstraint(
            "open_price > 0",
            name="ck_candle_open_positive",
        ),
        sa.CheckConstraint(
            "high_price > 0",
            name="ck_candle_high_positive",
        ),
        sa.CheckConstraint(
            "low_price > 0",
            name="ck_candle_low_positive",
        ),
        sa.CheckConstraint(
            "close_price > 0",
            name="ck_candle_close_positive",
        ),
        sa.CheckConstraint(
            "high_price >= low_price",
            name="ck_candle_high_gte_low",
        ),
        sa.CheckConstraint(
            "high_price >= open_price AND high_price >= close_price",
            name="ck_candle_high_covers_prices",
        ),
        sa.CheckConstraint(
            "low_price <= open_price AND low_price <= close_price",
            name="ck_candle_low_covers_prices",
        ),
        sa.CheckConstraint(
            "tick_volume >= 0",
            name="ck_candle_tick_volume_nonnegative",
        ),
        sa.CheckConstraint(
            "real_volume IS NULL OR real_volume >= 0",
            name="ck_candle_real_volume_nonnegative",
        ),
    )

    op.create_index(
        "ix_market_candles_symbol_timeframe_timestamp",
        "market_candles",
        ["symbol", "timeframe", "timestamp"],
        unique=False,
    )

    op.create_index(
        "ix_market_candles_batch_id",
        "market_candles",
        ["batch_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_market_candles_batch_id",
        table_name="market_candles",
    )

    op.drop_index(
        "ix_market_candles_symbol_timeframe_timestamp",
        table_name="market_candles",
    )

    op.drop_table("market_candles")

    op.drop_index(
        "ix_market_data_batches_symbol_timeframe",
        table_name="market_data_batches",
    )

    op.drop_table("market_data_batches")
