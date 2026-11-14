from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
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
        CheckConstraint(
            "candle_count > 0",
            name="ck_batch_candle_count_positive",
        ),
        Index(
            "ix_market_data_batches_symbol_timeframe",
            "symbol",
            "timeframe",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(8), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    candle_count: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    candles: Mapped[list[MarketCandleRecord]] = relationship(
        back_populates="batch",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class MarketCandleRecord(DatabaseBase):
    """Persisted OHLC candle with source and quality metadata."""

    __tablename__ = "market_candles"
    __table_args__ = (
        UniqueConstraint(
            "symbol",
            "timeframe",
            "timestamp",
            name="uq_market_candles_identity",
        ),
        CheckConstraint("open_price > 0", name="ck_candle_open_positive"),
        CheckConstraint("high_price > 0", name="ck_candle_high_positive"),
        CheckConstraint("low_price > 0", name="ck_candle_low_positive"),
        CheckConstraint("close_price > 0", name="ck_candle_close_positive"),
        CheckConstraint(
            "high_price >= low_price",
            name="ck_candle_high_gte_low",
        ),
        CheckConstraint(
            "high_price >= open_price AND high_price >= close_price",
            name="ck_candle_high_covers_prices",
        ),
        CheckConstraint(
            "low_price <= open_price AND low_price <= close_price",
            name="ck_candle_low_covers_prices",
        ),
        CheckConstraint(
            "tick_volume >= 0",
            name="ck_candle_tick_volume_nonnegative",
        ),
        CheckConstraint(
            "real_volume IS NULL OR real_volume >= 0",
            name="ck_candle_real_volume_nonnegative",
        ),
        Index(
            "ix_market_candles_symbol_timeframe_timestamp",
            "symbol",
            "timeframe",
            "timestamp",
        ),
        Index("ix_market_candles_batch_id", "batch_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    batch_id: Mapped[int] = mapped_column(
        ForeignKey("market_data_batches.id", ondelete="CASCADE"),
        nullable=False,
    )

    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(8), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    open_price: Mapped[float] = mapped_column(Float, nullable=False)
    high_price: Mapped[float] = mapped_column(Float, nullable=False)
    low_price: Mapped[float] = mapped_column(Float, nullable=False)
    close_price: Mapped[float] = mapped_column(Float, nullable=False)

    tick_volume: Mapped[int] = mapped_column(Integer, nullable=False)
    real_volume: Mapped[int | None] = mapped_column(Integer, nullable=True)
    volume_source: Mapped[str] = mapped_column(String(16), nullable=False)

    is_closed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    quality: Mapped[str] = mapped_column(String(16), nullable=False)

    batch: Mapped[MarketDataBatchRecord] = relationship(
        back_populates="candles",
    )
