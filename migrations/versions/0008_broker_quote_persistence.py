"""Persist observed MT5 broker quotes.

Revision ID: 0008_broker_quote_persistence
Revises: 0007_evaluation_evidence_integrity
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0008_broker_quote_persistence"
down_revision: Union[str, None] = "0007_evaluation_evidence_integrity"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "broker_quotes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("symbol", sa.String(length=64), nullable=False),
        sa.Column("broker_time_msc", sa.BigInteger(), nullable=False),
        sa.Column("received_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_monotonic_ns", sa.BigInteger(), nullable=False),
        sa.Column("bid", sa.Float(), nullable=False),
        sa.Column("ask", sa.Float(), nullable=False),
        sa.Column("account_login", sa.BigInteger(), nullable=False),
        sa.Column("broker_server", sa.String(length=128), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.CheckConstraint("broker_time_msc > 0", name="ck_broker_quote_time_positive"),
        sa.CheckConstraint("received_monotonic_ns >= 0", name="ck_broker_quote_monotonic_nonnegative"),
        sa.CheckConstraint("bid > 0", name="ck_broker_quote_bid_positive"),
        sa.CheckConstraint("ask >= bid", name="ck_broker_quote_ask_gte_bid"),
        sa.CheckConstraint("account_login > 0", name="ck_broker_quote_login_positive"),
        sa.UniqueConstraint(
            "symbol", "account_login", "broker_server",
            "broker_time_msc", "bid", "ask",
            name="uq_broker_quotes_observation",
        ),
    )
    op.create_index(
        "ix_broker_quotes_symbol_received_at",
        "broker_quotes", ["symbol", "received_at_utc"],
    )
    op.create_index(
        "ix_broker_quotes_identity_broker_time",
        "broker_quotes",
        ["symbol", "account_login", "broker_server", "broker_time_msc"],
    )


def downgrade() -> None:
    op.drop_index("ix_broker_quotes_identity_broker_time", table_name="broker_quotes")
    op.drop_index("ix_broker_quotes_symbol_received_at", table_name="broker_quotes")
    op.drop_table("broker_quotes")
