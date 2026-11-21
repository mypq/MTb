"""Persist the accepted market-data validation report.

Revision ID: 0003_validation_provenance
Revises: 0002_clock_evidence
Create Date: 2026-11-21
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0003_validation_provenance"
down_revision: Union[str, None] = "0002_clock_evidence"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "market_data_batches",
        sa.Column("validation_report", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("market_data_batches", "validation_report")
