"""Add tamper-evident digests and immutable updates for outcome evidence.

Revision ID: 0007_evaluation_evidence_integrity
Revises: 0006_evaluation_outcome_evidence
"""
from datetime import datetime, timezone
import hashlib
import json
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0007_evaluation_evidence_integrity"
down_revision: Union[str, None] = "0006_evaluation_outcome_evidence"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_PAYLOAD_COLUMNS = (
    "instrument",
    "timeframe",
    "requested_start_at",
    "requested_end_at",
    "available_at_cutoff",
    "latest_available_at",
    "source_candles",
    "observed_gaps",
    "duplicate_event_times",
    "is_contiguous",
    "evidence_schema_version",
)


def _canonical(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _canonical(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    return value


def _digest(payload):
    encoded = json.dumps(
        _canonical(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def upgrade() -> None:
    op.add_column(
        "evaluation_outcome_evidence",
        sa.Column("evidence_sha256", sa.String(length=64), nullable=True),
    )
    table = sa.table(
        "evaluation_outcome_evidence",
        sa.column("outcome_id", sa.String()),
        *[
            sa.column(name, sa.JSON() if name in {
                "source_candles", "observed_gaps", "duplicate_event_times"
            } else sa.String())
            for name in _PAYLOAD_COLUMNS
        ],
        sa.column("evidence_sha256", sa.String()),
    )
    connection = op.get_bind()
    rows = connection.execute(sa.select(table)).mappings().all()
    for row in rows:
        payload = {name: row[name] for name in _PAYLOAD_COLUMNS}
        connection.execute(
            table.update()
            .where(table.c.outcome_id == row["outcome_id"])
            .values(evidence_sha256=_digest(payload))
        )
    op.alter_column(
        "evaluation_outcome_evidence",
        "evidence_sha256",
        existing_type=sa.String(length=64),
        nullable=False,
    )

    dialect = connection.dialect.name
    if dialect == "sqlite":
        op.execute("""
            CREATE TRIGGER trg_eval_outcome_evidence_immutable_update
            BEFORE UPDATE ON evaluation_outcome_evidence
            BEGIN
                SELECT RAISE(ABORT, 'evaluation outcome evidence is immutable');
            END
        """)
    elif dialect == "postgresql":
        op.execute("""
            CREATE FUNCTION reject_evaluation_outcome_evidence_update()
            RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'evaluation outcome evidence is immutable';
            END;
            $$ LANGUAGE plpgsql
        """)
        op.execute("""
            CREATE TRIGGER trg_eval_outcome_evidence_immutable_update
            BEFORE UPDATE ON evaluation_outcome_evidence
            FOR EACH ROW EXECUTE FUNCTION reject_evaluation_outcome_evidence_update()
        """)


def downgrade() -> None:
    dialect = op.get_bind().dialect.name
    if dialect == "sqlite":
        op.execute("DROP TRIGGER IF EXISTS trg_eval_outcome_evidence_immutable_update")
    elif dialect == "postgresql":
        op.execute(
            "DROP TRIGGER IF EXISTS trg_eval_outcome_evidence_immutable_update "
            "ON evaluation_outcome_evidence"
        )
        op.execute("DROP FUNCTION IF EXISTS reject_evaluation_outcome_evidence_update()")
    op.drop_column("evaluation_outcome_evidence", "evidence_sha256")
