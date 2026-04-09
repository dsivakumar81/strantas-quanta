"""Add replay audits table

Revision ID: 20260321_000007
Revises: 20260321_000006
Create Date: 2026-03-21
"""

from alembic import op
import sqlalchemy as sa


revision = "20260321_000007"
down_revision = "20260321_000006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "replay_audits",
        sa.Column("audit_id", sa.String(length=32), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index("ix_replay_audits_tenant_created_at", "replay_audits", ["tenant_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_replay_audits_tenant_created_at", table_name="replay_audits")
    op.drop_table("replay_audits")
