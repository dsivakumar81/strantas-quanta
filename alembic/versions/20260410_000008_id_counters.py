"""Add shared ID counters table

Revision ID: 20260410_000008
Revises: 20260321_000007
Create Date: 2026-04-10
"""

from alembic import op
import sqlalchemy as sa


revision = "20260410_000008"
down_revision = "20260321_000007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "id_counters",
        sa.Column("prefix", sa.String(length=16), primary_key=True),
        sa.Column("year", sa.Integer(), primary_key=True),
        sa.Column("counter", sa.Integer(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("id_counters")
