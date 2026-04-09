"""idempotency keys"""

from alembic import op
import sqlalchemy as sa

revision = "20260312_000003"
down_revision = "20260312_000002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "idempotency_keys",
        sa.Column("tenant_id", sa.String(length=128), primary_key=True),
        sa.Column("fingerprint", sa.String(length=128), primary_key=True),
        sa.Column("submission_id", sa.String(length=32), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("idempotency_keys")
