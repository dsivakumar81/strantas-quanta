"""operations tables"""

from alembic import op
import sqlalchemy as sa

revision = "20260312_000002"
down_revision = "20260312_000001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "connector_cursors",
        sa.Column("provider", sa.String(length=64), primary_key=True),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_table(
        "jobs",
        sa.Column("job_id", sa.String(length=32), primary_key=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index("ix_jobs_status", "jobs", ["status"])
    op.create_table(
        "alerts",
        sa.Column("alert_id", sa.String(length=32), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index("ix_alerts_created_at", "alerts", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_alerts_created_at", table_name="alerts")
    op.drop_table("alerts")
    op.drop_index("ix_jobs_status", table_name="jobs")
    op.drop_table("jobs")
    op.drop_table("connector_cursors")
