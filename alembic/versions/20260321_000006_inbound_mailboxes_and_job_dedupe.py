"""inbound mailboxes and job dedupe"""

from alembic import op
import sqlalchemy as sa

revision = "20260321_000006"
down_revision = "20260313_000005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "inbound_mailboxes",
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("tenant_id", sa.String(length=128), nullable=False, server_default="default"),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("provider", "tenant_id"),
    )
    op.create_index("ix_inbound_mailboxes_tenant_provider", "inbound_mailboxes", ["tenant_id", "provider"], unique=True)

    op.add_column("jobs", sa.Column("dedupe_key", sa.String(length=255), nullable=True))
    op.create_index("ix_jobs_tenant_dedupe", "jobs", ["tenant_id", "dedupe_key"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_jobs_tenant_dedupe", table_name="jobs")
    op.drop_column("jobs", "dedupe_key")

    op.drop_index("ix_inbound_mailboxes_tenant_provider", table_name="inbound_mailboxes")
    op.drop_table("inbound_mailboxes")
