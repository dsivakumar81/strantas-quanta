"""tenant isolation for operations tables"""

from alembic import op
import sqlalchemy as sa

revision = "20260313_000005"
down_revision = "20260312_000004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("connector_cursors", sa.Column("tenant_id", sa.String(length=128), nullable=False, server_default="default"))
    op.drop_constraint("connector_cursors_pkey", "connector_cursors", type_="primary")
    op.create_primary_key("connector_cursors_pkey", "connector_cursors", ["provider", "tenant_id"])
    op.create_index("ix_connector_cursors_tenant_provider", "connector_cursors", ["tenant_id", "provider"], unique=True)

    op.add_column("jobs", sa.Column("tenant_id", sa.String(length=128), nullable=False, server_default="default"))
    op.create_index("ix_jobs_tenant_status", "jobs", ["tenant_id", "status"], unique=False)

    op.add_column("alerts", sa.Column("tenant_id", sa.String(length=128), nullable=False, server_default="default"))
    op.create_index("ix_alerts_tenant_created_at", "alerts", ["tenant_id", "created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_alerts_tenant_created_at", table_name="alerts")
    op.drop_column("alerts", "tenant_id")

    op.drop_index("ix_jobs_tenant_status", table_name="jobs")
    op.drop_column("jobs", "tenant_id")

    op.drop_index("ix_connector_cursors_tenant_provider", table_name="connector_cursors")
    op.drop_constraint("connector_cursors_pkey", "connector_cursors", type_="primary")
    op.create_primary_key("connector_cursors_pkey", "connector_cursors", ["provider"])
    op.drop_column("connector_cursors", "tenant_id")
