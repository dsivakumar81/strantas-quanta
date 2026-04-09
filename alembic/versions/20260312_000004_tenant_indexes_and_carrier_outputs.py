"""tenant indexes and carrier outputs"""

from alembic import op
import sqlalchemy as sa

revision = "20260312_000004"
down_revision = "20260312_000003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table in ["submissions", "quotes", "lobs", "census", "outputs"]:
        op.add_column(table, sa.Column("tenant_id", sa.String(length=128), nullable=False, server_default="default"))
    op.create_index("ix_submissions_tenant_submission", "submissions", ["tenant_id", "submission_id"], unique=True)
    op.create_index("ix_submissions_tenant_case", "submissions", ["tenant_id", "case_id"], unique=False)
    op.create_index("ix_quotes_tenant_case", "quotes", ["tenant_id", "case_id"], unique=True)
    op.create_index("ix_lobs_tenant_case", "lobs", ["tenant_id", "case_id"], unique=False)
    op.create_index("ix_census_tenant_case", "census", ["tenant_id", "case_id"], unique=True)
    op.create_index("ix_outputs_tenant_case", "outputs", ["tenant_id", "case_id"], unique=True)
    op.create_table(
        "carrier_outputs",
        sa.Column("case_id", sa.String(length=32), primary_key=True),
        sa.Column("tenant_id", sa.String(length=128), nullable=False, server_default="default"),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index("ix_carrier_outputs_tenant_case", "carrier_outputs", ["tenant_id", "case_id"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_carrier_outputs_tenant_case", table_name="carrier_outputs")
    op.drop_table("carrier_outputs")
    for index_name, table_name in [
        ("ix_outputs_tenant_case", "outputs"),
        ("ix_census_tenant_case", "census"),
        ("ix_lobs_tenant_case", "lobs"),
        ("ix_quotes_tenant_case", "quotes"),
        ("ix_submissions_tenant_case", "submissions"),
        ("ix_submissions_tenant_submission", "submissions"),
    ]:
        op.drop_index(index_name, table_name=table_name)
    for table in ["outputs", "census", "lobs", "quotes", "submissions"]:
        op.drop_column(table, "tenant_id")
