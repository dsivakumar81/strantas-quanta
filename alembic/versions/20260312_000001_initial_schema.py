"""initial schema"""

from alembic import op
import sqlalchemy as sa

revision = "20260312_000001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "submissions",
        sa.Column("submission_id", sa.String(length=32), primary_key=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("case_id", sa.String(length=32), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "quotes",
        sa.Column("case_id", sa.String(length=32), primary_key=True),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_table(
        "lobs",
        sa.Column("lob_case_id", sa.String(length=64), primary_key=True),
        sa.Column("case_id", sa.String(length=32), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index("ix_lobs_case_id", "lobs", ["case_id"])
    op.create_table(
        "census",
        sa.Column("census_id", sa.String(length=32), primary_key=True),
        sa.Column("case_id", sa.String(length=32), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index("ix_census_case_id", "census", ["case_id"], unique=True)
    op.create_table(
        "outputs",
        sa.Column("case_id", sa.String(length=32), primary_key=True),
        sa.Column("payload", sa.JSON(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("outputs")
    op.drop_index("ix_census_case_id", table_name="census")
    op.drop_table("census")
    op.drop_index("ix_lobs_case_id", table_name="lobs")
    op.drop_table("lobs")
    op.drop_table("quotes")
    op.drop_table("submissions")
