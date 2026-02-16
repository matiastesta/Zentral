from alembic import op
import sqlalchemy as sa

revision = 'o1p2q3r4s5t6'
down_revision = 'n1a2b3c4d5e6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    try:
        op.add_column('cash_count', sa.Column('efectivo_calculado_snapshot', sa.Float(), nullable=True))
    except Exception:
        pass


def downgrade() -> None:
    try:
        op.drop_column('cash_count', 'efectivo_calculado_snapshot')
    except Exception:
        pass
