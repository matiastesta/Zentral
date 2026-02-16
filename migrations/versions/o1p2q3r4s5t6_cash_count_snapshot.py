from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = 'o1p2q3r4s5t6'
down_revision = 'n1a2b3c4d5e6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    cols = {c.get('name') for c in (insp.get_columns('cash_count') or [])}
    if 'efectivo_calculado_snapshot' in cols:
        return
    op.add_column('cash_count', sa.Column('efectivo_calculado_snapshot', sa.Float(), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    cols = {c.get('name') for c in (insp.get_columns('cash_count') or [])}
    if 'efectivo_calculado_snapshot' not in cols:
        return
    op.drop_column('cash_count', 'efectivo_calculado_snapshot')
