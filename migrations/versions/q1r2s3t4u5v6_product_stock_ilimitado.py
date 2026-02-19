from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = 'q1r2s3t4u5v6'
down_revision = 'p1q2r3s4t5u6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    cols = {c.get('name') for c in (insp.get_columns('product') or [])}
    if 'stock_ilimitado' in cols:
        return
    op.add_column('product', sa.Column('stock_ilimitado', sa.Boolean(), nullable=False, server_default=sa.text('false')))


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    cols = {c.get('name') for c in (insp.get_columns('product') or [])}
    if 'stock_ilimitado' not in cols:
        return
    op.drop_column('product', 'stock_ilimitado')
