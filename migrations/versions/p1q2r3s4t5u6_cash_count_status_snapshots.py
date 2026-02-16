from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = 'p1q2r3s4t5u6'
down_revision = 'o1p2q3r4s5t6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    cols = {c.get('name') for c in (insp.get_columns('cash_count') or [])}
    if 'cash_expected_at_save' not in cols:
        op.add_column('cash_count', sa.Column('cash_expected_at_save', sa.Float(), nullable=True))
    if 'last_cash_event_at_save' not in cols:
        op.add_column('cash_count', sa.Column('last_cash_event_at_save', sa.DateTime(), nullable=True))
    if 'status' not in cols:
        op.add_column('cash_count', sa.Column('status', sa.String(length=16), nullable=False, server_default='draft'))
    if 'done_at' not in cols:
        op.add_column('cash_count', sa.Column('done_at', sa.DateTime(), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    cols = {c.get('name') for c in (insp.get_columns('cash_count') or [])}
    if 'done_at' in cols:
        op.drop_column('cash_count', 'done_at')
    if 'status' in cols:
        op.drop_column('cash_count', 'status')
    if 'last_cash_event_at_save' in cols:
        op.drop_column('cash_count', 'last_cash_event_at_save')
    if 'cash_expected_at_save' in cols:
        op.drop_column('cash_count', 'cash_expected_at_save')
