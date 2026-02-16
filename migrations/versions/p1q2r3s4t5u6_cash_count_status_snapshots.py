from alembic import op
import sqlalchemy as sa

revision = 'p1q2r3s4t5u6'
down_revision = 'o1p2q3r4s5t6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    try:
        op.add_column('cash_count', sa.Column('cash_expected_at_save', sa.Float(), nullable=True))
    except Exception:
        pass
    try:
        op.add_column('cash_count', sa.Column('last_cash_event_at_save', sa.DateTime(), nullable=True))
    except Exception:
        pass
    try:
        op.add_column('cash_count', sa.Column('status', sa.String(length=16), nullable=False, server_default='draft'))
    except Exception:
        pass
    try:
        op.add_column('cash_count', sa.Column('done_at', sa.DateTime(), nullable=True))
    except Exception:
        pass


def downgrade() -> None:
    try:
        op.drop_column('cash_count', 'done_at')
    except Exception:
        pass
    try:
        op.drop_column('cash_count', 'status')
    except Exception:
        pass
    try:
        op.drop_column('cash_count', 'last_cash_event_at_save')
    except Exception:
        pass
    try:
        op.drop_column('cash_count', 'cash_expected_at_save')
    except Exception:
        pass
