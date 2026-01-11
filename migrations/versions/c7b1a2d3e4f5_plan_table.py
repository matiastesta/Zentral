from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = 'c7b1a2d3e4f5'
down_revision = 'f6a1d2c3b4e5'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])
    if 'plan' in tables:
        return

    op.create_table(
        'plan',
        sa.Column('code', sa.String(length=64), nullable=False),
        sa.Column('active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('code'),
    )


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])
    if 'plan' not in tables:
        return
    op.drop_table('plan')
