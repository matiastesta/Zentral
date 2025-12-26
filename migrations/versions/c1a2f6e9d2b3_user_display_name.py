from alembic import op
import sqlalchemy as sa

revision = 'c1a2f6e9d2b3'
down_revision = 'b7c0d01c9a1f'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('user', schema=None) as batch_op:
        batch_op.add_column(sa.Column('display_name', sa.String(length=120), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table('user', schema=None) as batch_op:
        batch_op.drop_column('display_name')
