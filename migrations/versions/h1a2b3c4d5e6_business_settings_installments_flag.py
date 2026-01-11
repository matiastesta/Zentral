from alembic import op
import sqlalchemy as sa

revision = 'h1a2b3c4d5e6'
down_revision = 'g9h8i7j6k5l4'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('business_settings', schema=None) as batch_op:
        try:
            batch_op.add_column(sa.Column('habilitar_sistema_cuotas', sa.Boolean(), nullable=False, server_default=sa.text('0')))
        except Exception:
            pass


def downgrade() -> None:
    with op.batch_alter_table('business_settings', schema=None) as batch_op:
        try:
            batch_op.drop_column('habilitar_sistema_cuotas')
        except Exception:
            pass
