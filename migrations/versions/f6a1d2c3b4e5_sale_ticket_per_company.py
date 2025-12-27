from alembic import op
import sqlalchemy as sa

revision = 'f6a1d2c3b4e5'
down_revision = 'e3f2c9a4b1d0'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('sale', schema=None) as batch_op:
        batch_op.drop_index('ix_sale_ticket')
        batch_op.create_index('ix_sale_ticket', ['ticket'], unique=False)
        batch_op.create_unique_constraint('uq_sale_company_ticket', ['company_id', 'ticket'])


def downgrade() -> None:
    with op.batch_alter_table('sale', schema=None) as batch_op:
        batch_op.drop_constraint('uq_sale_company_ticket', type_='unique')
        batch_op.drop_index('ix_sale_ticket')
        batch_op.create_index('ix_sale_ticket', ['ticket'], unique=True)
