from alembic import op
import sqlalchemy as sa

revision = 'a3c9b1d7e5f0'
down_revision = 'f6a1d2c3b4e5'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('company', schema=None) as batch_op:
        batch_op.add_column(sa.Column('notes', sa.Text(), nullable=True))
        batch_op.add_column(sa.Column('admin_user_id', sa.Integer(), nullable=True))

    with op.batch_alter_table('user', schema=None) as batch_op:
        batch_op.add_column(sa.Column('password_plain', sa.Text(), nullable=True))

    op.create_table(
        'company_role',
        sa.Column('id', sa.String(length=36), nullable=False),
        sa.Column('company_id', sa.String(length=36), nullable=False),
        sa.Column('name', sa.String(length=64), nullable=False),
        sa.Column('permissions_json', sa.Text(), nullable=False, server_default='{}'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('company_id', 'name', name='uq_company_role_company_name'),
    )

    with op.batch_alter_table('company_role', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_company_role_company_id'), ['company_id'], unique=False)


def downgrade() -> None:
    with op.batch_alter_table('company_role', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_company_role_company_id'))

    op.drop_table('company_role')

    with op.batch_alter_table('user', schema=None) as batch_op:
        batch_op.drop_column('password_plain')

    with op.batch_alter_table('company', schema=None) as batch_op:
        batch_op.drop_column('admin_user_id')
        batch_op.drop_column('notes')
