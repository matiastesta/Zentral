from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = 'a3c9b1d7e5f0'
down_revision = 'f6a1d2c3b4e5'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])

    if 'company' in tables:
        company_cols = {str(c.get('name') or '') for c in (insp.get_columns('company') or [])}
        if 'notes' not in company_cols:
            try:
                op.execute(sa.text('ALTER TABLE company ADD COLUMN notes TEXT'))
            except Exception:
                pass
        if 'admin_user_id' not in company_cols:
            try:
                op.execute(sa.text('ALTER TABLE company ADD COLUMN admin_user_id INTEGER'))
            except Exception:
                pass

    if 'user' in tables:
        user_cols = {str(c.get('name') or '') for c in (insp.get_columns('user') or [])}
        if 'password_plain' not in user_cols:
            try:
                op.execute(sa.text('ALTER TABLE user ADD COLUMN password_plain TEXT'))
            except Exception:
                pass

    # company_role table
    if 'company_role' not in tables:
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

    # index
    try:
        with op.batch_alter_table('company_role', schema=None) as batch_op:
            batch_op.create_index(batch_op.f('ix_company_role_company_id'), ['company_id'], unique=False)
    except Exception:
        pass


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])

    if 'company_role' in tables:
        try:
            with op.batch_alter_table('company_role', schema=None) as batch_op:
                batch_op.drop_index(batch_op.f('ix_company_role_company_id'))
        except Exception:
            pass
        try:
            op.drop_table('company_role')
        except Exception:
            pass

    if 'user' in tables:
        user_cols = {str(c.get('name') or '') for c in (insp.get_columns('user') or [])}
        if 'password_plain' in user_cols:
            try:
                op.execute(sa.text('ALTER TABLE user DROP COLUMN password_plain'))
            except Exception:
                pass

    if 'company' in tables:
        company_cols = {str(c.get('name') or '') for c in (insp.get_columns('company') or [])}
        if 'admin_user_id' in company_cols:
            try:
                op.execute(sa.text('ALTER TABLE company DROP COLUMN admin_user_id'))
            except Exception:
                pass
        if 'notes' in company_cols:
            try:
                op.execute(sa.text('ALTER TABLE company DROP COLUMN notes'))
            except Exception:
                pass
