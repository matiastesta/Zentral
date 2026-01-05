from alembic import op
import sqlalchemy as sa

revision = 'g9h8i7j6k5l4'
down_revision = 'f6a1d2c3b4e5'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('calendar_user_config', schema=None) as batch_op:
        try:
            batch_op.drop_index('ix_calendar_user_config_user_id')
        except Exception:
            pass
        try:
            batch_op.create_index('ix_calendar_user_config_user_id', ['user_id'], unique=False)
        except Exception:
            pass
        try:
            batch_op.create_unique_constraint('uq_calendar_user_config_company_user', ['company_id', 'user_id'])
        except Exception:
            pass

    with op.batch_alter_table('cash_count', schema=None) as batch_op:
        try:
            batch_op.drop_index('ix_cash_count_count_date')
        except Exception:
            pass
        try:
            batch_op.create_index('ix_cash_count_count_date', ['count_date'], unique=False)
        except Exception:
            pass
        try:
            batch_op.create_unique_constraint('uq_cash_count_company_date', ['company_id', 'count_date'])
        except Exception:
            pass

    with op.batch_alter_table('sale', schema=None) as batch_op:
        try:
            batch_op.add_column(sa.Column('employee_id', sa.String(length=64), nullable=True))
        except Exception:
            pass
        try:
            batch_op.add_column(sa.Column('employee_name', sa.String(length=255), nullable=True))
        except Exception:
            pass


def downgrade() -> None:
    with op.batch_alter_table('sale', schema=None) as batch_op:
        try:
            batch_op.drop_column('employee_name')
        except Exception:
            pass
        try:
            batch_op.drop_column('employee_id')
        except Exception:
            pass

    with op.batch_alter_table('cash_count', schema=None) as batch_op:
        try:
            batch_op.drop_constraint('uq_cash_count_company_date', type_='unique')
        except Exception:
            pass
        try:
            batch_op.drop_index('ix_cash_count_count_date')
        except Exception:
            pass
        try:
            batch_op.create_index('ix_cash_count_count_date', ['count_date'], unique=True)
        except Exception:
            pass

    with op.batch_alter_table('calendar_user_config', schema=None) as batch_op:
        try:
            batch_op.drop_constraint('uq_calendar_user_config_company_user', type_='unique')
        except Exception:
            pass
        try:
            batch_op.drop_index('ix_calendar_user_config_user_id')
        except Exception:
            pass
        try:
            batch_op.create_index('ix_calendar_user_config_user_id', ['user_id'], unique=True)
        except Exception:
            pass
