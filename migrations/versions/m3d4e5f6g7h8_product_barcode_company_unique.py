from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = 'm3d4e5f6g7h8'
down_revision = 'l2c3d4e5f6g7'
branch_labels = None
depends_on = None


def _drop_constraint_if_exists(insp, table: str, name: str) -> None:
    try:
        for uc in (insp.get_unique_constraints(table) or []):
            if str(uc.get('name') or '') == name:
                op.drop_constraint(name, table, type_='unique')
                return
    except Exception:
        pass


def _drop_index_if_exists(insp, table: str, name: str) -> None:
    try:
        for ix in (insp.get_indexes(table) or []):
            if str(ix.get('name') or '') == name:
                op.drop_index(name, table_name=table)
                return
    except Exception:
        pass


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)

    tables = set(insp.get_table_names() or [])
    if 'product' not in tables:
        return

    # Postgres default unique constraint names
    _drop_constraint_if_exists(insp, 'product', 'product_internal_code_key')
    _drop_constraint_if_exists(insp, 'product', 'product_barcode_key')

    # Some DBs may use unique indexes instead
    _drop_index_if_exists(insp, 'product', 'ix_product_internal_code')
    _drop_index_if_exists(insp, 'product', 'ix_product_barcode')

    # Add composite unique constraints (per company)
    try:
        op.create_unique_constraint('uq_product_company_internal_code', 'product', ['company_id', 'internal_code'])
    except Exception:
        pass
    try:
        op.create_unique_constraint('uq_product_company_barcode', 'product', ['company_id', 'barcode'])
    except Exception:
        pass

    # Keep helpful non-unique indexes
    try:
        op.create_index('ix_product_internal_code', 'product', ['internal_code'], unique=False)
    except Exception:
        pass
    try:
        op.create_index('ix_product_barcode', 'product', ['barcode'], unique=False)
    except Exception:
        pass


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)

    tables = set(insp.get_table_names() or [])
    if 'product' not in tables:
        return

    try:
        op.drop_constraint('uq_product_company_internal_code', 'product', type_='unique')
    except Exception:
        pass
    try:
        op.drop_constraint('uq_product_company_barcode', 'product', type_='unique')
    except Exception:
        pass

    # Restore global uniqueness (best-effort)
    try:
        op.create_unique_constraint('product_internal_code_key', 'product', ['internal_code'])
    except Exception:
        pass
    try:
        op.create_unique_constraint('product_barcode_key', 'product', ['barcode'])
    except Exception:
        pass
