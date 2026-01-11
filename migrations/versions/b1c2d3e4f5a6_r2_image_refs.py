from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = 'b1c2d3e4f5a6'
down_revision = 'a1b2c3d4e5f6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])

    if 'product' in tables:
        cols = {str(c.get('name') or '') for c in (insp.get_columns('product') or [])}
        if 'image_file_id' not in cols:
            try:
                op.execute(sa.text('ALTER TABLE product ADD COLUMN image_file_id VARCHAR(64)'))
            except Exception:
                pass

        idxs = {str(i.get('name') or '') for i in (insp.get_indexes('product') or [])}
        if 'ix_product_image_file_id' not in idxs:
            try:
                op.execute(sa.text('CREATE INDEX IF NOT EXISTS ix_product_image_file_id ON product (image_file_id)'))
            except Exception:
                pass

    if 'business_settings' in tables:
        cols = {str(c.get('name') or '') for c in (insp.get_columns('business_settings') or [])}
        if 'logo_file_id' not in cols:
            try:
                op.execute(sa.text('ALTER TABLE business_settings ADD COLUMN logo_file_id VARCHAR(64)'))
            except Exception:
                pass
        if 'background_file_id' not in cols:
            try:
                op.execute(sa.text('ALTER TABLE business_settings ADD COLUMN background_file_id VARCHAR(64)'))
            except Exception:
                pass

        idxs = {str(i.get('name') or '') for i in (insp.get_indexes('business_settings') or [])}
        if 'ix_business_settings_logo_file_id' not in idxs:
            try:
                op.execute(sa.text('CREATE INDEX IF NOT EXISTS ix_business_settings_logo_file_id ON business_settings (logo_file_id)'))
            except Exception:
                pass
        if 'ix_business_settings_background_file_id' not in idxs:
            try:
                op.execute(sa.text('CREATE INDEX IF NOT EXISTS ix_business_settings_background_file_id ON business_settings (background_file_id)'))
            except Exception:
                pass


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])

    if 'business_settings' in tables:
        idxs = {str(i.get('name') or '') for i in (insp.get_indexes('business_settings') or [])}
        if 'ix_business_settings_background_file_id' in idxs:
            try:
                op.execute(sa.text('DROP INDEX ix_business_settings_background_file_id'))
            except Exception:
                pass
        if 'ix_business_settings_logo_file_id' in idxs:
            try:
                op.execute(sa.text('DROP INDEX ix_business_settings_logo_file_id'))
            except Exception:
                pass

        cols = {str(c.get('name') or '') for c in (insp.get_columns('business_settings') or [])}
        if 'background_file_id' in cols:
            try:
                op.execute(sa.text('ALTER TABLE business_settings DROP COLUMN background_file_id'))
            except Exception:
                pass
        if 'logo_file_id' in cols:
            try:
                op.execute(sa.text('ALTER TABLE business_settings DROP COLUMN logo_file_id'))
            except Exception:
                pass

    if 'product' in tables:
        idxs = {str(i.get('name') or '') for i in (insp.get_indexes('product') or [])}
        if 'ix_product_image_file_id' in idxs:
            try:
                op.execute(sa.text('DROP INDEX ix_product_image_file_id'))
            except Exception:
                pass

        cols = {str(c.get('name') or '') for c in (insp.get_columns('product') or [])}
        if 'image_file_id' in cols:
            try:
                op.execute(sa.text('ALTER TABLE product DROP COLUMN image_file_id'))
            except Exception:
                pass
