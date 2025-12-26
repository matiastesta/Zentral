from alembic import op
import sqlalchemy as sa

revision = 'd4f1e8a0c9b7'
down_revision = 'c1a2f6e9d2b3'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Backfill display_name with username for existing users (optional)
    op.execute('UPDATE "user" SET display_name = username WHERE display_name IS NULL')


def downgrade() -> None:
    op.execute('UPDATE "user" SET display_name = NULL')
