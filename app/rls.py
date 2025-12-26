import os

from sqlalchemy import func, inspect, text

from app import db


TENANT_TABLES = [
    'business_settings',
    'calendar_event',
    'category',
    'product',
    'inventory_lot',
    'inventory_movement',
    'sale',
    'sale_item',
    'calendar_user_config',
    'cash_count',
    'customer',
    'employee',
    'expense',
    'supplier',
    'expense_category',
    'user',
]


def reset_public_schema() -> None:
    db.session.execute(text('DROP SCHEMA public CASCADE'))
    db.session.execute(text('CREATE SCHEMA public'))
    db.session.execute(text('GRANT ALL ON SCHEMA public TO CURRENT_USER'))


def apply_rls_policies() -> None:
    for table in TENANT_TABLES:
        ident = '"user"' if table == 'user' else table
        db.session.execute(text(f'ALTER TABLE {ident} ENABLE ROW LEVEL SECURITY'))
        db.session.execute(text(f'ALTER TABLE {ident} FORCE ROW LEVEL SECURITY'))

        if table == 'user':
            db.session.execute(text('DROP POLICY IF EXISTS tenant_isolation ON "user"'))
            db.session.execute(
                text(
                    """
                    CREATE POLICY tenant_isolation ON "user"
                    USING (
                        current_setting('app.is_zentral_admin', true) = '1'
                        OR (company_id IS NOT NULL AND company_id = current_setting('app.current_company_id', true))
                        OR (
                            current_setting('app.is_login', true) = '1'
                            AND (
                                (email IS NOT NULL AND email = current_setting('app.login_email', true))
                                OR (username IS NOT NULL AND username = current_setting('app.login_email', true))
                                OR (company_id IS NOT NULL AND company_id = current_setting('app.current_company_id', true))
                                OR (company_id IS NULL AND role = 'zentral_admin')
                            )
                        )
                    )
                    WITH CHECK (
                        current_setting('app.is_zentral_admin', true) = '1'
                        OR (company_id IS NOT NULL AND company_id = current_setting('app.current_company_id', true))
                    )
                    """
                )
            )
            continue

        db.session.execute(text(f'DROP POLICY IF EXISTS tenant_isolation ON {ident}'))
        db.session.execute(
            text(
                f"""
                CREATE POLICY tenant_isolation ON {ident}
                USING (
                    current_setting('app.is_zentral_admin', true) = '1'
                    OR company_id = current_setting('app.current_company_id', true)
                )
                WITH CHECK (
                    current_setting('app.is_zentral_admin', true) = '1'
                    OR company_id = current_setting('app.current_company_id', true)
                )
                """
            )
        )

    db.session.execute(text('ALTER TABLE company ENABLE ROW LEVEL SECURITY'))
    db.session.execute(text('ALTER TABLE company FORCE ROW LEVEL SECURITY'))
    db.session.execute(text('DROP POLICY IF EXISTS company_access ON company'))
    db.session.execute(
        text(
            """
            CREATE POLICY company_access ON company
            USING (
                current_setting('app.is_zentral_admin', true) = '1'
                OR id = current_setting('app.current_company_id', true)
                OR slug = current_setting('app.company_slug', true)
            )
            WITH CHECK (
                current_setting('app.is_zentral_admin', true) = '1'
            )
            """
        )
    )


def bootstrap_schema(reset: bool) -> None:
    engine = db.engine
    is_sqlite = str(engine.url.drivername).startswith('sqlite')

    def _upgrade_db_to_head() -> None:
        try:
            from alembic import command
            from alembic.config import Config as AlembicConfig
        except Exception as e:
            raise RuntimeError('Alembic is required to bootstrap Postgres. Install Flask-Migrate/Alembic and run flask db upgrade.') from e

        root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        ini_path = os.path.join(root, 'alembic.ini')
        if not os.path.exists(ini_path):
            raise RuntimeError('Missing alembic.ini. Initialize migrations and run flask db upgrade.')

        cfg = AlembicConfig(ini_path)
        command.upgrade(cfg, 'head')

    def _sqlite_reset_all_tables() -> None:
        if not is_sqlite:
            return
        insp = inspect(engine)
        names = list(insp.get_table_names() or [])
        for t in names:
            db.session.execute(text(f'DROP TABLE IF EXISTS "{t}"'))

    def _sqlite_ensure_model_columns(model) -> None:
        if not is_sqlite:
            return
        table_name = str(getattr(model, '__tablename__', '') or '').strip()
        if not table_name:
            return

        insp = inspect(engine)
        if table_name not in set(insp.get_table_names() or []):
            return

        try:
            existing = {str(c.get('name') or '') for c in (insp.get_columns(table_name) or [])}
        except Exception:
            existing = set()

        for col in list(getattr(model, '__table__').columns):
            name = str(getattr(col, 'name', '') or '').strip()
            if not name or name in existing:
                continue
            try:
                coltype = col.type.compile(dialect=engine.dialect)
            except Exception:
                coltype = 'TEXT'
            db.session.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{name}" {coltype}'))
            existing.add(name)

    def _sqlite_rebuild_user_table_if_needed() -> None:
        if not is_sqlite:
            return
        insp = inspect(engine)
        if 'user' not in set(insp.get_table_names() or []):
            return

        try:
            cols = {str(c.get('name') or ''): bool(c.get('nullable', True)) for c in (insp.get_columns('user') or [])}
        except Exception:
            cols = {}

        email_nullable = bool(cols.get('email', True))

        has_unique_username = False
        has_unique_company_username = False
        try:
            for uc in (insp.get_unique_constraints('user') or []):
                cns = [str(x) for x in (uc.get('column_names') or [])]
                if cns == ['username']:
                    has_unique_username = True
                if cns == ['company_id', 'username']:
                    has_unique_company_username = True
        except Exception:
            pass

        if email_nullable and (not has_unique_username) and has_unique_company_username:
            return

        db.session.execute(text('PRAGMA foreign_keys=OFF'))
        db.session.execute(text('ALTER TABLE "user" RENAME TO "user_old"'))
        db.session.execute(
            text(
                """
                CREATE TABLE "user" (
                    id INTEGER NOT NULL,
                    company_id VARCHAR(36),
                    username VARCHAR(80) NOT NULL,
                    display_name VARCHAR(120),
                    email VARCHAR(255),
                    password_hash VARCHAR(255) NOT NULL,
                    role VARCHAR(32) NOT NULL,
                    level INTEGER NOT NULL,
                    created_by_user_id INTEGER,
                    permissions_json TEXT NOT NULL,
                    is_master BOOLEAN NOT NULL,
                    active BOOLEAN NOT NULL,
                    PRIMARY KEY (id),
                    CONSTRAINT uq_user_company_username UNIQUE (company_id, username)
                )
                """
            )
        )
        db.session.execute(
            text(
                """
                INSERT INTO "user" (
                    id, company_id, username, display_name, email, password_hash, role, level,
                    created_by_user_id, permissions_json, is_master, active
                )
                SELECT
                    id, company_id, username, NULLIF(TRIM(COALESCE(display_name, '')), ''),
                    NULLIF(TRIM(COALESCE(email, '')), ''),
                    password_hash, role, level,
                    created_by_user_id, permissions_json, is_master, active
                FROM "user_old"
                """
            )
        )
        db.session.execute(text('DROP TABLE "user_old"'))
        db.session.execute(text('CREATE INDEX IF NOT EXISTS ix_user_company_id ON "user" (company_id)'))
        db.session.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS ix_user_email ON "user" (email)'))
        db.session.execute(text('PRAGMA foreign_keys=ON'))

    if reset:
        if is_sqlite:
            _sqlite_reset_all_tables()
            db.session.commit()
        else:
            reset_public_schema()
            db.session.commit()

    from app.models import Company, SystemMeta, User

    if is_sqlite:
        db.create_all()
    else:
        _upgrade_db_to_head()

    # SQLite: si la DB existe desde antes (sin migraciones), aseguramos columnas faltantes
    if is_sqlite:
        try:
            from app.models import (
                BusinessSettings,
                CalendarEvent,
                CalendarUserConfig,
                CashCount,
                Category,
                Customer,
                Employee,
                Expense,
                ExpenseCategory,
                InventoryLot,
                InventoryMovement,
                Product,
                Sale,
                SaleItem,
                Supplier,
            )

            for m in (
                Company,
                SystemMeta,
                User,
                BusinessSettings,
                CalendarEvent,
                CalendarUserConfig,
                CashCount,
                Category,
                Customer,
                Employee,
                Expense,
                ExpenseCategory,
                InventoryLot,
                InventoryMovement,
                Product,
                Sale,
                SaleItem,
                Supplier,
            ):
                _sqlite_ensure_model_columns(m)

            _sqlite_rebuild_user_table_if_needed()
            db.session.commit()
        except Exception:
            db.session.rollback()

    meta = db.session.get(SystemMeta, 'initialized')
    if not meta:
        db.session.add(SystemMeta(key='initialized', value='1'))

    admin_username = (os.environ.get('ZENTRAL_ADMIN_USERNAME') or 'zentra').strip()
    admin_email = (os.environ.get('ZENTRAL_ADMIN_EMAIL') or 'zentra@zentral.local').strip().lower()
    admin_pass = os.environ.get('ZENTRAL_ADMIN_PASSWORD') or 'zentra'

    with db.session.no_autoflush:
        admin = db.session.query(User).filter(User.role == 'zentral_admin').first()
        admin_id = int(getattr(admin, 'id', 0) or 0) if admin else 0

        # Free up username/email if they are already used by another user
        uname_conflict = (
            db.session.query(User)
            .filter(func.lower(User.username) == admin_username.strip().lower())
            .filter(User.id != admin_id if admin_id else True)
            .first()
        )
        if uname_conflict:
            base = str(getattr(uname_conflict, 'username', '') or 'user').strip() or 'user'
            suffix = str(getattr(uname_conflict, 'id', '') or '').strip() or 'x'
            uname_conflict.username = f"{base}_{suffix}"

        email_conflict = (
            db.session.query(User)
            .filter(func.lower(User.email) == admin_email.strip().lower())
            .filter(User.id != admin_id if admin_id else True)
            .first()
        )
        if email_conflict:
            # Keep it simple; generate an address that stays unique.
            suffix = str(getattr(email_conflict, 'id', '') or '').strip() or 'x'
            email_conflict.email = f"conflict_{suffix}@zentral.local"

        if not admin:
            admin = User(
                username=admin_username,
                email=admin_email,
                role='zentral_admin',
                is_master=True,
                active=True,
                company_id=None,
                level=0,
            )
            admin.set_permissions_all(True)
            db.session.add(admin)
        else:
            admin.is_master = True
            admin.active = True
            admin.company_id = None
            admin.level = 0
            admin.role = 'zentral_admin'
            admin.set_permissions_all(True)
            admin.username = admin_username
            admin.email = admin_email

        admin.set_password(admin_pass)

    if not db.session.query(Company).first():
        demo_slug = (os.environ.get('DEFAULT_COMPANY_SLUG') or 'demo').strip().lower()
        demo_name = (os.environ.get('DEFAULT_COMPANY_NAME') or 'Empresa Demo').strip()
        c = Company(name=demo_name, slug=demo_slug)
        db.session.add(c)
        db.session.flush()

        demo_email = (os.environ.get('DEFAULT_COMPANY_ADMIN_EMAIL') or f'admin@{demo_slug}.local').strip().lower()
        demo_pass = os.environ.get('DEFAULT_COMPANY_ADMIN_PASSWORD') or 'admin'
        demo_user = User(
            username=f'admin_{demo_slug}',
            email=demo_email,
            role='company_admin',
            is_master=False,
            active=True,
            company_id=str(c.id),
            level=1,
        )
        demo_user.set_password(demo_pass)
        demo_user.set_permissions_all(True)
        db.session.add(demo_user)

    db.session.commit()

    if not is_sqlite:
        apply_rls_policies()
        db.session.commit()
