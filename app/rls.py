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
    'installment_plan',
    'installment',
    'calendar_user_config',
    'cash_count',
    'customer',
    'employee',
    'expense',
    'supplier',
    'expense_category',
    'company_role',
    'user',
]


def reset_public_schema() -> None:
    db.session.execute(text('DROP SCHEMA public CASCADE'))
    db.session.execute(text('CREATE SCHEMA public'))
    db.session.execute(text('GRANT ALL ON SCHEMA public TO CURRENT_USER'))


def apply_rls_policies() -> None:
    try:
        engine = db.engine
        insp = inspect(engine)
        existing = set(insp.get_table_names() or [])
    except Exception:
        existing = None

    for table in TENANT_TABLES:
        if existing is not None and table not in existing:
            continue
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

    def _postgres_ensure_sale_employee_columns() -> None:
        if is_sqlite:
            return
        try:
            insp = inspect(engine)
        except Exception:
            return
        try:
            tables = set(insp.get_table_names() or [])
        except Exception:
            return

        if 'sale' in tables:
            try:
                existing = {str(c.get('name') or '') for c in (insp.get_columns('sale') or [])}
            except Exception:
                existing = set()

            if 'employee_id' not in existing:
                db.session.execute(text('ALTER TABLE sale ADD COLUMN IF NOT EXISTS employee_id VARCHAR(64)'))
                existing.add('employee_id')
            if 'employee_name' not in existing:
                db.session.execute(text('ALTER TABLE sale ADD COLUMN IF NOT EXISTS employee_name VARCHAR(255)'))
                existing.add('employee_name')
            if 'is_installments' not in existing:
                db.session.execute(text('ALTER TABLE sale ADD COLUMN IF NOT EXISTS is_installments BOOLEAN NOT NULL DEFAULT FALSE'))
                existing.add('is_installments')

        if 'business_settings' in tables:
            try:
                bs_existing = {str(c.get('name') or '') for c in (insp.get_columns('business_settings') or [])}
            except Exception:
                bs_existing = set()
            if 'habilitar_sistema_cuotas' not in bs_existing:
                db.session.execute(
                    text(
                        'ALTER TABLE business_settings ADD COLUMN IF NOT EXISTS habilitar_sistema_cuotas BOOLEAN NOT NULL DEFAULT FALSE'
                    )
                )

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

        has_password_plain = False
        try:
            has_password_plain = 'password_plain' in set(cols.keys())
        except Exception:
            has_password_plain = False

        has_level = False
        try:
            has_level = 'level' in set(cols.keys())
        except Exception:
            has_level = False

        if email_nullable and (not has_unique_username) and has_unique_company_username and has_password_plain and has_level:
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
                    password_plain TEXT,
                    role VARCHAR(32) NOT NULL,
                    level INTEGER NOT NULL DEFAULT 0,
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
                    id, company_id, username, display_name, email, password_hash, password_plain, role, level,
                    created_by_user_id, permissions_json, is_master, active
                )
                SELECT
                    id, company_id, username, NULLIF(TRIM(COALESCE(display_name, '')), ''),
                    NULLIF(TRIM(COALESCE(email, '')), ''),
                    password_hash,
                    NULLIF(TRIM(COALESCE(password_plain, '')), ''),
                    role,
                    COALESCE(level, 0),
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

    from app.models import Company, CompanyRole, Plan, SystemMeta, User

    if is_sqlite:
        db.create_all()
    else:
        try:
            _upgrade_db_to_head()
        except Exception:
            db.session.rollback()

        try:
            insp = inspect(engine)
            tables = set(insp.get_table_names() or [])
        except Exception:
            tables = set()

        try:
            if 'installment_plan' not in tables or 'installment' not in tables:
                from app.models import Installment, InstallmentPlan

                db.metadata.create_all(bind=engine, tables=[InstallmentPlan.__table__, Installment.__table__])
        except Exception:
            db.session.rollback()

        try:
            _postgres_ensure_sale_employee_columns()
            db.session.commit()
        except Exception:
            db.session.rollback()

    # SQLite: si la DB existe desde antes (sin migraciones), aseguramos columnas faltantes
    if is_sqlite:
        try:
            from app.models import (
                BusinessSettings,
                CalendarEvent,
                CalendarUserConfig,
                CashCount,
                Category,
                CompanyRole,
                Plan,
                Installment,
                InstallmentPlan,
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
                CompanyRole,
                Plan,
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
                InstallmentPlan,
                Installment,
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
    db.session.commit()

    if not is_sqlite:
        apply_rls_policies()
        db.session.commit()
