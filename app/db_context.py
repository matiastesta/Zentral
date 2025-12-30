from flask import g, has_request_context, request, session
from sqlalchemy import event, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session, with_loader_criteria

from app import db
from app.tenancy import ensure_request_context, resolve_company_slug


_SQLITE_TENANT_GUARDS_CONFIGURED = False
_SESSION_TENANT_CONTEXT_HOOKS_CONFIGURED = False


def _rls_settings(*, is_login: bool, login_email: str | None = None) -> dict:
    if not has_request_context():
        return {
            'slug': '',
            'cid': '',
            'is_admin': '0',
            'is_login': '0',
            'login_email': '',
        }

    slug = resolve_company_slug() or ''

    ensure_request_context()
    cid = str(getattr(g, 'company_id', None) or '').strip()

    is_admin = '1' if (session.get('auth_is_zentral_admin') == '1') else '0'
    impersonating = bool(session.get('impersonate_company_id'))

    if is_admin == '1' and impersonating and cid:
        is_admin = '0'

    is_login_v = '1' if is_login else '0'
    login_email_v = (str(login_email or '').strip().lower() if is_login else '')

    return {
        'slug': slug,
        'cid': cid,
        'is_admin': is_admin,
        'is_login': is_login_v,
        'login_email': login_email_v,
    }


def _apply_rls_settings_on_connection(conn, *, is_login: bool, login_email: str | None = None) -> None:
    settings = _rls_settings(is_login=is_login, login_email=login_email)
    conn.execute(
        text(
            """
            SELECT
                set_config('app.company_slug', :slug, true),
                set_config('app.current_company_id', :cid, true),
                set_config('app.is_zentral_admin', :is_admin, true),
                set_config('app.is_login', :is_login, true),
                set_config('app.login_email', :login_email, true)
            """
        ),
        settings,
    )


def _apply_rls_settings_payload_on_connection(conn, *, settings: dict) -> None:
    conn.execute(
        text(
            """
            SELECT
                set_config('app.company_slug', :slug, true),
                set_config('app.current_company_id', :cid, true),
                set_config('app.is_zentral_admin', :is_admin, true),
                set_config('app.is_login', :is_login, true),
                set_config('app.login_email', :login_email, true)
            """
        ),
        settings,
    )


def apply_rls_context(*, is_login: bool, login_email: str | None = None) -> None:
    if not has_request_context():
        return

    def _is_missing_company_table(err: Exception) -> bool:
        try:
            msg = str(err)
        except Exception:
            msg = ''
        if 'relation "company" does not exist' in msg:
            return True
        if 'UndefinedTable' in msg and 'company' in msg:
            return True
        return False

    engine_drivername = ''
    try:
        engine_drivername = str(db.engine.url.drivername)
    except Exception:
        engine_drivername = 'sqlite'

    if engine_drivername.startswith('sqlite'):
        ensure_request_context()
        g._is_login = bool(is_login)
        g._login_email = (str(login_email or '').strip().lower() if is_login else '')
        return

    # Postgres: bootstrap RLS settings in two phases to avoid a circular dependency.
    #
    # - Resolving company_id may require querying Company by slug.
    # - Company table RLS policy allows access by app.company_slug, which is set via set_config.
    # - Therefore, we must set app.company_slug first (and session-derived company_id if present),
    #   then resolve request context, then re-apply with the final resolved company_id.
    slug = resolve_company_slug() or ''
    try:
        is_admin_v = '1' if (session.get('auth_is_zentral_admin') == '1') else '0'
    except Exception:
        is_admin_v = '0'
    try:
        cid_from_session = ''
        if is_admin_v == '1':
            cid_from_session = str(session.get('impersonate_company_id') or '').strip()
        if not cid_from_session:
            cid_from_session = str(session.get('auth_company_id') or '').strip()
    except Exception:
        cid_from_session = ''

    is_login_v = '1' if is_login else '0'
    login_email_v = (str(login_email or '').strip().lower() if is_login else '')

    initial_settings = {
        'slug': slug,
        'cid': cid_from_session,
        'is_admin': is_admin_v,
        'is_login': is_login_v,
        'login_email': login_email_v,
    }

    try:
        g._is_login = bool(is_login)
        g._login_email = (str(login_email or '').strip().lower() if is_login else '')
    except Exception:
        pass

    try:
        conn = db.session.connection()
        _apply_rls_settings_payload_on_connection(conn, settings=initial_settings)

        try:
            g._rls_settings_payload = dict(initial_settings)
        except Exception:
            pass

        ensure_request_context()
        resolved_cid = str(getattr(g, 'company_id', None) or '').strip()
        if resolved_cid and resolved_cid != str(cid_from_session or '').strip():
            updated = dict(initial_settings)
            updated['cid'] = resolved_cid
            _apply_rls_settings_payload_on_connection(conn, settings=updated)

            try:
                g._rls_settings_payload = dict(updated)
            except Exception:
                pass
    except Exception as e:
        if isinstance(e, ProgrammingError) or _is_missing_company_table(e):
            try:
                db.session.rollback()
            except Exception:
                pass
            return
        try:
            db.session.rollback()
        except Exception:
            pass
        raise


def configure_session_tenant_context_hooks() -> None:
    global _SESSION_TENANT_CONTEXT_HOOKS_CONFIGURED
    if _SESSION_TENANT_CONTEXT_HOOKS_CONFIGURED:
        return

    @event.listens_for(Session, 'after_begin')
    def _reapply_rls_context_after_begin(sess, transaction, connection):
        if not has_request_context():
            return

        try:
            engine_drivername = str(connection.engine.url.drivername)
        except Exception:
            engine_drivername = ''
        if engine_drivername.startswith('sqlite'):
            return

        try:
            is_login = bool(getattr(g, '_is_login', False))
            login_email = str(getattr(g, '_login_email', '') or '')
        except Exception:
            is_login = False
            login_email = ''

        try:
            # IMPORTANT: do not call ensure_request_context() (or any ORM query) here.
            # after_begin fires while the Session is provisioning a new connection and
            # concurrent ORM operations are not permitted.
            payload = None
            try:
                payload = getattr(g, '_rls_settings_payload', None)
            except Exception:
                payload = None

            if not isinstance(payload, dict):
                slug = resolve_company_slug() or ''
                try:
                    is_admin_v = '1' if (session.get('auth_is_zentral_admin') == '1') else '0'
                except Exception:
                    is_admin_v = '0'
                try:
                    cid = ''
                    if is_admin_v == '1':
                        cid = str(session.get('impersonate_company_id') or '').strip()
                    if not cid:
                        cid = str(session.get('auth_company_id') or '').strip()
                except Exception:
                    cid = ''

                payload = {
                    'slug': slug,
                    'cid': cid,
                    'is_admin': is_admin_v,
                    'is_login': '1' if is_login else '0',
                    'login_email': (str(login_email or '').strip().lower() if is_login else ''),
                }

            _apply_rls_settings_payload_on_connection(connection, settings=payload)
        except Exception:
            try:
                sess.rollback()
            except Exception:
                pass
            raise

    _SESSION_TENANT_CONTEXT_HOOKS_CONFIGURED = True


def configure_sqlite_tenant_guards() -> None:
    global _SQLITE_TENANT_GUARDS_CONFIGURED
    if _SQLITE_TENANT_GUARDS_CONFIGURED:
        return

    def _is_sqlite() -> bool:
        try:
            return str(db.engine.url.drivername).startswith('sqlite')
        except Exception:
            return False

    def _from_table_name(f):
        try:
            n = str(getattr(f, 'name', '') or '').strip().lower()
            if n:
                return n
        except Exception:
            pass
        try:
            el = getattr(f, 'element', None)
            n = str(getattr(el, 'name', '') or '').strip().lower()
            if n:
                return n
        except Exception:
            pass
        return ''

    @event.listens_for(Session, 'do_orm_execute')
    def _sqlite_tenant_filter(execute_state):
        if not _is_sqlite():
            return
        if not getattr(execute_state, 'is_select', False):
            return
        if not has_request_context():
            return

        try:
            if execute_state.execution_options.get('_sqlite_tenant_guard_applied'):
                return
        except Exception:
            pass

        try:
            stmt = execute_state.statement
            if not hasattr(stmt, 'options'):
                return
        except Exception:
            return

        try:
            if session.get('auth_is_zentral_admin') == '1' and not session.get('impersonate_company_id'):
                return
        except Exception:
            pass

        ensure_request_context()
        cid = str(getattr(g, 'company_id', '') or '').strip()
        if not cid:
            return

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
            FileAsset,
            InventoryLot,
            InventoryMovement,
            Product,
            Sale,
            SaleItem,
            Supplier,
            User,
        )

        is_login = bool(getattr(g, '_is_login', False))

        # Extra safety: cover statements that select only columns (no ORM entities)
        # by adding WHERE clauses for any FROM that exposes a company_id column.
        try:
            stmt = execute_state.statement
            froms = list(getattr(stmt, 'get_final_froms', lambda: [])() or [])
            conds = []
            for f in froms:
                try:
                    cols = getattr(f, 'c', None)
                    if cols is None or 'company_id' not in cols:
                        continue
                    if is_login and _from_table_name(f) == 'user':
                        continue
                    conds.append(cols.company_id == cid)
                except Exception:
                    continue
            if conds:
                execute_state.statement = stmt.where(*conds)
        except Exception:
            pass

        models = (
            BusinessSettings,
            CalendarEvent,
            CalendarUserConfig,
            CashCount,
            Category,
            Customer,
            Employee,
            Expense,
            ExpenseCategory,
            FileAsset,
            InventoryLot,
            InventoryMovement,
            Product,
            Sale,
            SaleItem,
            Supplier,
            User,
        )

        opts = []
        for m in models:
            if m is User and is_login:
                continue
            opts.append(with_loader_criteria(m, lambda cls, cid=cid: cls.company_id == cid, include_aliases=True))

        if opts:
            execute_state.statement = execute_state.statement.options(*opts)

        try:
            execute_state.statement = execute_state.statement.execution_options(_sqlite_tenant_guard_applied=True)
            execute_state.update_execution_options(_sqlite_tenant_guard_applied=True)
        except Exception:
            pass

    @event.listens_for(Session, 'before_flush')
    def _sqlite_tenant_write_guard(sess, flush_context, instances):
        if not _is_sqlite():
            return
        if not has_request_context():
            return

        try:
            if session.get('auth_is_zentral_admin') == '1' and not session.get('impersonate_company_id'):
                return
        except Exception:
            pass

        ensure_request_context()
        cid = str(getattr(g, 'company_id', '') or '').strip()
        if not cid:
            return

        for obj in list(getattr(sess, 'new', []) or []):
            if hasattr(obj, 'company_id'):
                cur = getattr(obj, 'company_id', None)
                cur_s = str(cur or '').strip()
                if not cur_s:
                    setattr(obj, 'company_id', cid)
                elif cur_s != cid:
                    raise PermissionError('Cross-tenant insert blocked')

        for obj in list(getattr(sess, 'dirty', []) or []):
            if hasattr(obj, 'company_id'):
                cur_s = str(getattr(obj, 'company_id', None) or '').strip()
                if cur_s and cur_s != cid:
                    raise PermissionError('Cross-tenant update blocked')

        for obj in list(getattr(sess, 'deleted', []) or []):
            if hasattr(obj, 'company_id'):
                cur_s = str(getattr(obj, 'company_id', None) or '').strip()
                if cur_s and cur_s != cid:
                    raise PermissionError('Cross-tenant delete blocked')

    _SQLITE_TENANT_GUARDS_CONFIGURED = True
