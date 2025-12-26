from flask import g, has_request_context, request, session
from sqlalchemy import event, text
from sqlalchemy.orm import Session, with_loader_criteria

from app import db
from app.tenancy import ensure_request_context, resolve_company_slug


_SQLITE_TENANT_GUARDS_CONFIGURED = False


def apply_rls_context(*, is_login: bool, login_email: str | None = None) -> None:
    if not has_request_context():
        return

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

    slug = resolve_company_slug() or ''

    pre_company_id = ''
    is_admin = '1' if (session.get('auth_is_zentral_admin') == '1') else '0'
    if is_admin == '1':
        imp = session.get('impersonate_company_id')
        pre_company_id = str(imp) if imp else ''
    else:
        cid = session.get('auth_company_id')
        pre_company_id = str(cid) if cid else ''
    is_login_v = '1' if is_login else '0'

    login_email_v = (str(login_email or '').strip().lower() if is_login else '')

    try:
        db.session.execute(
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
            {
                'slug': slug,
                'cid': pre_company_id,
                'is_admin': is_admin,
                'is_login': is_login_v,
                'login_email': login_email_v,
            },
        )

        ensure_request_context()

        cid2 = str(getattr(g, 'company_id', None) or '')
        db.session.execute(
            text("SELECT set_config('app.current_company_id', :cid, true)"),
            {'cid': cid2},
        )
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
        raise


def configure_sqlite_tenant_guards() -> None:
    global _SQLITE_TENANT_GUARDS_CONFIGURED
    if _SQLITE_TENANT_GUARDS_CONFIGURED:
        return

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
            if not str(db.engine.url.drivername).startswith('sqlite'):
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
        if not has_request_context():
            return
        try:
            if not str(db.engine.url.drivername).startswith('sqlite'):
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
