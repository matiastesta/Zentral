from datetime import date as dt_date, datetime, timedelta
from io import BytesIO
from typing import Any, Dict, List, Optional
import re
import uuid
import json
import math
import os
import unicodedata

from flask import abort, current_app, g, jsonify, render_template, request, send_file, url_for
from flask_login import login_required, current_user

from sqlalchemy import func, inspect, text, and_, or_, false
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import selectinload, joinedload

from app import db
from app.models import BusinessSettings, CashCount, Category, Customer, Employee, Expense, FileAsset, Installment, InstallmentPlan, InventoryLot, InventoryMovement, Product, Sale, SaleItem, SalePayment, SalesHistoryUserConfig, User, UserTableColumnPrefs
from app.permissions import module_required, module_required_any
from app.sales import bp


def _dt_to_ms(dt):
    if not dt:
        return 0

    try:
        return int(dt.timestamp() * 1000)
    except Exception:
        current_app.logger.exception('Failed to convert datetime to milliseconds')
        return 0


def _default_sales_history_columns_config() -> dict:
    # Columns order for Ventas -> Historial de movimientos.
    return {
        'columns': [
            'ticket',
            'fecha',
            'cliente',
            'empleado',
            'total',
            'pago',
            'tipo',
        ]
    }


_VENTAS_HISTORIAL_MODULE_KEY = 'ventas_historial_movimientos'
_VENTAS_HISTORIAL_ALLOWED_COLUMNS = {
    'ticket',
    'fecha',
    'cliente',
    'empleado',
    'total',
    'pago',
    'tipo',
    'productos',
    'cantidad_items',
    'descuento',
    'recargo',
    'regalo',
    'margen_bruto',
    'cmv',
    'observaciones',
    'cliente_direccion',
    'cliente_email',
    'cliente_telefono',
    'cliente_cumple',
    'cliente_clasificacion',
    'cliente_saldo_cc',
}


def _default_inventory_current_columns() -> list[str]:
    return [
        'imagen',
        'producto',
        'categoria',
        'stock',
        'costo_prom',
        'valor_total',
        'codigo_interno',
        'estado',
    ]


_INVENTORY_CURRENT_MODULE_KEY = 'inventory_historial_movimientos_current'
_INVENTORY_CURRENT_ALLOWED_COLUMNS = {
    'imagen',
    'producto',
    'categoria',
    'stock',
    'costo_prom',
    'valor_total',
    'codigo_interno',
    'estado',
    'precio',
    'codigo_barras',
    'proveedor',
    'descripcion',
    'vencimiento',
    'margen',
    'margen_pct',
    'notas',
}


def _filter_ventas_historial_visible_columns_for_load(cols_in: list) -> list[str]:
    cols = cols_in if isinstance(cols_in, list) else []
    out: list[str] = []
    for c in cols:
        k = str(c or '').strip().lower()
        if not k:
            continue
        if k not in _VENTAS_HISTORIAL_ALLOWED_COLUMNS:
            continue
        if k not in out:
            out.append(k)
    if len(out) > 10:
        out = out[:10]
    if len(out) < 1:
        out = _default_ventas_historial_visible_columns()
    return out


def _default_ventas_historial_visible_columns() -> list[str]:
    return ['ticket', 'fecha', 'cliente', 'empleado', 'total', 'pago', 'tipo']


def _filter_inventory_current_visible_columns_for_load(cols_in: list) -> list[str]:
    cols = cols_in if isinstance(cols_in, list) else []
    out: list[str] = []
    for c in cols:
        k = str(c or '').strip().lower()
        if not k:
            continue
        if k not in _INVENTORY_CURRENT_ALLOWED_COLUMNS:
            continue
        if k not in out:
            out.append(k)
    if len(out) > 10:
        out = out[:10]
    if len(out) < 1:
        out = _default_inventory_current_columns()
    return out


def _validate_inventory_current_visible_columns(cols_in: list) -> tuple[list[str] | None, str | None]:
    cols = cols_in if isinstance(cols_in, list) else []
    out: list[str] = []
    unknown: list[str] = []
    for c in cols:
        k = str(c or '').strip().lower()
        if not k:
            continue
        if k not in _INVENTORY_CURRENT_ALLOWED_COLUMNS:
            unknown.append(k)
            continue
        if k not in out:
            out.append(k)
    if unknown:
        return None, 'Columnas desconocidas.'
    if len(out) < 1:
        return None, 'Debe haber al menos 1 columna.'
    if len(out) > 10:
        return None, 'Máximo 10 columnas.'
    return out, None


def _ensure_user_table_column_prefs_table() -> None:
    try:
        engine = db.engine
        if str(engine.url.drivername).startswith('sqlite'):
            try:
                db.metadata.create_all(bind=engine, tables=[UserTableColumnPrefs.__table__])
            except Exception:
                pass
            return
        insp = inspect(engine)
        tables = set(insp.get_table_names() or [])
        if 'user_table_column_prefs' in tables:
            return
        with engine.begin() as conn:
            conn.execute(text(
                """
                CREATE TABLE IF NOT EXISTS user_table_column_prefs (
                    id SERIAL PRIMARY KEY,
                    company_id VARCHAR(36) NOT NULL,
                    user_id INTEGER NOT NULL,
                    module_key VARCHAR(128) NOT NULL,
                    visible_columns_json TEXT NOT NULL DEFAULT '[]',
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            ))
            conn.execute(text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_user_table_column_prefs_company_user_module
                ON user_table_column_prefs (company_id, user_id, module_key)
                """
            ))
    except Exception:
        try:
            current_app.logger.exception('Failed to ensure user_table_column_prefs table')
        except Exception:
            pass


def _validate_ventas_historial_visible_columns(cols_in: list) -> tuple[list[str] | None, str | None]:
    cols = cols_in if isinstance(cols_in, list) else []
    out: list[str] = []
    unknown: list[str] = []
    for c in cols:
        k = str(c or '').strip().lower()
        if not k:
            continue
        if k not in _VENTAS_HISTORIAL_ALLOWED_COLUMNS:
            unknown.append(k)
            continue
        if k not in out:
            out.append(k)

    if unknown:
        return None, 'Columnas desconocidas.'
    if len(out) < 1:
        return None, 'Debe haber al menos 1 columna.'
    if len(out) > 10:
        return None, 'Máximo 10 columnas.'
    return out, None


@bp.get('/api/user-table-column-prefs')
@login_required
@module_required_any('sales', 'movements', 'inventory')
def get_user_table_column_prefs():
    _ensure_user_table_column_prefs_table()
    cid = _company_id()
    uid = int(getattr(current_user, 'id', 0) or 0)
    module_key = str(request.args.get('module_key') or '').strip()
    if not uid:
        return jsonify({'ok': False, 'message': 'no_user'}), 400
    if not module_key:
        return jsonify({'ok': False, 'message': 'module_key requerido'}), 400

    row = (
        db.session.query(UserTableColumnPrefs)
        .execution_options(_sqlite_tenant_guard_applied=True)
        .filter(UserTableColumnPrefs.company_id == (cid or ''), UserTableColumnPrefs.user_id == uid, UserTableColumnPrefs.module_key == module_key)
        .first()
    )

    visible_columns = row.get_visible_columns() if row else []
    if module_key == _VENTAS_HISTORIAL_MODULE_KEY:
        if not visible_columns:
            visible_columns = _default_ventas_historial_visible_columns()
        visible_columns = _filter_ventas_historial_visible_columns_for_load(visible_columns)
    elif module_key == _INVENTORY_CURRENT_MODULE_KEY:
        if not visible_columns:
            visible_columns = _default_inventory_current_columns()
        visible_columns = _filter_inventory_current_visible_columns_for_load(visible_columns)

    return jsonify({
        'ok': True,
        'module_key': module_key,
        'visible_columns': visible_columns,
        'updated_at': _dt_to_ms(getattr(row, 'updated_at', None)) if row else 0,
    })


@bp.post('/api/user-table-column-prefs')
@login_required
@module_required_any('sales', 'movements', 'inventory')
def save_user_table_column_prefs():
    _ensure_user_table_column_prefs_table()
    cid = _company_id()
    uid = int(getattr(current_user, 'id', 0) or 0)
    if not uid:
        return jsonify({'ok': False, 'message': 'no_user'}), 400

    payload = request.get_json(silent=True) or {}
    module_key = str(payload.get('module_key') or '').strip()
    visible_columns_in = payload.get('visible_columns')

    if not module_key:
        return jsonify({'ok': False, 'message': 'module_key requerido'}), 400

    if module_key == _VENTAS_HISTORIAL_MODULE_KEY:
        cols, err = _validate_ventas_historial_visible_columns(visible_columns_in)
        if err:
            return jsonify({'ok': False, 'message': err}), 400
        visible_columns = cols or []
    elif module_key == _INVENTORY_CURRENT_MODULE_KEY:
        cols, err = _validate_inventory_current_visible_columns(visible_columns_in)
        if err:
            return jsonify({'ok': False, 'message': err}), 400
        visible_columns = cols or []
    else:
        visible_columns = []
        if isinstance(visible_columns_in, list):
            for c in visible_columns_in:
                k = str(c or '').strip().lower()
                if k and k not in visible_columns:
                    visible_columns.append(k)
        if not visible_columns:
            return jsonify({'ok': False, 'message': 'Debe haber al menos 1 columna.'}), 400
        if len(visible_columns) > 10:
            return jsonify({'ok': False, 'message': 'Máximo 10 columnas.'}), 400

    row = (
        db.session.query(UserTableColumnPrefs)
        .execution_options(_sqlite_tenant_guard_applied=True)
        .filter(UserTableColumnPrefs.company_id == (cid or ''), UserTableColumnPrefs.user_id == uid, UserTableColumnPrefs.module_key == module_key)
        .first()
    )
    if not row:
        row = UserTableColumnPrefs(company_id=(cid or ''), user_id=uid, module_key=module_key)
        db.session.add(row)
    row.set_visible_columns(visible_columns)
    row.updated_at = datetime.utcnow()

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'message': 'No se pudo guardar la configuración.'}), 400

    return jsonify({
        'ok': True,
        'module_key': module_key,
        'visible_columns': row.get_visible_columns(),
        'updated_at': _dt_to_ms(row.updated_at),
    })


def _ensure_sales_history_user_config_table() -> None:
    try:
        engine = db.engine
        if str(engine.url.drivername).startswith('sqlite'):
            try:
                db.metadata.create_all(bind=engine, tables=[SalesHistoryUserConfig.__table__])
            except Exception:
                pass
            return
        insp = inspect(engine)
        tables = set(insp.get_table_names() or [])
        if 'sales_history_user_config' in tables:
            return
        with engine.begin() as conn:
            conn.execute(text(
                """
                CREATE TABLE IF NOT EXISTS sales_history_user_config (
                    id SERIAL PRIMARY KEY,
                    company_id VARCHAR(36) NOT NULL,
                    user_id INTEGER NOT NULL,
                    config_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            ))
            conn.execute(text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_sales_history_user_config_company_user
                ON sales_history_user_config (company_id, user_id)
                """
            ))
    except Exception:
        try:
            current_app.logger.exception('Failed to ensure sales_history_user_config table')
        except Exception:
            pass


def _get_sales_history_user_config():
    _ensure_sales_history_user_config_table()
    try:
        uid = int(getattr(current_user, 'id', 0) or 0)
    except Exception:
        uid = 0
    cid = _company_id()
    if not uid:
        return None

    # Idempotent: mirror CalendarUserConfig approach (handle legacy UNIQUE(user_id) safely).
    with db.session.no_autoflush:
        try:
            row = db.session.execute(
                text('SELECT id, company_id FROM sales_history_user_config WHERE user_id = :uid LIMIT 1'),
                {'uid': uid},
                execution_options={'_sqlite_tenant_guard_applied': True},
            ).fetchone()
        except Exception:
            row = None

    existing_id = None
    existing_cid = ''
    if row:
        try:
            existing_id = int(row[0])
        except Exception:
            existing_id = None
        try:
            existing_cid = str(row[1] or '').strip()
        except Exception:
            existing_cid = ''

    if existing_id is not None:
        if cid and existing_cid != cid:
            try:
                db.session.execute(
                    text('UPDATE sales_history_user_config SET company_id = :cid WHERE user_id = :uid'),
                    {'cid': cid, 'uid': uid},
                    execution_options={'_sqlite_tenant_guard_applied': True},
                )
                db.session.commit()
            except Exception:
                try:
                    db.session.rollback()
                except Exception:
                    pass

        with db.session.no_autoflush:
            cfg = db.session.get(SalesHistoryUserConfig, existing_id)
            if not cfg:
                cfg = (
                    db.session.query(SalesHistoryUserConfig)
                    .execution_options(_sqlite_tenant_guard_applied=True)
                    .filter(SalesHistoryUserConfig.user_id == uid)
                    .first()
                )
        if cfg:
            return cfg

    cfg = SalesHistoryUserConfig(user_id=uid)
    cfg.company_id = cid or ''
    cfg.set_config(_default_sales_history_columns_config())
    db.session.add(cfg)
    try:
        db.session.commit()
        return cfg
    except IntegrityError:
        try:
            db.session.rollback()
        except Exception:
            pass
        with db.session.no_autoflush:
            cfg2 = (
                db.session.query(SalesHistoryUserConfig)
                .execution_options(_sqlite_tenant_guard_applied=True)
                .filter(SalesHistoryUserConfig.user_id == uid)
                .first()
            )
        if cfg2 and cid:
            try:
                db.session.execute(
                    text('UPDATE sales_history_user_config SET company_id = :cid WHERE user_id = :uid'),
                    {'cid': cid, 'uid': uid},
                    execution_options={'_sqlite_tenant_guard_applied': True},
                )
                db.session.commit()
            except Exception:
                try:
                    db.session.rollback()
                except Exception:
                    pass
        if cfg2:
            return cfg2

        fallback = SalesHistoryUserConfig(user_id=uid, company_id=(cid or ''))
        fallback.set_config(_default_sales_history_columns_config())
        return fallback


@bp.get('/api/sales-history/columns')
@login_required
@module_required_any('sales', 'movements')
def get_sales_history_columns_config():
    cfg = _get_sales_history_user_config()
    data = cfg.get_config() if cfg else {}
    if not isinstance(data, dict):
        data = {}
    if 'columns' not in data:
        data = _default_sales_history_columns_config()
    return jsonify({'ok': True, 'config': data})


@bp.post('/api/sales-history/columns')
@login_required
@module_required_any('sales', 'movements')
def save_sales_history_columns_config():
    payload = request.get_json(silent=True) or {}
    cfg_in = payload.get('config') if isinstance(payload, dict) else None
    cfg_in = cfg_in if isinstance(cfg_in, dict) else {}

    cols = cfg_in.get('columns')
    cols = cols if isinstance(cols, list) else []
    cols_norm = []
    for c in cols:
        k = str(c or '').strip().lower()
        if not k:
            continue
        if k not in cols_norm:
            cols_norm.append(k)

    if not cols_norm:
        cols_norm = _default_sales_history_columns_config().get('columns') or []

    out = {'columns': cols_norm}
    cfg = _get_sales_history_user_config()
    if not cfg:
        return jsonify({'ok': False, 'error': 'no_user'}), 400
    cfg.set_config(out)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'config': out})


def _ensure_cash_count_snapshot_column() -> None:
    try:
        engine = db.engine
        insp = inspect(engine)
        if 'cash_count' not in set(insp.get_table_names() or []):
            return
        cols = {str(c.get('name') or '') for c in (insp.get_columns('cash_count') or [])}
        driver = str(engine.url.drivername)
        stmts = []
        if 'efectivo_calculado_snapshot' not in cols:
            if driver.startswith('sqlite'):
                stmts.append('ALTER TABLE cash_count ADD COLUMN efectivo_calculado_snapshot FLOAT')
            else:
                stmts.append('ALTER TABLE cash_count ADD COLUMN IF NOT EXISTS efectivo_calculado_snapshot DOUBLE PRECISION')
        if 'cash_expected_at_save' not in cols:
            if driver.startswith('sqlite'):
                stmts.append('ALTER TABLE cash_count ADD COLUMN cash_expected_at_save FLOAT')
            else:
                stmts.append('ALTER TABLE cash_count ADD COLUMN IF NOT EXISTS cash_expected_at_save DOUBLE PRECISION')
        if 'last_cash_event_at_save' not in cols:
            if driver.startswith('sqlite'):
                stmts.append('ALTER TABLE cash_count ADD COLUMN last_cash_event_at_save DATETIME')
            else:
                stmts.append('ALTER TABLE cash_count ADD COLUMN IF NOT EXISTS last_cash_event_at_save TIMESTAMP')
        if 'status' not in cols:
            if driver.startswith('sqlite'):
                stmts.append("ALTER TABLE cash_count ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'draft'")
            else:
                stmts.append("ALTER TABLE cash_count ADD COLUMN IF NOT EXISTS status VARCHAR(16) NOT NULL DEFAULT 'draft'")
        if 'done_at' not in cols:
            if driver.startswith('sqlite'):
                stmts.append('ALTER TABLE cash_count ADD COLUMN done_at DATETIME')
            else:
                stmts.append('ALTER TABLE cash_count ADD COLUMN IF NOT EXISTS done_at TIMESTAMP')
        if 'shift_code' not in cols:
            if driver.startswith('sqlite'):
                stmts.append('ALTER TABLE cash_count ADD COLUMN shift_code VARCHAR(16)')
            else:
                stmts.append('ALTER TABLE cash_count ADD COLUMN IF NOT EXISTS shift_code VARCHAR(16)')
        with engine.begin() as conn:
            for sql in stmts:
                conn.execute(text(sql))
            try:
                unique_names = {str((c or {}).get('name') or '') for c in (insp.get_unique_constraints('cash_count') or [])}
            except Exception:
                unique_names = set()
            try:
                index_rows = insp.get_indexes('cash_count') or []
                index_names = {str((c or {}).get('name') or '') for c in index_rows}
            except Exception:
                index_names = set()

            if driver.startswith('sqlite'):
                try:
                    if 'uq_cash_count_company_date_shift' not in index_names:
                        conn.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS uq_cash_count_company_date_shift ON cash_count (company_id, count_date, shift_code)'))
                except Exception:
                    pass
            else:
                if 'uq_cash_count_company_date' in unique_names:
                    try:
                        conn.execute(text('ALTER TABLE cash_count DROP CONSTRAINT IF EXISTS uq_cash_count_company_date'))
                    except Exception:
                        pass
                try:
                    conn.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS uq_cash_count_company_date_shift ON cash_count (company_id, count_date, shift_code)'))
                except Exception:
                    pass
    except Exception:
        try:
            current_app.logger.exception('Failed to ensure cash_count snapshot column')
        except Exception:
            pass


def _resolve_customer_display_name(cid: str, customer_id: str = None, customer_name: str = None) -> str:
    name = str(customer_name or '').strip()
    if name:
        return name
    cust_id = str(customer_id or '').strip()
    if not cust_id:
        return ''
    try:
        row = db.session.query(Customer).filter(Customer.company_id == cid, Customer.id == cust_id).first()
        if row:
            full = (str(getattr(row, 'first_name', '') or '') + ' ' + str(getattr(row, 'last_name', '') or '')).strip()
            if not full:
                full = str(getattr(row, 'name', '') or '').strip()
            return full or cust_id
    except Exception:
        current_app.logger.exception('Failed to resolve customer display name')
    return cust_id


def _products_label_from_sale(sale: Sale) -> str:
    try:
        names = []
        for it in (getattr(sale, 'items', None) or []):
            nm = str(getattr(it, 'product_name', '') or '').strip()
            if nm:
                names.append(nm)
        names = [n for n in names if n]
        if not names:
            return 'Producto —'
        if len(names) == 1:
            return f"Producto {names[0]}"
        return f"Productos {names[0]} (+{len(names) - 1})"
    except Exception:
        return 'Producto —'


def _resolve_unlimited_plan_price(cid: str, plan: InstallmentPlan):
    try:
        is_indef = bool(getattr(plan, 'is_indefinite', False)) or (str(getattr(plan, 'mode', '') or '').strip().lower() == 'indefinite')
    except Exception:
        is_indef = False
    if not is_indef:
        return None

    pid = None
    try:
        sale = getattr(plan, 'sale', None)
        for it in (getattr(sale, 'items', None) or []):
            raw = str(getattr(it, 'product_id', '') or '').strip()
            if raw:
                pid = raw
                break
    except Exception:
        pid = None

    if not pid:
        return None

    try:
        pid_int = int(pid)
    except Exception:
        return None

    try:
        prod = db.session.query(Product).filter(Product.company_id == cid, Product.id == pid_int).first()
    except Exception:
        prod = None
    if not prod:
        return None
    try:
        price = float(getattr(prod, 'sale_price', 0.0) or 0.0)
    except Exception:
        price = 0.0
    if price > 0:
        return float(price)
    return None


def _format_installment_payment_note(plan: InstallmentPlan, inst_row: Installment) -> str:
    try:
        customer = _resolve_customer_display_name(
            cid=str(getattr(plan, 'company_id', '') or '').strip(),
            customer_id=str(getattr(plan, 'customer_id', '') or '').strip() or None,
            customer_name=str(getattr(plan, 'customer_name', '') or '').strip() or None,
        )
    except Exception:
        customer = str(getattr(plan, 'customer_name', '') or '').strip() or str(getattr(plan, 'customer_id', '') or '').strip()

    ticket_origen = str(getattr(plan, 'sale_ticket', '') or '').strip()
    if not ticket_origen:
        try:
            ticket_origen = str(getattr(plan, 'sale_id', '') or '').strip()
        except Exception:
            ticket_origen = ''

    customer_txt = customer or '—'
    ticket_txt = ticket_origen or '—'

    note_products = 'Producto —'
    try:
        sale_id = int(getattr(plan, 'sale_id', 0) or 0)
    except Exception:
        sale_id = 0
    if sale_id > 0:
        try:
            src_sale = db.session.query(Sale).options(selectinload(Sale.items)).filter(Sale.company_id == str(getattr(plan, 'company_id', '') or '').strip(), Sale.id == sale_id).first()
            if src_sale:
                note_products = _products_label_from_sale(src_sale)
        except Exception:
            note_products = 'Producto —'

    is_indef = bool(getattr(plan, 'is_indefinite', False)) or (str(getattr(plan, 'mode', '') or '').strip().lower() == 'indefinite')
    if is_indef:
        try:
            interval_days = int(getattr(plan, 'interval_days', 30) or 30)
        except Exception:
            interval_days = 30
        return f"Venta recurrente – Cuota indefinida – {note_products} – Intervalo {interval_days} días"

    try:
        n = int(getattr(inst_row, 'installment_number', 0) or 0)
    except Exception:
        n = 0
    try:
        pid = int(getattr(plan, 'id', 0) or 0)
    except Exception:
        pid = 0
    try:
        plan_count = int(getattr(plan, 'installments_count', 0) or 0)
    except Exception:
        plan_count = 0
    if plan_count > 0:
        return f"Cobro de cuota {n}/{plan_count} – Ticket #{ticket_txt} – {note_products}"
    return f"Cobro de cuota {n} – Ticket #{ticket_txt} – {note_products}"


def _parse_date_iso(raw, fallback=None):
    s = str(raw or '').strip()
    if not s:
        return fallback
    try:
        return dt_date.fromisoformat(s)
    except Exception:
        current_app.logger.exception('Failed to parse date from iso format')
        return fallback


def _company_id() -> str:
    try:
        cid = str(getattr(g, 'company_id', '') or '').strip()
        if cid:
            return cid
    except Exception:
        cid = ''

    try:
        from app.tenancy import effective_company_id

        cid2 = str(effective_company_id() or '').strip()
        if cid2:
            return cid2
    except Exception:
        pass

    try:
        if getattr(current_user, 'is_authenticated', False):
            cid3 = str(getattr(current_user, 'company_id', '') or '').strip()
            if cid3:
                return cid3
    except Exception:
        pass

    try:
        current_app.logger.warning('sales._company_id: missing company_id for request', extra={'path': str(getattr(request, 'path', '') or '')})
    except Exception:
        pass
    return ''


def _ensure_product_columns_for_sales() -> None:
    """Failsafe para Postgres: asegura columnas mínimas en product para catálogo de ventas.

    Motivo: si Railway no aplicó migraciones, /sales/api/products puede romper con
    UndefinedColumn y el catálogo queda vacío.
    """
    try:
        engine = db.engine
        if str(engine.url.drivername).startswith('sqlite'):
            return

        insp = inspect(engine)
        if 'product' not in set(insp.get_table_names() or []):
            return

        cols = {str(c.get('name') or '') for c in (insp.get_columns('product') or [])}
        stmts = []

        if 'internal_code' not in cols:
            stmts.append('ALTER TABLE product ADD COLUMN IF NOT EXISTS internal_code VARCHAR(64)')
        if 'barcode' not in cols:
            stmts.append('ALTER TABLE product ADD COLUMN IF NOT EXISTS barcode VARCHAR(64)')
        if 'deleted_at' not in cols:
            stmts.append('ALTER TABLE product ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP')
        if 'stock_ilimitado' not in cols:
            stmts.append('ALTER TABLE product ADD COLUMN IF NOT EXISTS stock_ilimitado BOOLEAN NOT NULL DEFAULT FALSE')
        if 'image_filename' not in cols:
            stmts.append('ALTER TABLE product ADD COLUMN IF NOT EXISTS image_filename VARCHAR(255)')
        if 'image_file_id' not in cols:
            stmts.append('ALTER TABLE product ADD COLUMN IF NOT EXISTS image_file_id VARCHAR(64)')

        if not stmts:
            return

        with engine.begin() as conn:
            for sql in stmts:
                conn.execute(text(sql))
    except Exception:
        try:
            current_app.logger.exception('Failed to ensure product columns for sales')
        except Exception:
            pass


def _ensure_sale_employee_columns() -> None:
    """Failsafe para Postgres: asegura columnas employee_id/employee_name en sale.

    Motivo: si Railway no aplicó migraciones, los endpoints de ventas rompen con
    UndefinedColumn. Esto es idempotente y seguro.
    """
    try:
        engine = db.engine
        if str(engine.url.drivername).startswith('sqlite'):
            return

        insp = inspect(engine)
        if 'sale' not in set(insp.get_table_names() or []):
            return

        cols = {str(c.get('name') or '') for c in (insp.get_columns('sale') or [])}
        stmts = []
        if 'employee_id' not in cols:
            stmts.append('ALTER TABLE sale ADD COLUMN IF NOT EXISTS employee_id VARCHAR(64)')
        if 'employee_name' not in cols:
            stmts.append('ALTER TABLE sale ADD COLUMN IF NOT EXISTS employee_name VARCHAR(255)')
        if 'is_installments' not in cols:
            stmts.append('ALTER TABLE sale ADD COLUMN IF NOT EXISTS is_installments BOOLEAN NOT NULL DEFAULT FALSE')
        if not stmts:
            return

        # Ejecutar DDL fuera de la sesión para evitar interferir con transacciones del request.
        with engine.begin() as conn:
            for sql in stmts:
                conn.execute(text(sql))
    except Exception:
        current_app.logger.exception('Failed to ensure sale employee columns')


def _ensure_sale_surcharge_columns() -> None:
    """Failsafe para Postgres: asegura columnas general_surcharge_pct/surcharge_general_amount en sale."""
    try:
        engine = db.engine
        if str(engine.url.drivername).startswith('sqlite'):
            return

        insp = inspect(engine)
        if 'sale' not in set(insp.get_table_names() or []):
            return

        cols = {str(c.get('name') or '') for c in (insp.get_columns('sale') or [])}
        stmts = []
        if 'general_surcharge_pct' not in cols:
            stmts.append('ALTER TABLE sale ADD COLUMN IF NOT EXISTS general_surcharge_pct FLOAT NOT NULL DEFAULT 0')
        if 'surcharge_general_amount' not in cols:
            stmts.append('ALTER TABLE sale ADD COLUMN IF NOT EXISTS surcharge_general_amount FLOAT NOT NULL DEFAULT 0')
        if not stmts:
            return

        with engine.begin() as conn:
            for sql in stmts:
                conn.execute(text(sql))
    except Exception:
        try:
            current_app.logger.exception('Failed to ensure sale surcharge columns')
        except Exception:
            pass


def _ensure_installment_plan_columns() -> None:
    """Failsafe para asegurar columnas de modo indefinido en installment_plan."""
    try:
        engine = db.engine
        insp = inspect(engine)
        try:
            tables = set(insp.get_table_names() or [])
        except Exception:
            tables = set()
        if 'installment_plan' not in tables:
            return

        cols = {str(c.get('name') or '') for c in (insp.get_columns('installment_plan') or [])}
        stmts = []
        if 'is_indefinite' not in cols:
            if str(engine.url.drivername).startswith('sqlite'):
                stmts.append('ALTER TABLE installment_plan ADD COLUMN is_indefinite BOOLEAN')
            else:
                stmts.append('ALTER TABLE installment_plan ADD COLUMN IF NOT EXISTS is_indefinite BOOLEAN')
        if 'amount_per_period' not in cols:
            if str(engine.url.drivername).startswith('sqlite'):
                stmts.append('ALTER TABLE installment_plan ADD COLUMN amount_per_period FLOAT')
            else:
                stmts.append('ALTER TABLE installment_plan ADD COLUMN IF NOT EXISTS amount_per_period FLOAT')
        if 'mode' not in cols:
            if str(engine.url.drivername).startswith('sqlite'):
                stmts.append('ALTER TABLE installment_plan ADD COLUMN mode VARCHAR(16)')
            else:
                stmts.append('ALTER TABLE installment_plan ADD COLUMN IF NOT EXISTS mode VARCHAR(16)')
        if not stmts:
            return

        with engine.begin() as conn:
            for s in stmts:
                conn.exec_driver_sql(s)
    except Exception:
        current_app.logger.exception('Failed to ensure installment_plan columns')


def _ensure_installments_tables() -> None:
    try:
        engine = db.engine
        if str(engine.url.drivername).startswith('sqlite'):
            return
        insp = inspect(engine)
        try:
            tables = set(insp.get_table_names() or [])
        except Exception:
            tables = set()
        if 'installment_plan' in tables and 'installment' in tables:
            return
        try:
            from app.models import Installment, InstallmentPlan
            db.metadata.create_all(bind=engine, tables=[InstallmentPlan.__table__, Installment.__table__])
        except Exception:
            current_app.logger.exception('Failed to ensure installments tables')
    except Exception:
        current_app.logger.exception('Failed to ensure installments tables')


def _ensure_sale_payments_table() -> None:
    try:
        engine = db.engine
        insp = inspect(engine)
        try:
            tables = set(insp.get_table_names() or [])
        except Exception:
            tables = set()
        if 'sale_payment' in tables:
            return
        try:
            db.metadata.create_all(bind=engine, tables=[SalePayment.__table__])
            # Backfill mínimo: ventas viejas sin registros en sale_payment.
            # Solo en Postgres; en sqlite se deja sin backfill.
            try:
                if not str(engine.url.drivername).startswith('sqlite'):
                    with engine.begin() as conn:
                        conn.execute(text(
                            """
                            INSERT INTO sale_payment (company_id, sale_id, method, amount, created_at)
                            SELECT
                                s.company_id,
                                s.id,
                                CASE
                                    WHEN lower(s.payment_method) LIKE '%efectiv%' THEN 'cash'
                                    WHEN lower(s.payment_method) LIKE '%transfer%' THEN 'transfer'
                                    WHEN lower(s.payment_method) LIKE '%debit%' OR lower(s.payment_method) LIKE '%debito%' THEN 'debit'
                                    WHEN lower(s.payment_method) LIKE '%credit%' OR lower(s.payment_method) LIKE '%credito%' THEN 'credit'
                                    ELSE lower(s.payment_method)
                                END AS method,
                                CASE
                                    WHEN COALESCE(s.on_account, false) = true OR COALESCE(s.is_installments, false) = true THEN COALESCE(s.paid_amount, 0)
                                    ELSE COALESCE(s.total, 0)
                                END AS amount,
                                NOW() AS created_at
                            FROM sale s
                            WHERE s.sale_type = 'Venta'
                              AND (
                                  COALESCE(s.on_account, false) = false
                                  OR COALESCE(s.is_installments, false) = true
                                  OR COALESCE(s.paid_amount, 0) > 0.0001
                              )
                              AND NOT EXISTS (
                                  SELECT 1 FROM sale_payment sp
                                  WHERE sp.company_id = s.company_id AND sp.sale_id = s.id
                              )
                            """
                        ))
            except Exception:
                try:
                    current_app.logger.exception('Failed to backfill sale_payment rows')
                except Exception:
                    pass
        except Exception:
            current_app.logger.exception('Failed to ensure sale_payment table')
    except Exception:
        current_app.logger.exception('Failed to ensure sale_payment table')


def _canonical_payment_method_key(raw: str) -> str:
    s = str(raw or '').strip().lower()
    if not s:
        return ''
    try:
        s = unicodedata.normalize('NFD', s)
        s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    except Exception:
        s = str(raw or '').strip().lower()

    if s in {'cash', 'efectivo'} or 'efectiv' in s:
        return 'cash'
    if s in {'transfer', 'transferencia'} or 'transfer' in s:
        return 'transfer'
    if s in {'debit', 'debito', 'débito'} or 'debit' in s:
        return 'debit'
    if s in {'credit', 'credito', 'crédito'} or 'credit' in s:
        return 'credit'
    return s


def _parse_payments_payload(payload: dict) -> list[dict]:
    raw = payload.get('payments')
    arr = raw if isinstance(raw, list) else []
    out: list[dict] = []
    seen: set[str] = set()
    for it in arr:
        d = it if isinstance(it, dict) else {}
        mk = _canonical_payment_method_key(d.get('method'))
        if not mk:
            continue
        if mk in seen:
            raise ValueError('methods_duplicate')
        seen.add(mk)
        amt = _num(d.get('amount'))
        try:
            if not math.isfinite(float(amt)):
                raise ValueError('amount_invalid')
        except Exception:
            raise ValueError('amount_invalid')
        if float(amt) < 0:
            raise ValueError('amount_negative')
        out.append({'method': mk, 'amount': float(amt)})
    return out


def _sum_payments(payments: list[dict]) -> float:
    s = 0.0
    for p in (payments or []):
        try:
            s += float(p.get('amount') or 0.0)
        except Exception:
            continue
    return float(s)


def _has_real_payment_amount(amount: object) -> bool:
    try:
        return float(amount or 0.0) > 0.0001
    except Exception:
        return False


def _should_clear_payment_method_for_sale(*, sale_type: str, on_account: bool, paid_amount: object, is_installments: bool = False) -> bool:
    try:
        st = str(sale_type or '').strip()
    except Exception:
        st = str(sale_type or '')
    if st != 'Venta':
        return False
    if bool(is_installments):
        return False
    return bool(on_account) and (not _has_real_payment_amount(paid_amount))


def _normalize_sale_payment_fields(*, sale_type: str, on_account: bool, paid_amount: object, payment_method: object, payments: list[dict] | None, is_installments: bool = False) -> tuple[str | None, list[dict]]:
    cleaned: list[dict] = []
    for p in (payments or []):
        d = p if isinstance(p, dict) else {}
        mk = _canonical_payment_method_key(d.get('method'))
        amt = _num(d.get('amount'))
        if not mk:
            continue
        if not _has_real_payment_amount(amt):
            continue
        cleaned.append({'method': mk, 'amount': float(amt)})

    if _should_clear_payment_method_for_sale(
        sale_type=sale_type,
        on_account=on_account,
        paid_amount=paid_amount,
        is_installments=is_installments,
    ):
        return None, []

    pm = str(payment_method or '').strip() or None
    if cleaned:
        if len(cleaned) == 1:
            pm = str(cleaned[0].get('method') or '').strip() or pm
        else:
            pm = ' + '.join([str(x.get('method') or '').strip() for x in cleaned if str(x.get('method') or '').strip()]) or pm
    return pm, cleaned


def _ensure_sale_ticket_numbering() -> None:
    try:
        engine = db.engine
        insp = inspect(engine)
        if 'sale' not in set(insp.get_table_names() or []):
            return

        cols = {str(c.get('name') or '') for c in (insp.get_columns('sale') or [])}
        stmts = []
        if 'ticket_number' not in cols:
            if str(engine.url.drivername).startswith('sqlite'):
                stmts.append('ALTER TABLE sale ADD COLUMN ticket_number INTEGER')
            else:
                stmts.append('ALTER TABLE sale ADD COLUMN IF NOT EXISTS ticket_number INTEGER')

        idx_sql = 'CREATE UNIQUE INDEX IF NOT EXISTS uq_sale_company_ticket_number ON sale (company_id, ticket_number)'

        with engine.begin() as conn:
            for sql in stmts:
                try:
                    conn.execute(text(sql))
                except Exception:
                    continue
            try:
                conn.execute(text(idx_sql))
            except Exception:
                pass

        try:
            # Limpiar tickets no numéricos (ej: AJMV-..., AJOP-...) para que no contaminen la secuencia.
            invalid = (
                db.session.query(Sale)
                .filter(Sale.ticket_number.isnot(None))
                .filter(~Sale.ticket.like('#%'))
                .limit(5000)
                .all()
            )
            touched = False
            touched_payments = False
            for r in (invalid or []):
                r.ticket_number = None
                touched = True

            # Los cobros (CobroVenta/CobroCC/CobroCuota) NO deben ocupar ticket_number
            # porque harían que la secuencia de ventas salte de 2 en 2.
            payments = (
                db.session.query(Sale)
                .filter(Sale.sale_type.in_(['CobroVenta', 'CobroCC', 'CobroCuota']))
                .limit(10000)
                .all()
            )
            for r in (payments or []):
                if getattr(r, 'ticket_number', None) is not None:
                    r.ticket_number = None
                    touched = True
                    touched_payments = True

                tk = str(getattr(r, 'ticket', '') or '').strip()
                # Migrar cobros viejos que tienen ticket numérico '#0054' a '#P0054'
                # para no consumir números de venta.
                if tk.startswith('#P'):
                    continue
                if re.match(r'^#\d+$', tk):
                    digits = ''.join([ch for ch in tk[1:] if ch.isdigit()])
                    if digits:
                        candidate = '#P' + str(int(digits)).zfill(4)
                        exists = (
                            db.session.query(Sale.id)
                            .filter(Sale.company_id == getattr(r, 'company_id', None))
                            .filter(Sale.ticket == candidate)
                            .first()
                        )
                        if exists is not None:
                            candidate = candidate + '-' + str(int(getattr(r, 'id', 0) or 0))
                        r.ticket = candidate
                        touched = True
                        touched_payments = True

            if touched_payments:
                db.session.commit()
                touched = False

            # Backfill SOLO para tickets de VENTA con formato estrictamente '#0001' (no '#C0001', no '#P0001').
            missing = (
                db.session.query(Sale)
                .filter(Sale.ticket_number.is_(None))
                .filter(Sale.ticket.like('#%'))
                .filter(~Sale.sale_type.in_(['CobroVenta', 'CobroCC', 'CobroCuota']))
                .limit(5000)
                .all()
            )
            for r in (missing or []):
                tk = str(getattr(r, 'ticket', '') or '').strip()
                if not re.match(r'^#\d+$', tk):
                    continue
                digits = ''.join([ch for ch in tk[1:] if ch.isdigit()])
                if not digits:
                    continue
                try:
                    r.ticket_number = int(digits)
                    touched = True
                except Exception:
                    continue

            if touched:
                db.session.commit()
        except Exception:
            try:
                db.session.rollback()
            except Exception:
                pass
    except Exception:
        current_app.logger.exception('Failed to ensure sale ticket numbering')


def _next_ticket_number(cid: str) -> int:
    company_id = str(cid or '').strip()
    if not company_id:
        return 1
    try:
        mx = (
            db.session.query(func.max(Sale.ticket_number))
            .filter(Sale.company_id == company_id)
            .filter(~Sale.sale_type.in_(['CobroVenta', 'CobroCC', 'CobroCuota']))
            .scalar()
        )
        n = int(mx or 0)
        if n > 0:
            return n + 1
    except Exception:
        pass

    try:
        rows = (
            db.session.query(Sale.ticket)
            .filter(Sale.company_id == company_id)
            .filter(~Sale.sale_type.in_(['CobroVenta', 'CobroCC', 'CobroCuota']))
            .filter(Sale.ticket.like('#%'))
            .all()
        )
        max_n = 0
        for (t,) in (rows or []):
            s = str(t or '').strip()
            if not re.match(r'^#\d+$', s):
                continue
            digits = ''.join([ch for ch in s[1:] if ch.isdigit()])
            if not digits:
                continue
            try:
                max_n = max(max_n, int(digits))
            except Exception:
                continue
        return max_n + 1 if max_n > 0 else 1
    except Exception:
        return 1


def _next_payment_number(cid: str) -> int:
    company_id = str(cid or '').strip()
    if not company_id:
        return 1
    try:
        rows = (
            db.session.query(Sale.ticket)
            .filter(Sale.company_id == company_id)
            .filter(Sale.sale_type.in_(['CobroVenta', 'CobroCC', 'CobroCuota']))
            .filter(Sale.ticket.like('#P%'))
            .all()
        )
        max_n = 0
        for (t,) in (rows or []):
            s = str(t or '').strip()
            m = re.match(r'^#P(\d+)', s)
            if not m:
                continue
            try:
                max_n = max(max_n, int(m.group(1)))
            except Exception:
                continue
        return max_n + 1 if max_n > 0 else 1
    except Exception:
        return 1


def _format_ticket_number(n: int, prefix: str = '') -> str:
    try:
        num = int(n or 0)
    except Exception:
        num = 0
    if num < 0:
        num = 0
    return '#' + str(prefix or '') + str(num).zfill(4)


def _next_change_number(cid: str) -> int:
    company_id = str(cid or '').strip()
    if not company_id:
        return 1
    try:
        rows = (
            db.session.query(Sale.ticket)
            .filter(Sale.company_id == company_id)
            .filter(Sale.ticket.like('#C%'))
            .all()
        )
        max_n = 0
        for (t,) in (rows or []):
            s = str(t or '').strip()
            if not s.startswith('#C'):
                continue
            digits = ''.join([ch for ch in s[2:] if ch.isdigit()])
            if not digits:
                continue
            try:
                max_n = max(max_n, int(digits))
            except Exception:
                continue
        return max_n + 1 if max_n > 0 else 1
    except Exception:
        return 1


def _resolve_employee_fields(*, cid: str, employee_id: str | None, employee_name: str | None):
    eid = str(employee_id or '').strip() or None
    ename = str(employee_name or '').strip() or None

    if not cid:
        return None, ename

    if eid:
        try:
            row = db.session.query(Employee).filter(Employee.company_id == cid, Employee.id == eid).first()
        except Exception:
            row = None
        if row:
            try:
                if hasattr(row, 'active') and not bool(getattr(row, 'active', True)):
                    return None, ename
            except Exception:
                pass
            full_name = (str(getattr(row, 'first_name', '') or '').strip() + ' ' + str(getattr(row, 'last_name', '') or '').strip()).strip()
            name = str(getattr(row, 'name', '') or '').strip() or full_name or 'Empleado'
            return eid, name
    return None, ename


def _tmp_ticket(prefix: str = '') -> str:
    try:
        p = str(prefix or '')
        return f"{p}__tmp__{uuid.uuid4().hex[:10]}"
    except Exception:
        return f"__tmp__{datetime.utcnow().timestamp()}"


def _ticket_from_sale_id(sale_id: int) -> str:
    try:
        n = int(sale_id or 0)
        if n <= 0:
            return '#0000'
        return '#' + str(n).zfill(4)
    except Exception:
        return '#0000'


def _exchange_ticket_from_sale_id(sale_id: int) -> str:
    try:
        n = int(sale_id or 0)
        if n <= 0:
            return '#C0000'
        return '#C' + str(n).zfill(4)
    except Exception:
        return '#C0000'


def _parse_related_from_notes(notes: str):
    try:
        note_rel = str(notes or '').strip()
        if not note_rel:
            return None, ''
        m = re.search(r"Relacionado\s+a\s+(venta|cambio)\s+([^\n\r]+)", note_rel, re.IGNORECASE)
        if not m:
            return None, ''
        kind = str(m.group(1) or '').strip().lower() or None
        tok = str(m.group(2) or '').strip()
        return kind, tok
    except Exception:
        current_app.logger.exception('Failed to parse related reference from notes')
        return None, ''


def _is_tmp_related_ticket(tok: str) -> bool:
    try:
        t = str(tok or '').strip().lower()
        if not t:
            return False
        return ('__tmp__' in t) or ('_tmp_' in t)
    except Exception:
        return False


def _related_type_slug(sale_type: str) -> str:
    t = str(sale_type or '').strip()
    if t == 'Venta':
        return 'sale'
    if t == 'Cambio':
        return 'change'
    return ''


def _build_related_label(this_type: str, related_type: str, related_ticket: str) -> str:
    tt = str(this_type or '').strip()
    rt = str(related_type or '').strip()
    tk = str(related_ticket or '').strip()
    if not tk:
        return ''
    if tt == 'Venta' and rt == 'Cambio':
        return f"Venta → Cambio {tk}"
    if tt == 'Cambio' and rt == 'Venta':
        return f"Cambio → Venta {tk}"
    if rt:
        return f"Relacionado a {rt} {tk}"
    return f"Relacionado {tk}"


def _format_date_dmy(d) -> str:
    try:
        if not d:
            return ''
        return d.strftime('%d/%m/%Y')
    except Exception:
        return ''


def _format_money_ar(amount: float) -> str:
    try:
        n = float(amount or 0.0)
        s = f"{abs(n):,.0f}"
        s = s.replace(',', '.')
        return f"-${s}" if n < 0 else f"${s}"
    except Exception:
        return '$0'


def _fallback_related_summary(related_row: Sale) -> str:
    try:
        if not related_row:
            return ''
        d = _format_date_dmy(getattr(related_row, 'sale_date', None))
        cust = str(getattr(related_row, 'customer_name', '') or '').strip()
        total = _format_money_ar(float(getattr(related_row, 'total', 0.0) or 0.0))
        base = ' – '.join([x for x in [d, cust, total] if x])
        if not base:
            return 'Relacionado'
        rt = str(getattr(related_row, 'sale_type', '') or '').strip()
        if rt == 'Venta':
            return f"Venta relacionada: {base}"
        if rt == 'Cambio':
            return f"Cambio relacionado: {base}"
        return f"Relacionado: {base}"
    except Exception:
        current_app.logger.exception('Failed to build related fallback summary')
        return 'Relacionado'


def _sanitize_notes_for_display(notes: str) -> str:
    try:
        txt = str(notes or '').strip()
        if not txt:
            return ''

        out_lines = []
        for raw_line in txt.splitlines():
            line = str(raw_line or '').strip()
            if not line:
                continue

            mrel = re.search(r"^Relacionado\s+a\s+(venta|cambio)\s+([^\n\r]+)$", line, re.IGNORECASE)
            if mrel:
                tok = str(mrel.group(2) or '').strip()
                if _is_tmp_related_ticket(tok):
                    out_lines.append('Relacionado (no disponible)')
                    continue

            mcc = re.search(r"^(CC\s+(?:cobrada\s+parcialmente|saldada)\s+por)\s+([^\s]+)\s*(\(.*\))$", line, re.IGNORECASE)
            if mcc:
                tok = str(mcc.group(2) or '').strip()
                suffix = str(mcc.group(3) or '').strip()
                if _is_tmp_related_ticket(tok):
                    out_lines.append(f"{mcc.group(1)} {suffix}".replace('  ', ' ').strip())
                    continue

            if _is_tmp_related_ticket(line):
                continue
            line = re.sub(r"\b[A-Z]?__tmp__[a-f0-9]{6,}\b", "", line, flags=re.IGNORECASE)
            line = re.sub(r"\b[A-Z]?_tmp_[a-f0-9]{6,}\b", "", line, flags=re.IGNORECASE)
            line = re.sub(r"\s{2,}", " ", line).strip()
            if line:
                out_lines.append(line)

        return '\n'.join(out_lines).strip()
    except Exception:
        current_app.logger.exception('Failed to sanitize notes')
        return ''


@bp.route('/')
@bp.route('/index')
@login_required
@module_required('sales')
def index():
    try:
        from datetime import date as dt_date
        today = dt_date.today()
        today_dmy = today.strftime('%d/%m/%Y')
    except Exception:
        today_dmy = ''
    business = BusinessSettings.get_for_company(_company_id())
    return render_template('sales/list.html', title='Ventas', today_dmy=today_dmy, business=business)


def _cash_count_shift_enabled(cid: str) -> bool:
    try:
        bs = BusinessSettings.get_for_company(cid)
        return bool(getattr(bs, 'habilitar_doble_turno_arqueo', False))
    except Exception:
        return False


def _normalize_cash_shift(raw_shift, enabled: bool) -> str:
    raw = str(raw_shift or '').strip().lower()
    if enabled:
        if raw in {'turno_2', 'segundo_turno', 'segundo', '2'}:
            return 'turno_2'
        return 'turno_1'
    return 'turno_1'


def _parse_shift_minutes(raw_value: str, default_minutes: int) -> int:
    s = str(raw_value or '').strip()
    try:
        if len(s) == 5 and s[2] == ':':
            hh = int(s[:2])
            mm = int(s[3:5])
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return (hh * 60) + mm
    except Exception:
        pass
    return int(default_minutes)


def _format_shift_hhmm(minutes: int) -> str:
    m = int(minutes or 0) % 1440
    hh = m // 60
    mm = m % 60
    return f'{hh:02d}:{mm:02d}'


def _get_shift_schedule_for_company(cid: str, shift_code: str) -> dict:
    enabled = _cash_count_shift_enabled(cid)
    if not enabled:
        return {
            'enabled': False,
            'shift_code': 'turno_unico',
            'label': 'Turno único',
            'desde': '00:00',
            'hasta': '00:00',
            'display': '24 hs',
            'start_minutes': 0,
            'end_minutes': 0,
        }

    bs = BusinessSettings.get_for_company(cid)
    t1_from = _parse_shift_minutes(getattr(bs, 'arqueo_turno_1_desde', '08:00'), 8 * 60)
    t1_to = _parse_shift_minutes(getattr(bs, 'arqueo_turno_1_hasta', '16:00'), 16 * 60)
    t2_from = _parse_shift_minutes(getattr(bs, 'arqueo_turno_2_desde', '16:00'), 16 * 60)
    t2_to = _parse_shift_minutes(getattr(bs, 'arqueo_turno_2_hasta', '08:00'), 8 * 60)

    normalized = _normalize_cash_shift(shift_code, True)
    if normalized == 'turno_2':
        start_minutes = t2_from
        end_minutes = t2_to
        label = 'Segundo turno'
    else:
        start_minutes = t1_from
        end_minutes = t1_to
        label = 'Primer turno'

    return {
        'enabled': True,
        'shift_code': normalized,
        'label': label,
        'desde': _format_shift_hhmm(start_minutes),
        'hasta': _format_shift_hhmm(end_minutes),
        'display': _format_shift_hhmm(start_minutes) + '–' + _format_shift_hhmm(end_minutes),
        'start_minutes': start_minutes,
        'end_minutes': end_minutes,
    }


def _get_shift_window(cid: str, d: dt_date, shift_code: str) -> tuple[datetime, datetime, dict]:
    info = _get_shift_schedule_for_company(cid, shift_code)
    if not info.get('enabled'):
        start_dt = datetime.combine(d, datetime.min.time())
        end_dt = start_dt + timedelta(days=1)
        return start_dt, end_dt, info

    start_minutes = int(info.get('start_minutes') or 0)
    end_minutes = int(info.get('end_minutes') or 0)
    start_dt = datetime.combine(d, datetime.min.time()) + timedelta(minutes=start_minutes)
    end_dt = datetime.combine(d, datetime.min.time()) + timedelta(minutes=end_minutes)
    if end_minutes <= start_minutes:
        end_dt = end_dt + timedelta(days=1)
    return start_dt, end_dt, info


def _apply_dt_window(q, model_col, start_dt: datetime, end_dt: datetime):
    return q.filter(model_col >= start_dt, model_col < end_dt)


def _compute_related_for_row(row: Sale, cid: str) -> dict:
    """Build related metadata for a Sale row (Venta/Cambio cross-link).

    Kept small and local to avoid refactors: mirrors the logic used in get_sale().
    """
    try:
        kind, tok = _parse_related_from_notes(getattr(row, 'notes', '') or '')
    except Exception:
        kind, tok = None, ''

    rel_row = None
    if tok and (not _is_tmp_related_ticket(tok)):
        try:
            rel_row = db.session.query(Sale).filter(Sale.company_id == cid, Sale.ticket == tok).first()
        except Exception:
            current_app.logger.exception('Failed to load related sale by ticket')
            rel_row = None

    if not rel_row:
        try:
            rt = getattr(row, 'exchange_return_total', None)
            nt = getattr(row, 'exchange_new_total', None)
            if rt is not None and nt is not None:
                qrel = (
                    db.session.query(Sale)
                    .filter(Sale.company_id == cid)
                    .filter(Sale.id != row.id)
                    .filter(Sale.sale_date == row.sale_date)
                    .filter(Sale.exchange_return_total == rt)
                    .filter(Sale.exchange_new_total == nt)
                )
                try:
                    if row.created_by_user_id:
                        qrel = qrel.filter(Sale.created_by_user_id == row.created_by_user_id)
                except Exception:
                    pass
                try:
                    if row.customer_id:
                        qrel = qrel.filter(Sale.customer_id == row.customer_id)
                    elif row.customer_name:
                        qrel = qrel.filter(Sale.customer_name == row.customer_name)
                except Exception:
                    pass
                if row.created_at:
                    try:
                        lo = row.created_at - timedelta(minutes=5)
                        hi = row.created_at + timedelta(minutes=5)
                        qrel = qrel.filter(Sale.created_at >= lo, Sale.created_at <= hi)
                    except Exception:
                        pass
                rel_row = qrel.order_by(Sale.created_at.desc(), Sale.id.desc()).first()
        except Exception:
            current_app.logger.exception('Failed to load related sale by exchange fields')
            rel_row = None

    if rel_row:
        rel_ticket = str(getattr(rel_row, 'ticket', '') or '').strip()
        rel_type = str(getattr(rel_row, 'sale_type', '') or '').strip()
        try:
            rel_status = str(getattr(rel_row, 'status', '') or '').strip().lower()
        except Exception:
            rel_status = ''

        if rel_status in {'voided', 'anulado', 'anulada'}:
            return {
                'ticket': rel_ticket,
                'type': _related_type_slug(rel_type),
                'label': 'Relacionado: Ticket anulado',
                'url': '',
            }
        if _is_tmp_related_ticket(rel_ticket):
            return {
                'ticket': '',
                'type': _related_type_slug(rel_type),
                'label': _fallback_related_summary(rel_row),
                'url': '',
            }
        return {
            'ticket': rel_ticket,
            'type': _related_type_slug(rel_type),
            'label': _build_related_label(getattr(row, 'sale_type', ''), rel_type, rel_ticket),
            'url': '',
        }

    has_rel_hint = bool(tok) or (
        getattr(row, 'exchange_return_total', None) is not None and getattr(row, 'exchange_new_total', None) is not None
    )
    return {
        'ticket': '',
        'type': ('sale' if kind == 'venta' else 'change' if kind == 'cambio' else ''),
        'label': 'Relacionado (no disponible)' if has_rel_hint else '',
        'url': '',
    }


@bp.get('/changes/<int:change_id>')
@login_required
@module_required('sales')
def change_detail(change_id: int):
    """Read-only view for Cambio tickets."""
    cid = _company_id()
    try:
        sid = int(change_id or 0)
    except Exception:
        sid = 0
    if not cid or sid <= 0:
        abort(404)

    row = db.session.query(Sale).filter(Sale.company_id == cid, Sale.id == sid).first()
    if not row:
        abort(404)
    if str(getattr(row, 'sale_type', '') or '').strip() != 'Cambio':
        abort(404)

    related = _compute_related_for_row(row, cid)
    item = _serialize_sale(row, related=related)

    related_item = None
    try:
        rel_ticket = str((related or {}).get('ticket') or '').strip()
        if rel_ticket and (not _is_tmp_related_ticket(rel_ticket)):
            rel_row = db.session.query(Sale).filter(Sale.company_id == cid, Sale.ticket == rel_ticket).first()
            if rel_row is not None:
                rel_meta = _compute_related_for_row(rel_row, cid)
                related_item = _serialize_sale(rel_row, related=rel_meta)
    except Exception:
        related_item = None

    return render_template('sales/change_detail.html', title='Cambio', item=item, related_item=related_item)


@bp.get('/changes/by-ticket/<path:ticket>')
@login_required
@module_required('sales')
def change_detail_by_ticket(ticket):
    """Backward-compatible entrypoint: resolve by ticket and redirect to /changes/<id>."""
    t = str(ticket or '').strip()
    cid = _company_id()
    if not cid or not t:
        abort(404)
    row = db.session.query(Sale).filter(Sale.company_id == cid, Sale.ticket == t).first()
    if not row:
        abort(404)
    if str(getattr(row, 'sale_type', '') or '').strip() != 'Cambio':
        abort(404)
    related = _compute_related_for_row(row, cid)
    item = _serialize_sale(row, related=related)

    related_item = None
    try:
        rel_ticket = str((related or {}).get('ticket') or '').strip()
        if rel_ticket and (not _is_tmp_related_ticket(rel_ticket)):
            rel_row = db.session.query(Sale).filter(Sale.company_id == cid, Sale.ticket == rel_ticket).first()
            if rel_row is not None:
                rel_meta = _compute_related_for_row(rel_row, cid)
                related_item = _serialize_sale(rel_row, related=rel_meta)
    except Exception:
        related_item = None

    return render_template('sales/change_detail.html', title='Cambio', item=item, related_item=related_item)


@bp.post('/changes/<int:change_id>/void')
@login_required
@module_required('sales')
def void_change(change_id: int):
    cid = _company_id()
    try:
        sid = int(change_id or 0)
    except Exception:
        sid = 0
    if not cid or sid <= 0:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    role = str(getattr(current_user, 'role', '') or '').strip()
    if role not in {'admin', 'company_admin', 'zentral_admin'}:
        return jsonify({'ok': False, 'error': 'forbidden', 'message': 'No tenés permisos para anular cambios.'}), 403

    row = (
        db.session.query(Sale)
        .filter(Sale.company_id == cid, Sale.id == sid)
        .with_for_update()
        .first()
    )
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    if str(getattr(row, 'sale_type', '') or '').strip() != 'Cambio':
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    change_ticket = str(getattr(row, 'ticket', '') or '').strip()
    if not change_ticket:
        return jsonify({'ok': False, 'error': 'invalid', 'message': 'Ticket inválido.'}), 400

    res = void_operation(change_ticket)
    if not res.get('ok'):
        return jsonify(res), 400
    return jsonify(res)


def void_operation(ticket: str) -> dict:
    """Anulación bilateral y atómica.

    - Si el ticket es Cambio, anula también la Venta relacionada (si existe).
    - Si el ticket es Venta con Cambio relacionado, anula también el Cambio.
    - Revierte stock de ambos.
    - Limpia relación en notas y exchange fields.
    """

    t = str(ticket or '').strip()
    cid = _company_id()
    if not cid or not t:
        return {'ok': False, 'error': 'not_found'}

    now = datetime.utcnow()
    uid = None
    try:
        uid = int(getattr(current_user, 'id', 0) or 0) or None
    except Exception:
        uid = None

    try:
        role = str(getattr(current_user, 'role', '') or '').strip()
        if role not in {'admin', 'company_admin', 'zentral_admin'}:
            return {'ok': False, 'error': 'forbidden', 'message': 'No tenés permisos para anular tickets.'}
    except Exception:
        return {'ok': False, 'error': 'forbidden', 'message': 'No tenés permisos para anular tickets.'}

    def _is_voided(obj: Sale) -> bool:
        try:
            st = str(getattr(obj, 'status', '') or '').strip().lower()
        except Exception:
            st = ''
        return st in {'voided', 'anulado', 'anulada'}

    def _set_void_fields(obj: Sale) -> None:
        try:
            obj.status = 'voided'
        except Exception:
            pass
        try:
            if hasattr(obj, 'voided_at'):
                obj.voided_at = now
        except Exception:
            pass
        try:
            if hasattr(obj, 'voided_by'):
                obj.voided_by = uid
        except Exception:
            pass

    def _normalize_ref_ticket(tok: str) -> str:
        s = str(tok or '').strip()
        if not s:
            return ''
        return '#' + s.lstrip('#')

    def _extract_ref_from_notes(txt: str) -> str:
        try:
            note = str(txt or '')
        except Exception:
            note = ''
        if not note:
            return ''
        m = re.search(r"Ticket\s*(?:original\s*)?#\s*(#+?\w+)", note, re.IGNORECASE)
        ref = (m.group(1) if (m and m.group(1)) else '').strip()
        return _normalize_ref_ticket(ref) if ref else ''

    def _void_associated_payment_tickets(*, base_ticket: str) -> list[str]:
        """Void Cobro* tickets linked by notes to the given base ticket.

        This keeps Movimientos consistent: no Cobro* active without its Venta.
        """
        base_norm = _normalize_ref_ticket(base_ticket)
        if not base_norm:
            return []
        out: list[str] = []
        try:
            payments = (
                db.session.query(Sale)
                .filter(Sale.company_id == cid)
                .filter(Sale.sale_type.in_(['CobroVenta', 'CobroCC', 'CobroCuota']))
                .filter(Sale.notes.isnot(None))
                .filter(Sale.notes.ilike('%Ticket%'))
                .with_for_update()
                .all()
            )
        except Exception:
            payments = []

        for pay in (payments or []):
            try:
                pt = str(getattr(pay, 'ticket', '') or '').strip()
            except Exception:
                pt = ''
            if not pt:
                continue

            ref = _extract_ref_from_notes(str(getattr(pay, 'notes', '') or ''))
            if not ref or ref != base_norm:
                continue

            # Best-effort revert side-effects for CC/cuotas so the financial state is coherent.
            try:
                st_pay = str(getattr(pay, 'sale_type', '') or '').strip()
            except Exception:
                st_pay = ''
            if st_pay == 'CobroCC':
                try:
                    orig = (
                        db.session.query(Sale)
                        .filter(Sale.company_id == cid, Sale.ticket == base_norm)
                        .with_for_update()
                        .first()
                    )
                    if orig:
                        amt = abs(float(getattr(pay, 'total', 0.0) or 0.0))
                        orig.paid_amount = max(0.0, float(orig.paid_amount or 0.0) - amt)
                        orig.due_amount = max(0.0, float(orig.due_amount or 0.0) + amt)
                        orig.on_account = bool(orig.due_amount and float(orig.due_amount or 0.0) > 0)
                except Exception:
                    current_app.logger.exception('Failed to revert CobroCC side-effects on void')
            if st_pay == 'CobroCuota':
                try:
                    _revert_installment_payment_by_sale_id(cid=cid, paid_sale_id=int(getattr(pay, 'id', 0) or 0))
                except Exception:
                    current_app.logger.exception('Failed to revert CobroCuota side-effects on void')

            if not _is_voided(pay):
                _set_void_fields(pay)
            out.append(pt)
        return out

    def _clean_relation_notes(a: Sale, b_ticket: str) -> None:
        if not a:
            return
        bt = str(b_ticket or '').strip()
        if not bt:
            return
        try:
            txt = str(getattr(a, 'notes', '') or '')
        except Exception:
            txt = ''
        if not txt:
            return
        try:
            txt2 = re.sub(r"^Relacionado\s+a\s+(?:venta|cambio)\s+%s\s*$" % re.escape(bt), "", txt, flags=re.IGNORECASE | re.MULTILINE)
            txt2 = re.sub(r"\n{3,}", "\n\n", txt2).strip() or None
            a.notes = txt2
        except Exception:
            pass

    try:
        # Lock primary ticket
        row = (
            db.session.query(Sale)
            .filter(Sale.company_id == cid, Sale.ticket == t)
            .with_for_update()
            .first()
        )
        if not row:
            return {'ok': False, 'error': 'not_found', 'message': 'Ticket no encontrado.'}

        # Find related ticket (either direction) using existing resolver.
        rel_info = None
        rel_ticket = ''
        related_sale = None
        try:
            rel_info = _compute_related_for_row(row, cid)
            rel_ticket = str((rel_info or {}).get('ticket') or '').strip()
        except Exception:
            rel_ticket = ''

        if rel_ticket and (not _is_tmp_related_ticket(rel_ticket)) and rel_ticket != t:
            related_sale = (
                db.session.query(Sale)
                .filter(Sale.company_id == cid, Sale.ticket == rel_ticket)
                .with_for_update()
                .first()
            )

        # Idempotent: if both are already voided, succeed.
        if _is_voided(row) and (related_sale is None or _is_voided(related_sale)):
            out = [t]
            if related_sale is not None:
                try:
                    rt = str(getattr(related_sale, 'ticket', '') or '').strip()
                    if rt and rt not in out:
                        out.append(rt)
                except Exception:
                    pass
            return {'ok': True, 'voided_tickets': out}

        # Revert inventory for both (only if not already voided).
        if not _is_voided(row):
            _revert_inventory_for_ticket(t)
        if related_sale is not None and (not _is_voided(related_sale)):
            rt = str(getattr(related_sale, 'ticket', '') or '').strip()
            if rt and rt != t:
                _revert_inventory_for_ticket(rt)

        # Clean relation notes both ways (best-effort).
        if related_sale is not None:
            _clean_relation_notes(row, str(getattr(related_sale, 'ticket', '') or '').strip())
            _clean_relation_notes(related_sale, t)
        else:
            # If no related row loaded but notes have relation line, clean generic relation line.
            try:
                txt = str(getattr(row, 'notes', '') or '')
                txt2 = re.sub(r"^Relacionado\s+a\s+(?:venta|cambio)\s+[^\n\r]+\s*$", "", txt, flags=re.IGNORECASE | re.MULTILINE)
                txt2 = re.sub(r"\n{3,}", "\n\n", txt2).strip() or None
                row.notes = txt2
            except Exception:
                pass

        # Clear exchange fields both ways so no residual pairing stays.
        try:
            row.exchange_return_total = None
            row.exchange_new_total = None
        except Exception:
            pass
        try:
            if related_sale is not None:
                related_sale.exchange_return_total = None
                related_sale.exchange_new_total = None
        except Exception:
            pass

        _set_void_fields(row)
        if related_sale is not None:
            _set_void_fields(related_sale)

        # Also void payment tickets (Movimientos) that reference this ticket.
        voided_payments = []
        try:
            st_main = str(getattr(row, 'sale_type', '') or '').strip()
        except Exception:
            st_main = ''
        if st_main not in ('CobroVenta', 'CobroCC', 'CobroCuota'):
            try:
                voided_payments = _void_associated_payment_tickets(base_ticket=t)
            except Exception:
                current_app.logger.exception('Failed to void associated payment tickets')

        db.session.commit()

        out = [t]
        if related_sale is not None:
            try:
                rt = str(getattr(related_sale, 'ticket', '') or '').strip()
                if rt and rt not in out:
                    out.append(rt)
            except Exception:
                pass
        if voided_payments:
            for pt in voided_payments:
                if pt and pt not in out:
                    out.append(pt)
        return {'ok': True, 'voided_tickets': out}

    except Exception as e:
        try:
            db.session.rollback()
        except Exception:
            pass
        try:
            current_app.logger.exception('void_operation failed', extra={'company_id': cid, 'ticket': t})
        except Exception:
            pass
        return {'ok': False, 'error': 'void_failed', 'message': str(e)}


@bp.post('/api/sales/<ticket>/void')
@login_required
@module_required('sales')
def void_sale(ticket):
    t = str(ticket or '').strip()
    res = void_operation(t)
    if not res.get('ok'):
        code = 403 if res.get('error') == 'forbidden' else 404 if res.get('error') == 'not_found' else 400
        return jsonify(res), code
    return jsonify(res)


def _serialize_sale(row: Sale, related: dict | None = None, users_map: dict | None = None, customers_map: dict | None = None, customer_saldo_map: dict | None = None, customer_sales_count_map: dict | None = None, customer_clasificacion_map: dict | None = None, customer_clasificacion_tags_map: dict | None = None, customer_clasificacion_primary_map: dict | None = None, customer_clasificacion_primary_tag_map: dict | None = None, cmv_by_ticket: dict | None = None) -> dict:
    has_venta_libre = False
    venta_libre_count = 0
    try:
        for it in (row.items or []):
            pid = str(getattr(it, 'product_id', '') or '').strip()
            nm = str(getattr(it, 'product_name', '') or '').strip().lower()
            if (not pid) or (nm == 'venta libre'):
                has_venta_libre = True
                venta_libre_count += 1
    except Exception:
        current_app.logger.exception('Failed to compute venta libre flag')
        has_venta_libre = False
        venta_libre_count = 0

    rel = related if isinstance(related, dict) else {}

    emp_id = str(getattr(row, 'employee_id', None) or '').strip()
    emp_name = str(getattr(row, 'employee_name', None) or '').strip()
    try:
        created_by_user_id = int(getattr(row, 'created_by_user_id', 0) or 0) or None
    except Exception:
        created_by_user_id = None

    responsible_id = ''
    responsible_name = '-'
    if emp_id:
        responsible_id = 'e:' + emp_id
        responsible_name = emp_name or 'Empleado'

    cust_id = str(getattr(row, 'customer_id', '') or '').strip() or None
    cust_name_raw = str(getattr(row, 'customer_name', '') or '').strip() or None
    cust_name = _resolve_customer_display_name(str(getattr(row, 'company_id', '') or '').strip(), cust_id, cust_name_raw)

    cust_row = None
    try:
        cust_row = (customers_map or {}).get(str(cust_id or '').strip()) if cust_id else None
    except Exception:
        cust_row = None

    cust_address = ''
    cust_email = ''
    cust_phone = ''
    cust_birth_iso = ''
    try:
        if cust_row is not None:
            cust_address = str(getattr(cust_row, 'address', '') or '').strip()
            cust_email = str(getattr(cust_row, 'email', '') or '').strip()
            cust_phone = str(getattr(cust_row, 'phone', '') or '').strip()
            bd = getattr(cust_row, 'birthday', None)
            if bd:
                cust_birth_iso = bd.isoformat()
    except Exception:
        cust_address = ''
        cust_email = ''
        cust_phone = ''
        cust_birth_iso = ''

    saldo_cc = 0.0
    try:
        if cust_id:
            saldo_cc = float((customer_saldo_map or {}).get(str(cust_id), 0.0) or 0.0)
    except Exception:
        saldo_cc = 0.0

    sales_n = 0
    try:
        if cust_id:
            sales_n = int((customer_sales_count_map or {}).get(str(cust_id), 0) or 0)
    except Exception:
        sales_n = 0

    customer_clasificacion = ''
    customer_clasificacion_tags: list[str] = []
    customer_clasificacion_primary = ''
    customer_clasificacion_primary_tag = ''
    try:
        if cust_id:
            customer_clasificacion = str((customer_clasificacion_map or {}).get(str(cust_id), '') or '').strip()
            raw_tags = (customer_clasificacion_tags_map or {}).get(str(cust_id), [])
            if isinstance(raw_tags, list):
                customer_clasificacion_tags = [str(x or '').strip() for x in raw_tags if str(x or '').strip()]
            customer_clasificacion_primary = str((customer_clasificacion_primary_map or {}).get(str(cust_id), '') or '').strip()
            customer_clasificacion_primary_tag = str((customer_clasificacion_primary_tag_map or {}).get(str(cust_id), '') or '').strip()
    except Exception:
        customer_clasificacion = ''
        customer_clasificacion_tags = []
        customer_clasificacion_primary = ''
        customer_clasificacion_primary_tag = ''

    cmv_total = None
    try:
        tk = str(getattr(row, 'ticket', '') or '').strip()
        if tk and (cmv_by_ticket or None) is not None:
            cmv_total = float((cmv_by_ticket or {}).get(tk, 0.0) or 0.0)
    except Exception:
        cmv_total = None

    costo_total_ticket = cmv_total
    margen_bruto = None
    try:
        if cmv_total is not None:
            margen_bruto = float(getattr(row, 'total', 0.0) or 0.0) - float(cmv_total or 0.0)
    except Exception:
        margen_bruto = None

    display_ticket = str(getattr(row, 'ticket', '') or '').strip()
    try:
        st = str(getattr(row, 'sale_type', '') or '').strip()
        if st in ('CobroVenta', 'CobroCC', 'CobroCuota'):
            txt = str(getattr(row, 'notes', '') or '')
            m = re.search(r"Ticket\s*(?:original\s*)?#\s*(#?\w+)", txt, re.IGNORECASE)
            if m and m.group(1):
                tok = str(m.group(1) or '').strip()
                if tok.startswith('#'):
                    tok = tok[1:]
                if tok:
                    display_ticket = '#' + tok
    except Exception:
        display_ticket = str(getattr(row, 'ticket', '') or '').strip()

    payments_out: list[dict] = []
    try:
        for p in (getattr(row, 'payments', None) or []):
            mk = _canonical_payment_method_key(getattr(p, 'method', None))
            if not mk:
                continue
            amt = float(getattr(p, 'amount', 0.0) or 0.0)
            if not _has_real_payment_amount(amt):
                continue
            payments_out.append({'method': mk, 'amount': amt})
    except Exception:
        payments_out = []
    if _should_clear_payment_method_for_sale(
        sale_type=getattr(row, 'sale_type', ''),
        on_account=bool(getattr(row, 'on_account', False)),
        paid_amount=getattr(row, 'paid_amount', 0.0),
        is_installments=bool(getattr(row, 'is_installments', False)),
    ):
        payments_out = []

    # Para cobros (CobroVenta/CobroCC/CobroCuota), el desglose real suele estar en la venta original.
    # Si el cobro no tiene sale_payment, intentamos leer el ticket referenciado en la nota.
    try:
        st_pay = str(getattr(row, 'sale_type', '') or '').strip()
        if (not payments_out) and st_pay in ('CobroVenta', 'CobroCC', 'CobroCuota'):
            txt = str(getattr(row, 'notes', '') or '')
            mref = re.search(r"Ticket\s*(?:original\s*)?#\s*(#?\w+)", txt, re.IGNORECASE)
            ref_ticket = ''
            if mref and mref.group(1):
                tok = str(mref.group(1) or '').strip()
                if tok.startswith('#'):
                    tok = tok[1:]
                if tok:
                    ref_ticket = '#' + tok
            if ref_ticket:
                cid = str(getattr(row, 'company_id', '') or '').strip()
                src = (
                    db.session.query(Sale)
                    .options(selectinload(Sale.payments))
                    .filter(Sale.company_id == cid, Sale.ticket == ref_ticket)
                    .first()
                )
                if src is not None:
                    tmp: list[dict] = []
                    for p in (getattr(src, 'payments', None) or []):
                        mk = _canonical_payment_method_key(getattr(p, 'method', None))
                        if not mk:
                            continue
                        tmp.append({'method': mk, 'amount': float(getattr(p, 'amount', 0.0) or 0.0)})
                    if tmp:
                        payments_out = tmp
    except Exception:
        pass

    return {
        'id': row.id,
        'ticket': row.ticket,
        'display_ticket': display_ticket,
        'ticket_number': getattr(row, 'ticket_number', None) or None,
        'fecha': row.sale_date.isoformat() if row.sale_date else '',
        'type': row.sale_type,
        'status': row.status,
        'payment_method': (None if _should_clear_payment_method_for_sale(
            sale_type=getattr(row, 'sale_type', ''),
            on_account=bool(getattr(row, 'on_account', False)),
            paid_amount=getattr(row, 'paid_amount', 0.0),
            is_installments=bool(getattr(row, 'is_installments', False)),
        ) else row.payment_method),
        'payments': payments_out,
        'notes': row.notes or '',

        'notes_display': _sanitize_notes_for_display(row.notes or ''),

        'related_label': str(rel.get('label') or ''),
        'related_ticket': str(rel.get('ticket') or ''),
        'related_type': str(rel.get('type') or ''),
        'related_url': str(rel.get('url') or ''),

        'is_gift': bool(getattr(row, 'is_gift', False)),
        'gift_code': (getattr(row, 'gift_code', None) or ''),

        'total': row.total,
        'discount_general_pct': row.discount_general_pct,
        'discount_general_amount': row.discount_general_amount,
        'general_surcharge_pct': float(getattr(row, 'general_surcharge_pct', 0.0) or 0.0),
        'surcharge_general_amount': float(getattr(row, 'surcharge_general_amount', 0.0) or 0.0),
        'cmv_total': cmv_total,
        'costo_total_ticket': costo_total_ticket,
        'margen_bruto': margen_bruto,
        'cmv_incomplete': bool(getattr(row, 'cmv_incomplete', False)),
        'cmv_incomplete_reason': str(getattr(row, 'cmv_incomplete_reason', '') or ''),
        'customer_id': cust_id or '',
        'customer_name': cust_name or '',
        'customer_address': cust_address,
        'customer_email': cust_email,
        'customer_phone': cust_phone,
        'customer_birth_date': cust_birth_iso,
        'customer_clasificacion': customer_clasificacion,
        'customer_clasificacion_tags': customer_clasificacion_tags,
        'customer_clasificacion_primary': customer_clasificacion_primary,
        'customer_clasificacion_primary_tag': customer_clasificacion_primary_tag,
        'customer_saldo_cc': saldo_cc,
        'employee_id': emp_id,
        'employee_name': emp_name,
        'responsible_id': responsible_id,
        'responsible_name': responsible_name,
        'on_account': bool(row.on_account),
        'paid_amount': row.paid_amount,
        'due_amount': row.due_amount,
        'is_installments': bool(getattr(row, 'is_installments', False)),
        'exchange_return_total': row.exchange_return_total,
        'exchange_new_total': row.exchange_new_total,
        'created_at': _dt_to_ms(row.created_at),
        'updated_at': _dt_to_ms(row.updated_at),
        'has_venta_libre': bool(has_venta_libre),
        'venta_libre_count': int(venta_libre_count),
        'items': [
            {
                'id': it.id,
                'product_id': it.product_id,
                'nombre': it.product_name,
                'cantidad': it.qty,
                'precio': it.unit_price,
                'descuento': it.discount_pct,
                'subtotal': it.subtotal,
                'direction': getattr(it, 'direction', 'out') or 'out',
            }
            for it in (row.items or [])
        ],
    }


def _format_currency_ars(v) -> str:
    n = _num(v)
    sign = '-' if n < 0 else ''
    n = abs(n)
    s = f"{n:,.2f}"
    s = s.replace(',', 'X').replace('.', ',').replace('X', '.')
    return f"{sign}$ {s}"


def _get_receipt_business_info(cid: str) -> tuple[BusinessSettings | None, str, object | None]:
    bs = None
    try:
        bs = BusinessSettings.get_for_company(cid)
    except Exception:
        bs = None

    business_name = str(getattr(bs, 'name', '') or '').strip() or 'Zentral'
    logo_source = None
    try:
        logo_file_id = str(getattr(bs, 'logo_file_id', '') or '').strip() if bs else ''
        if logo_file_id:
            asset = db.session.query(FileAsset).filter(FileAsset.company_id == cid, FileAsset.id == logo_file_id).first()
            if asset and (str(getattr(asset, 'status', 'active') or 'active') == 'active'):
                endpoint = str(current_app.config.get('R2_ENDPOINT_URL') or '').strip()
                access_key = str(current_app.config.get('R2_ACCESS_KEY_ID') or '').strip()
                secret_key = str(current_app.config.get('R2_SECRET_ACCESS_KEY') or '').strip()
                region = str(current_app.config.get('R2_REGION') or 'auto').strip() or 'auto'
                bucket = str(getattr(asset, 'bucket', '') or '').strip()
                object_key = str(getattr(asset, 'object_key', '') or '').strip()
                if endpoint and access_key and secret_key and bucket and object_key:
                    import boto3
                    from botocore.config import Config as BotoConfig

                    client = boto3.client(
                        's3',
                        endpoint_url=endpoint,
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key,
                        region_name=region,
                        config=BotoConfig(signature_version='s3v4', s3={'addressing_style': 'path'}),
                    )
                    body = client.get_object(Bucket=bucket, Key=object_key)['Body'].read()
                    if body:
                        logo_source = BytesIO(body)
    except Exception:
        logo_source = None

    try:
        if not logo_source and bs and getattr(bs, 'logo_filename', None):
            p = os.path.join(current_app.static_folder, 'uploads', str(bs.logo_filename))
            if os.path.exists(p):
                logo_source = p
    except Exception:
        logo_source = None

    if not logo_source:
        try:
            fallback = os.path.join(current_app.static_folder, 'uploads', 'business_logo.png')
            if os.path.exists(fallback):
                logo_source = fallback
        except Exception:
            logo_source = None

    return bs, business_name, logo_source


def _draw_receipt_logo(c, logo_source, x: float, y: float, size: float, brand_color: str):
    if not logo_source:
        return
    try:
        from reportlab.lib import colors
        from reportlab.lib.utils import ImageReader

        image = ImageReader(logo_source)
        p = c.beginPath()
        p.circle(x + (size / 2), y + (size / 2), size / 2)
        c.saveState()
        c.clipPath(p, stroke=0, fill=0)
        c.drawImage(image, x, y, width=size, height=size, preserveAspectRatio=True, mask='auto')
        c.restoreState()
        c.setStrokeColor(colors.HexColor(brand_color))
        c.circle(x + (size / 2), y + (size / 2), size / 2, stroke=1, fill=0)
    except Exception:
        return


def _clean_ticket_for_filename(ticket: str) -> str:
    raw = str(ticket or '').strip()
    if raw.startswith('#'):
        raw = raw[1:]
    return raw or 'venta'


def _payment_method_label_es(value: str) -> str:
    raw = str(value or '').strip()
    key = raw.lower()
    mapping = {
        'cash': 'Efectivo',
        'efectivo': 'Efectivo',
        'transfer': 'Transferencia',
        'transferencia': 'Transferencia',
        'debit': 'Débito',
        'debito': 'Débito',
        'débito': 'Débito',
        'card_debit': 'Débito',
        'credit': 'Crédito',
        'credito': 'Crédito',
        'crédito': 'Crédito',
        'card_credit': 'Crédito',
        'cc': 'Cuenta corriente',
        'cuenta corriente': 'Cuenta corriente',
        'current_account': 'Cuenta corriente',
        'installments': 'Sistema de cuotas',
        'cuotas': 'Sistema de cuotas',
        'sistema de cuotas': 'Sistema de cuotas',
    }
    return mapping.get(key, raw or '—')


def _payment_methods_label_es(values: list[str]) -> str:
    labels = []
    seen = set()
    for value in (values or []):
        label = _payment_method_label_es(value)
        if not label or label in seen:
            continue
        seen.add(label)
        labels.append(label)
    return ' + '.join(labels) if labels else '—'


def _receipt_brand_color(bs: BusinessSettings | None) -> str:
    color = str(getattr(bs, 'primary_color', '') or '').strip()
    if re.match(r'^#(?:[0-9a-fA-F]{3}){1,2}$', color):
        return color
    return '#0d1067'


def _payment_breakdown_rows(raw_rows: list[dict]) -> list[dict]:
    grouped: dict[str, float] = {}
    order: list[str] = []
    for raw in (raw_rows or []):
        d = raw if isinstance(raw, dict) else {}
        method = str(d.get('method') or '').strip()
        if not method:
            continue
        amount = _num(d.get('amount'))
        key = _canonical_payment_method_key(method) or method.lower()
        if key not in grouped:
            grouped[key] = 0.0
            order.append(key)
        grouped[key] += float(amount or 0.0)
    out = []
    for key in order:
        out.append({
            'method': key,
            'label': _payment_method_label_es(key),
            'amount': float(grouped.get(key, 0.0) or 0.0),
        })
    return out


def _receipt_adjustment_label(base: str, pct: float, amount: float, *, negative: bool = False) -> str:
    label = str(base or '').strip() or 'Ajuste'
    try:
        pct_value = float(pct or 0.0)
    except Exception:
        pct_value = 0.0
    try:
        amount_value = abs(float(amount or 0.0))
    except Exception:
        amount_value = 0.0
    if pct_value > 0.0001:
        pct_txt = f"{pct_value:.2f}".rstrip('0').rstrip('.').replace('.', ',')
        return f"{label} ({pct_txt}%)"
    if amount_value > 0.0001 and negative:
        return f"{label}"
    return label


def _format_currency_signed(value: float, *, positive_sign: str = '', negative_sign: str = '-') -> str:
    n = _num(value)
    if n < 0:
        return f"{negative_sign}{_format_currency_ars(abs(n))}"
    if n > 0 and positive_sign:
        return f"{positive_sign}{_format_currency_ars(n)}"
    return _format_currency_ars(n)


def _resolve_receipt_adjustment_amount(base_amount: float, pct: float, subtotal: float) -> float:
    amount = abs(_num(base_amount))
    pct_value = abs(_num(pct))
    subtotal_value = abs(_num(subtotal))
    if amount > 0.0001:
        return amount
    if pct_value > 0.0001 and subtotal_value > 0.0001:
        return subtotal_value * (pct_value / 100.0)
    return 0.0


def _build_receipt_context_from_sale(row: Sale, bs: BusinessSettings | None) -> dict:
    created_dt = getattr(row, 'created_at', None)
    sale_date = getattr(row, 'sale_date', None)
    date_label = ''
    hour_label = ''
    try:
        if created_dt:
            date_label = created_dt.strftime('%d/%m/%Y')
            hour_label = created_dt.strftime('%H:%M')
        elif sale_date:
            date_label = sale_date.strftime('%d/%m/%Y')
    except Exception:
        date_label = ''
        hour_label = ''

    items = []
    subtotal_before_general = 0.0
    try:
        for it in (getattr(row, 'items', None) or []):
            qty = float(getattr(it, 'qty', 0.0) or 0.0)
            unit_price = float(getattr(it, 'unit_price', 0.0) or 0.0)
            line_base = qty * unit_price
            line_subtotal = float(getattr(it, 'subtotal', 0.0) or 0.0)
            if abs(line_subtotal) < 0.0001 and abs(line_base) > 0.0001:
                line_subtotal = line_base
            subtotal_before_general += max(0.0, line_base)
            items.append({
                'product': str(getattr(it, 'product_name', '') or 'Producto').strip() or 'Producto',
                'qty': qty,
                'unit_price': unit_price,
                'subtotal': line_subtotal,
            })
    except Exception:
        items = []

    payment_values = []
    payment_rows = []
    try:
        for p in (getattr(row, 'payments', None) or []):
            value = str(getattr(p, 'method', '') or '').strip()
            if value:
                payment_values.append(value)
                payment_rows.append({
                    'method': value,
                    'amount': float(getattr(p, 'amount', 0.0) or 0.0),
                })
    except Exception:
        payment_values = []
        payment_rows = []
    if _should_clear_payment_method_for_sale(
        sale_type=getattr(row, 'sale_type', ''),
        on_account=bool(getattr(row, 'on_account', False)),
        paid_amount=getattr(row, 'paid_amount', 0.0),
        is_installments=bool(getattr(row, 'is_installments', False)),
    ):
        payment_values = []
        payment_rows = []
    if not payment_values:
        fallback_payment_method = str(getattr(row, 'payment_method', '') or '').strip()
        payment_values = ([fallback_payment_method] if fallback_payment_method else [])
    if not payment_rows and payment_values:
        fallback_amount = float(getattr(row, 'paid_amount', 0.0) or 0.0)
        if fallback_amount <= 0.0001:
            fallback_amount = float(getattr(row, 'total', 0.0) or 0.0)
        payment_rows = [{'method': payment_values[0], 'amount': fallback_amount}]

    discount_pct = float(getattr(row, 'discount_general_pct', 0.0) or 0.0)
    surcharge_pct = float(getattr(row, 'general_surcharge_pct', 0.0) or 0.0)
    discount_amount = _resolve_receipt_adjustment_amount(getattr(row, 'discount_general_amount', 0.0), discount_pct, subtotal_before_general)
    surcharge_amount = _resolve_receipt_adjustment_amount(getattr(row, 'surcharge_general_amount', 0.0), surcharge_pct, max(0.0, subtotal_before_general - discount_amount))

    return {
        'ticket': str(getattr(row, 'ticket', '') or '').strip(),
        'business_name': str(getattr(bs, 'name', '') or '').strip() or 'Zentral',
        'business_address': str(getattr(bs, 'address', '') or '').strip(),
        'business_phone': str(getattr(bs, 'phone', '') or '').strip(),
        'business_email': str(getattr(bs, 'email', '') or '').strip(),
        'customer_name': str(getattr(row, 'customer_name', '') or '').strip() or 'Consumidor final',
        'date_label': date_label,
        'hour_label': hour_label,
        'items': items,
        'subtotal_amount': subtotal_before_general,
        'discount_amount': discount_amount,
        'discount_pct': discount_pct,
        'surcharge_amount': surcharge_amount,
        'surcharge_pct': surcharge_pct,
        'total_amount': float(getattr(row, 'total', 0.0) or 0.0),
        'payment_method_label': _payment_methods_label_es(payment_values),
        'payment_rows': _payment_breakdown_rows(payment_rows),
        'notes': str(getattr(row, 'notes', '') or '').strip(),
    }


def _build_receipt_context_from_payload(payload: dict, bs: BusinessSettings | None) -> dict:
    sale_date = _parse_date_iso(payload.get('fecha') or payload.get('date'), dt_date.today())
    items = []
    subtotal_before_general = 0.0
    for raw in (payload.get('items') if isinstance(payload.get('items'), list) else []):
        d = raw if isinstance(raw, dict) else {}
        qty = _num(d.get('cantidad') if d.get('cantidad') is not None else d.get('qty'))
        unit_price = _num(d.get('precio') if d.get('precio') is not None else d.get('unit_price'))
        line_base = qty * unit_price
        line_subtotal = _num(d.get('subtotal'))
        if abs(line_subtotal) < 0.0001 and abs(line_base) > 0.0001:
            discount_pct = _num(d.get('descuento') if d.get('descuento') is not None else d.get('discount_pct'))
            line_subtotal = line_base * (1 - (max(0.0, min(100.0, discount_pct)) / 100.0))
        subtotal_before_general += max(0.0, line_base)
        items.append({
            'product': str(d.get('nombre') or d.get('product_name') or 'Producto').strip() or 'Producto',
            'qty': qty,
            'unit_price': unit_price,
            'subtotal': line_subtotal,
        })

    payment_values = []
    payment_rows = []
    for p in (payload.get('payments') if isinstance(payload.get('payments'), list) else []):
        if not isinstance(p, dict):
            continue
        value = str(p.get('method') or '').strip()
        if value:
            payment_values.append(value)
            payment_rows.append({'method': value, 'amount': _num(p.get('amount'))})
    if _should_clear_payment_method_for_sale(
        sale_type=str(payload.get('type') or 'Venta').strip(),
        on_account=bool(payload.get('on_account')),
        paid_amount=_num(payload.get('paid_amount')),
        is_installments=bool((payload.get('installments') if isinstance(payload.get('installments'), dict) else {}).get('enabled')),
    ):
        payment_values = []
        payment_rows = []
    if not payment_values:
        fallback_payment_method = str(payload.get('payment_method') or '').strip()
        payment_values = ([fallback_payment_method] if fallback_payment_method else [])
    if not payment_rows and payment_values:
        fallback_amount = _num(payload.get('paid_amount'))
        if fallback_amount <= 0.0001:
            fallback_amount = _num(payload.get('total'))
        payment_rows = [{'method': payment_values[0], 'amount': fallback_amount}]

    discount_pct = _num(payload.get('discount_general_pct'))
    surcharge_pct = _num(payload.get('surcharge_general_pct') if payload.get('surcharge_general_pct') is not None else payload.get('general_surcharge_pct'))
    discount_amount = _resolve_receipt_adjustment_amount(payload.get('discount_general_amount'), discount_pct, subtotal_before_general)
    surcharge_amount = _resolve_receipt_adjustment_amount(payload.get('surcharge_general_amount'), surcharge_pct, max(0.0, subtotal_before_general - discount_amount))

    return {
        'ticket': str(payload.get('ticket') or '').strip(),
        'business_name': str(getattr(bs, 'name', '') or '').strip() or 'Zentral',
        'business_address': str(getattr(bs, 'address', '') or '').strip(),
        'business_phone': str(getattr(bs, 'phone', '') or '').strip(),
        'business_email': str(getattr(bs, 'email', '') or '').strip(),
        'customer_name': str(payload.get('customer_name') or '').strip() or 'Consumidor final',
        'date_label': sale_date.strftime('%d/%m/%Y') if sale_date else '',
        'hour_label': datetime.now().strftime('%H:%M'),
        'items': items,
        'subtotal_amount': subtotal_before_general,
        'discount_amount': discount_amount,
        'discount_pct': discount_pct,
        'surcharge_amount': surcharge_amount,
        'surcharge_pct': surcharge_pct,
        'total_amount': _num(payload.get('total')),
        'payment_method_label': _payment_methods_label_es(payment_values),
        'payment_rows': _payment_breakdown_rows(payment_rows),
        'notes': str(payload.get('notes') or '').strip(),
    }


def _render_sale_receipt_pdf(receipt: dict, bs: BusinessSettings | None, logo_path: object | None):
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfgen import canvas

    brand_color = _receipt_brand_color(bs)

    def _wrap_text(text: str, font_name: str, font_size: int, max_width: float) -> list[str]:
        words = str(text or '').replace('\n', ' ').split()
        if not words:
            return ['']
        lines = []
        cur = ''
        for w in words:
            cand = (cur + ' ' + w).strip() if cur else w
            try:
                cand_w = pdfmetrics.stringWidth(cand, font_name, font_size)
            except Exception:
                cand_w = len(cand) * (font_size * 0.5)
            if cand_w <= max_width:
                cur = cand
            else:
                if cur:
                    lines.append(cur)
                cur = w
        if cur:
            lines.append(cur)
        return lines or ['']

    def _qty_label(value: float) -> str:
        try:
            if abs(float(value) - int(float(value))) < 0.001:
                return str(int(float(value)))
        except Exception:
            pass
        return f"{float(value):.2f}".replace('.', ',')

    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    margin = 16 * mm
    usable_width = width - (margin * 2)
    y = height - margin
    footer_y = 17 * mm

    def _new_page():
        nonlocal y
        c.showPage()
        y = height - margin

    def _ensure_space(required: float):
        nonlocal y
        if y < footer_y + required:
            _new_page()

    header_bottom = y - 28 * mm
    logo_size = 24 * mm
    logo_center_y = y - 11 * mm
    text_x = margin + logo_size + 6 * mm

    _draw_receipt_logo(c, logo_path, margin, logo_center_y - (logo_size / 2), logo_size, brand_color)

    c.setFillColor(colors.HexColor('#111827'))
    c.setFont('Helvetica-Bold', 15)
    c.drawString(text_x, y - 6 * mm, str(receipt.get('business_name') or 'Zentral'))
    c.setFont('Helvetica', 10)
    c.setFillColor(colors.HexColor(brand_color))
    c.drawString(text_x, y - 12 * mm, 'Comprobante de venta')

    right_x = width - margin
    c.setFillColor(colors.HexColor('#6b7280'))
    c.setFont('Helvetica-Bold', 9)
    c.drawRightString(right_x - 18 * mm, y - 7 * mm, 'Fecha:')
    c.drawRightString(right_x - 18 * mm, y - 13 * mm, 'Hora:')
    c.setFont('Helvetica', 9)
    c.setFillColor(colors.HexColor('#111827'))
    c.drawRightString(right_x, y - 7 * mm, str(receipt.get('date_label') or '—'))
    c.drawRightString(right_x, y - 13 * mm, str(receipt.get('hour_label') or '—'))

    c.setStrokeColor(colors.HexColor(brand_color))
    c.setLineWidth(0.8)
    c.line(margin, header_bottom, width - margin, header_bottom)
    y = header_bottom - 8 * mm

    c.setFont('Helvetica-Bold', 10)
    c.setFillColor(colors.HexColor('#111827'))
    c.drawString(margin, y, 'Datos del negocio')
    y -= 5 * mm
    c.setFont('Helvetica', 8.8)
    business_lines = [
        str(receipt.get('business_name') or '').strip(),
        ('Dirección: ' + str(receipt.get('business_address') or '').strip()) if receipt.get('business_address') else '',
        ('Teléfono: ' + str(receipt.get('business_phone') or '').strip()) if receipt.get('business_phone') else '',
        ('Email: ' + str(receipt.get('business_email') or '').strip()) if receipt.get('business_email') else '',
    ]
    for line in business_lines:
        if not line:
            continue
        c.setFillColor(colors.HexColor('#374151'))
        for wrapped_line in _wrap_text(str(line), 'Helvetica', 8.8, usable_width):
            c.drawString(margin, y, wrapped_line)
            y -= 4.6 * mm
    c.setStrokeColor(colors.HexColor('#e5e7eb'))
    c.setLineWidth(0.5)
    c.line(margin, y + 1.5 * mm, width - margin, y + 1.5 * mm)
    y -= 3 * mm

    c.setFont('Helvetica-Bold', 10)
    c.setFillColor(colors.HexColor('#111827'))
    c.drawString(margin, y, 'Datos del cliente')
    y -= 5 * mm
    c.setFont('Helvetica', 8.8)
    c.setFillColor(colors.HexColor('#374151'))
    c.drawString(margin, y, 'Cliente: ' + str(receipt.get('customer_name') or 'Consumidor final'))
    y -= 8 * mm

    table_x = margin
    table_w = usable_width
    product_col_w = table_w * 0.50
    qty_col_w = table_w * 0.13
    unit_col_w = table_w * 0.19
    subtotal_col_w = table_w - product_col_w - qty_col_w - unit_col_w
    product_text_w = product_col_w - 3 * mm
    qty_right_x = table_x + product_col_w + qty_col_w - 1 * mm
    unit_right_x = table_x + product_col_w + qty_col_w + unit_col_w - 1 * mm
    subtotal_right_x = table_x + table_w

    _ensure_space(70 * mm)
    c.setFont('Helvetica-Bold', 8.7)
    c.setFillColor(colors.HexColor('#111827'))
    c.drawString(table_x, y, 'Producto')
    c.drawRightString(qty_right_x, y, 'Cantidad')
    c.drawRightString(unit_right_x, y, 'Precio unitario')
    c.drawRightString(subtotal_right_x, y, 'Subtotal')
    y -= 2.5 * mm
    c.setStrokeColor(colors.HexColor(brand_color))
    c.line(table_x, y, table_x + table_w, y)
    y -= 5 * mm

    c.setFont('Helvetica', 8.7)
    c.setFillColor(colors.HexColor('#111827'))
    for item in (receipt.get('items') or []):
        wrapped = _wrap_text(str(item.get('product') or ''), 'Helvetica', 8.7, product_text_w)
        row_h = max(5.5 * mm, len(wrapped) * 4.2 * mm)
        _ensure_space(row_h + 8 * mm)
        line_y = y
        for line in wrapped:
            c.drawString(table_x, line_y, line)
            line_y -= 4.2 * mm
        c.drawRightString(qty_right_x, y, _qty_label(float(item.get('qty') or 0.0)))
        c.drawRightString(unit_right_x, y, _format_currency_ars(item.get('unit_price')))
        c.drawRightString(subtotal_right_x, y, _format_currency_ars(item.get('subtotal')))
        y -= row_h
        c.setStrokeColor(colors.HexColor('#e5e7eb'))
        c.setLineWidth(0.4)
        c.line(table_x, y + 2 * mm, table_x + table_w, y + 2 * mm)
        y -= 2.5 * mm

    summary_w = min(64 * mm, usable_width * 0.42)
    summary_x = width - margin - summary_w
    summary_h = 31 * mm
    _ensure_space(summary_h + 14 * mm)
    summary_top = y - 3 * mm
    c.setStrokeColor(colors.HexColor('#e5e7eb'))
    c.roundRect(summary_x, summary_top - summary_h, summary_w, summary_h, 3 * mm, stroke=1, fill=0)
    c.setFont('Helvetica-Bold', 9)
    c.setFillColor(colors.HexColor('#111827'))
    c.drawString(summary_x + 3 * mm, summary_top - 4 * mm, 'Resumen de la venta')
    c.setStrokeColor(colors.HexColor(brand_color))
    c.line(summary_x + 3 * mm, summary_top - 6 * mm, summary_x + summary_w - 3 * mm, summary_top - 6 * mm)
    sy = summary_top - 11 * mm
    rows = [
        ('Subtotal', receipt.get('subtotal_amount')),
        (_receipt_adjustment_label('Descuento aplicado', receipt.get('discount_pct'), receipt.get('discount_amount'), negative=True), -abs(_num(receipt.get('discount_amount')))),
        (_receipt_adjustment_label('Recargo aplicado', receipt.get('surcharge_pct'), receipt.get('surcharge_amount')), abs(_num(receipt.get('surcharge_amount')))),
    ]
    c.setFont('Helvetica', 8.5)
    c.setFillColor(colors.HexColor('#374151'))
    for label, value in rows:
        c.drawString(summary_x + 3 * mm, sy, label)
        c.drawRightString(summary_x + summary_w - 3 * mm, sy, _format_currency_signed(value, positive_sign='+', negative_sign='-'))
        sy -= 5 * mm
    c.setFont('Helvetica-Bold', 10)
    c.setFillColor(colors.HexColor(brand_color))
    c.drawString(summary_x + 3 * mm, sy - 1 * mm, 'TOTAL')
    c.drawRightString(summary_x + summary_w - 3 * mm, sy - 1 * mm, _format_currency_ars(receipt.get('total_amount')))

    y = summary_top - summary_h - 7 * mm

    c.setStrokeColor(colors.HexColor('#e5e7eb'))
    c.line(margin, y, width - margin, y)
    y -= 6 * mm
    c.setFont('Helvetica-Bold', 9)
    c.setFillColor(colors.HexColor('#111827'))
    c.drawString(margin, y, 'Forma de pago')
    y -= 5 * mm
    payment_rows = receipt.get('payment_rows') if isinstance(receipt.get('payment_rows'), list) else []
    c.setFont('Helvetica', 9)
    c.setFillColor(colors.HexColor('#374151'))
    if payment_rows:
        payments_total = 0.0
        for pay in payment_rows:
            _ensure_space(6 * mm)
            label = str(pay.get('label') or '—')
            amount = _num(pay.get('amount'))
            payments_total += amount
            c.drawString(margin, y, f'{label}:')
            c.drawRightString(width - margin, y, _format_currency_ars(amount))
            y -= 4.8 * mm
        _ensure_space(7 * mm)
        c.setStrokeColor(colors.HexColor('#e5e7eb'))
        c.setLineWidth(0.4)
        c.line(margin, y + 1.4 * mm, width - margin, y + 1.4 * mm)
        c.setFont('Helvetica-Bold', 9)
        c.setFillColor(colors.HexColor('#111827'))
        c.drawString(margin, y - 2.2 * mm, 'Total medios de pago:')
        c.drawRightString(width - margin, y - 2.2 * mm, _format_currency_ars(payments_total))
        y -= 7 * mm
    else:
        c.drawString(margin, y, str(receipt.get('payment_method_label') or '—'))
        y -= 4.8 * mm
    y -= 2 * mm

    notes = str(receipt.get('notes') or '').strip()
    if notes:
        c.setFont('Helvetica-Bold', 9)
        c.setFillColor(colors.HexColor('#111827'))
        c.drawString(margin, y, 'Observaciones')
        y -= 5 * mm
        c.setFont('Helvetica', 8.6)
        c.setFillColor(colors.HexColor('#374151'))
        for line in _wrap_text(notes, 'Helvetica', 8.6, usable_width):
            _ensure_space(6 * mm)
            c.drawString(margin, y, line)
            y -= 4.2 * mm

    c.setStrokeColor(colors.HexColor(brand_color))
    c.line(margin, footer_y + 10 * mm, width - margin, footer_y + 10 * mm)
    c.setFont('Helvetica-Bold', 9)
    c.setFillColor(colors.HexColor('#111827'))
    c.drawCentredString(width / 2, footer_y + 5 * mm, 'Gracias por su compra')
    c.setFont('Helvetica', 7.8)
    c.setFillColor(colors.HexColor('#6b7280'))
    c.drawCentredString(width / 2, footer_y, 'Documento generado automáticamente por Zentral – Sistema de Gestión')

    c.save()
    buf.seek(0)
    return buf


@bp.get('/api/sales/<ticket>/receipt-pdf')
@login_required
@module_required('sales')
def download_sale_receipt_pdf(ticket):
    try:
        import reportlab
    except Exception:
        return jsonify({'ok': False, 'error': 'reportlab_missing'}), 400

    t = str(ticket or '').strip()
    cid = _company_id()
    if not t or not cid:
        return jsonify({'ok': False, 'error': 'invalid_ticket'}), 400

    row = (
        db.session.query(Sale)
        .options(selectinload(Sale.items), selectinload(Sale.payments))
        .filter(Sale.company_id == cid, Sale.ticket == t)
        .first()
    )
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    bs, business_name, logo_path = _get_receipt_business_info(cid)
    _ = business_name
    receipt = _build_receipt_context_from_sale(row, bs)
    buf = _render_sale_receipt_pdf(receipt, bs, logo_path)
    filename = f"comprobante_venta_{_clean_ticket_for_filename(t)}.pdf"
    return send_file(buf, mimetype='application/pdf', as_attachment=True, download_name=filename)


@bp.post('/api/sales/receipt-pdf/preview')
@login_required
@module_required('sales')
def preview_sale_receipt_pdf():
    try:
        import reportlab
    except Exception:
        return jsonify({'ok': False, 'error': 'reportlab_missing'}), 400

    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    payload = request.get_json(silent=True) or {}
    items = payload.get('items') if isinstance(payload.get('items'), list) else []
    if not items:
        return jsonify({'ok': False, 'error': 'items_required'}), 400

    bs, _, logo_path = _get_receipt_business_info(cid)
    receipt = _build_receipt_context_from_payload(payload, bs)
    buf = _render_sale_receipt_pdf(receipt, bs, logo_path)
    filename = 'comprobante_venta_previsualizacion.pdf'
    return send_file(buf, mimetype='application/pdf', as_attachment=True, download_name=filename)


_CODIGO_INTERNO_MIN_LEN = 4
_CODIGO_INTERNO_MAX_LEN = 12
_CODIGO_INTERNO_LEN = 8
_CODIGO_INTERNO_PATTERN = re.compile(r'^\S{4,12}$')


def _normalize_codigo_interno(raw: str) -> str:
    try:
        # Keep special characters; only remove whitespace and normalize case.
        return re.sub(r'\s+', '', str(raw or '')).upper()
    except Exception:
        return str(raw or '').strip().upper()


def _is_valid_codigo_interno(code: str) -> bool:
    c = _normalize_codigo_interno(code)
    if not c or len(c) < _CODIGO_INTERNO_MIN_LEN or len(c) > _CODIGO_INTERNO_MAX_LEN:
        return False
    return _CODIGO_INTERNO_PATTERN.match(c) is not None


def _strip_accents(s: str) -> str:
    try:
        return ''.join(ch for ch in unicodedata.normalize('NFKD', s) if not unicodedata.combining(ch))
    except Exception:
        return s


def _alnum_upper(s: str) -> str:
    raw = _strip_accents(str(s or '').upper())
    return re.sub(r'[^A-Z0-9]', '', raw)


def _code3(s: str, fallback: str = 'XXX') -> str:
    base = _alnum_upper(s)
    if not base:
        base = _alnum_upper(fallback)
    return (base + 'XXX')[:3]


def _codigo_prefix_from(name: str, category_name: str) -> str:
    nnn = _code3(name, 'XXX')
    ccc = _code3(category_name or 'GEN', 'GEN')
    return (nnn + ccc)[:6]


def _generate_unique_internal_code_for_sales() -> str:
    for _ in range(30):
        code = uuid.uuid4().hex[:_CODIGO_INTERNO_LEN].upper()
        try:
            exists = db.session.query(Product.id).filter(Product.internal_code == code).first() is not None
        except Exception:
            exists = False
        if not exists:
            return code
    return uuid.uuid4().hex[:_CODIGO_INTERNO_LEN].upper()


def _generate_codigo_interno(company_id: str, name: str, category_name: str = '', used: Optional[set] = None) -> str:
    prefix = _codigo_prefix_from(name, category_name or 'GEN')
    taken = used if isinstance(used, set) else set()
    for n in range(1, 100):
        candidate = prefix + str(n).zfill(2)
        if candidate in taken:
            continue
        try:
            exists_global = db.session.query(Product.id).filter(Product.internal_code == candidate).first() is not None
        except Exception:
            exists_global = False
        if exists_global:
            continue
        if isinstance(taken, set):
            taken.add(candidate)
        return candidate
    try:
        current_app.logger.warning('codigo_interno prefix exhausted for prefix=%s (company_id=%s)', prefix, str(company_id or '').strip())
    except Exception:
        pass
    return _generate_unique_internal_code_for_sales()


def _ensure_codigo_interno_for_sales(rows) -> bool:
    changed = False
    used = set()
    for p in (rows or []):
        if not p:
            continue
        before = str(getattr(p, 'internal_code', '') or '').strip()
        if _is_valid_codigo_interno(before):
            continue
        cat_name = ''
        try:
            cat_name = str(getattr(getattr(p, 'category', None), 'name', '') or '').strip()
        except Exception:
            cat_name = ''
        if not cat_name:
            cat_name = 'GEN'
        next_code = _generate_codigo_interno(str(getattr(p, 'company_id', '') or _company_id()), getattr(p, 'name', '') or '', cat_name, used=used)
        if next_code and before != next_code:
            p.internal_code = next_code
            changed = True
    return changed


def _make_gift_code(ticket: str, items_list: list) -> str:
    t = str(ticket or '').strip()
    digits = ''.join([ch for ch in t if ch.isdigit()])
    if not digits:
        digits = '0000'
    letters = []
    seen = set()
    for it in (items_list if isinstance(items_list, list) else []):
        d = it if isinstance(it, dict) else {}
        name = str(d.get('nombre') or d.get('product_name') or '').strip()
        ch = ''
        for c in name:
            if c.isalnum():
                ch = c.upper()
                break
        if not ch:
            continue
        if ch in seen:
            continue
        seen.add(ch)
        letters.append(ch)
        if len(letters) >= 6:
            break
    suffix = ''.join(letters) or 'X'
    return f"R{digits}{suffix}"


def _image_url(p: Product):
    file_id = str(getattr(p, 'image_file_id', '') or '').strip()
    if file_id:
        try:
            return url_for('files.download_file_api', file_id=file_id)
        except Exception:
            current_app.logger.exception('Failed to generate image url')
            return ''
    filename = str(getattr(p, 'image_filename', '') or '').strip()
    if not filename:
        return ''
    try:
        return url_for('static', filename=f'uploads/{filename}')
    except Exception:
        current_app.logger.exception('Failed to generate image url')
        return ''


def _serialize_product_for_sales(p: Product):
    cat = None
    try:
        if getattr(p, 'category', None):
            cat = {'id': p.category.id, 'name': p.category.name, 'parent_id': p.category.parent_id}
    except Exception:
        current_app.logger.exception('Failed to serialize product category')
        cat = None
    return {
        'id': p.id,
        'name': p.name,
        'codigo_interno': (p.internal_code or ''),
        'internal_code': (p.internal_code or ''),
        'barcode': (getattr(p, 'barcode', None) or ''),
        'supplier_id': (getattr(p, 'primary_supplier_id', None) or ''),
        'supplier_name': (getattr(p, 'primary_supplier_name', None) or ''),
        'primary_supplier_id': (getattr(p, 'primary_supplier_id', None) or ''),
        'primary_supplier_name': (getattr(p, 'primary_supplier_name', None) or ''),
        'description': (p.description or ''),
        'sale_price': p.sale_price,
        'stock_ilimitado': bool(getattr(p, 'stock_ilimitado', False)),
        'costo_unitario_referencia': getattr(p, 'costo_unitario_referencia', None),
        'category_id': p.category_id,
        'category': cat,
        'category_name': (cat.get('name') if isinstance(cat, dict) else ''),
        'active': bool(p.active),
        'image_url': _image_url(p),
    }


def _ensure_sale_cmv_flags_columns() -> None:
    try:
        engine = db.engine
        insp = inspect(engine)
        if 'sale' not in set(insp.get_table_names() or []):
            return
        cols = {str(c.get('name') or '') for c in (insp.get_columns('sale') or [])}
        stmts = []
        if 'cmv_incomplete' not in cols:
            if str(engine.url.drivername).startswith('sqlite'):
                stmts.append('ALTER TABLE sale ADD COLUMN cmv_incomplete BOOLEAN NOT NULL DEFAULT 0')
            else:
                stmts.append('ALTER TABLE sale ADD COLUMN IF NOT EXISTS cmv_incomplete BOOLEAN NOT NULL DEFAULT false')
        if 'cmv_incomplete_reason' not in cols:
            if str(engine.url.drivername).startswith('sqlite'):
                stmts.append('ALTER TABLE sale ADD COLUMN cmv_incomplete_reason VARCHAR(255)')
            else:
                stmts.append('ALTER TABLE sale ADD COLUMN IF NOT EXISTS cmv_incomplete_reason VARCHAR(255)')
        if not stmts:
            return
        with engine.begin() as conn:
            for sql in stmts:
                conn.execute(text(sql))
    except Exception:
        try:
            current_app.logger.exception('Failed to ensure sale CMV flags columns')
        except Exception:
            pass


def _compute_sale_cmv_incomplete(*, cid: str, items: list) -> tuple[bool, str]:
    if not cid:
        return False, ''
    missing_names: list[str] = []
    for it in (items or []):
        d = it if isinstance(it, dict) else {}
        direction = str(d.get('direction') or 'out').strip().lower() or 'out'
        if direction != 'out':
            continue
        pid = _int_or_none(d.get('product_id'))
        if not pid:
            continue
        prod = db.session.get(Product, pid)
        if not prod:
            continue
        if str(getattr(prod, 'company_id', '') or '') != cid:
            continue
        if not bool(getattr(prod, 'stock_ilimitado', False)):
            continue
        ref = getattr(prod, 'costo_unitario_referencia', None)
        if ref is None:
            nm = str(getattr(prod, 'name', '') or '').strip() or str(d.get('nombre') or d.get('product_name') or '').strip() or 'Producto'
            missing_names.append(nm)
    if not missing_names:
        return False, ''
    unique = []
    for n in missing_names:
        if n in unique:
            continue
        unique.append(n)
    reason = 'Sin costo de referencia en: ' + ', '.join(unique[:8])
    return True, reason


def _serialize_lot_for_sales(l: InventoryLot):
    return {
        'id': l.id,
        'product_id': l.product_id,
        'qty_available': l.qty_available,
        'unit_cost': l.unit_cost,
        'received_at': l.received_at.isoformat() if l.received_at else None,
    }


@bp.get('/api/sales')
@login_required
@module_required_any('sales', 'customers', 'movements')
def list_sales():
    _ensure_sale_employee_columns()
    _ensure_sale_surcharge_columns()
    try:
        _ensure_sale_payments_table()
    except Exception:
        # No interrumpir el historial si la tabla no se puede asegurar en runtime.
        pass
    raw_from = (request.args.get('from') or '').strip()
    raw_to = (request.args.get('to') or '').strip()
    raw_payment_method = str(request.args.get('payment_method') or '').strip()
    include_replaced = str(request.args.get('include_replaced') or '').strip() in ('1', 'true', 'True')
    include_voided = str(request.args.get('include_voided') or '').strip() in ('1', 'true', 'True')
    exclude_cc = str(request.args.get('exclude_cc') or '').strip() in ('1', 'true', 'True')
    limit = int(request.args.get('limit') or 300)
    if limit <= 0 or limit > 20000:
        limit = 300

    def _normalize_payment_method_filter(raw: str) -> tuple[str, list[str]]:
        k = str(raw or '').strip().lower()
        if not k:
            return '', []

        aliases = {
            'cash': ['cash', 'efectivo', 'Efectivo'],
            'efectivo': ['cash', 'efectivo', 'Efectivo'],
            'transfer': ['transfer', 'transferencia', 'Transferencia'],
            'transferencia': ['transfer', 'transferencia', 'Transferencia'],
            'debit': ['debit', 'debito', 'débito', 'Débito'],
            'debito': ['debit', 'debito', 'débito', 'Débito'],
            'débito': ['debit', 'debito', 'débito', 'Débito'],
            'credit': ['credit', 'credito', 'crédito', 'Crédito'],
            'credito': ['credit', 'credito', 'crédito', 'Crédito'],
            'crédito': ['credit', 'credito', 'crédito', 'Crédito'],
        }
        if k in aliases:
            vals = aliases[k]
            key = vals[0]
            legacy = [v for v in vals[1:] if v]
            return key, legacy

        # Si ya viene como key esperada (cash/transfer/debit/credit)
        if k in ('cash', 'transfer', 'debit', 'credit'):
            return k, [raw]

        return k, [raw]

    filter_payment_key, filter_payment_legacy = _normalize_payment_method_filter(raw_payment_method)

    d_from = _parse_date_iso(raw_from, None)
    d_to = _parse_date_iso(raw_to, None)

    cid = _company_id()
    if not cid:
        return jsonify({'ok': True, 'items': []})

    try:
        # Auto-heal: cobros viejos (Cobro*) con ticket '#0001' o ticket_number seteado
        # pueden hacer que la secuencia de ventas salte de 2 en 2.
        needs_fix = (
            db.session.query(Sale.id)
            .filter(Sale.company_id == cid)
            .filter(Sale.sale_type.in_(['CobroVenta', 'CobroCC', 'CobroCuota']))
            .filter(
                or_(
                    Sale.ticket_number.isnot(None),
                    and_(Sale.ticket.like('#%'), ~Sale.ticket.like('#P%')),
                )
            )
            .limit(1)
            .first()
        )
        if needs_fix is not None:
            _ensure_sale_ticket_numbering()
    except Exception:
        # No interrumpir el listado si la auto-corrección falla.
        pass
    def _base_query(include_payments: bool):
        opts = [selectinload(Sale.items)]
        if include_payments:
            opts.append(selectinload(Sale.payments))
        return (
            db.session.query(Sale)
            .options(*opts)
            .filter(Sale.company_id == cid)
        )

    q = _base_query(True)
    if d_from:
        q = q.filter(Sale.sale_date >= d_from)
    if d_to:
        q = q.filter(Sale.sale_date <= d_to)
    if not include_replaced:
        q = q.filter(Sale.status != 'Reemplazada')
    if not include_voided:
        q = q.filter(Sale.status.notin_(['Anulado', 'voided']))
    if exclude_cc:
        q = q.filter(Sale.sale_type != 'CobroCC')

    # Filtro por medio de pago:
    # - Nuevo: pertenencia a SalePayment.method (pagos mixtos)
    # - Legacy: Sale.payment_method == X (ventas viejas sin sale_payment)
    if filter_payment_key:
        q = (
            q.outerjoin(
                SalePayment,
                and_(SalePayment.company_id == cid, SalePayment.sale_id == Sale.id),
            )
            .filter(
                or_(
                    SalePayment.method == filter_payment_key,
                    Sale.payment_method == filter_payment_key,
                    Sale.payment_method.in_(filter_payment_legacy) if filter_payment_legacy else false(),
                )
            )
            .distinct()
        )

    q = q.order_by(Sale.sale_date.desc(), Sale.id.desc()).limit(limit)
    try:
        rows = q.all()
    except ProgrammingError as e:
        # Fallback para DBs viejas: si sale_payment no existe todavía, listamos sin payments.
        msg = str(getattr(e, 'orig', e) or '')
        if 'sale_payment' in msg or 'does not exist' in msg or 'UndefinedTable' in msg:
            try:
                q2 = _base_query(False)
                if d_from:
                    q2 = q2.filter(Sale.sale_date >= d_from)
                if d_to:
                    q2 = q2.filter(Sale.sale_date <= d_to)
                if not include_replaced:
                    q2 = q2.filter(Sale.status != 'Reemplazada')
                if not include_voided:
                    q2 = q2.filter(Sale.status.notin_(['Anulado', 'voided']))
                if exclude_cc:
                    q2 = q2.filter(Sale.sale_type != 'CobroCC')
                if filter_payment_key:
                    q2 = q2.filter(
                        or_(
                            Sale.payment_method == filter_payment_key,
                            Sale.payment_method.in_(filter_payment_legacy) if filter_payment_legacy else false(),
                        )
                    )
                rows = q2.order_by(Sale.sale_date.desc(), Sale.id.desc()).limit(limit).all()
                for r in (rows or []):
                    try:
                        r.__dict__['payments'] = []
                    except Exception:
                        pass
            except Exception:
                current_app.logger.exception('Failed to list sales (fallback no payments)', extra={'company_id': cid, 'from': raw_from, 'to': raw_to, 'limit': limit})
                return jsonify({'ok': False, 'error': 'db_error', 'items': []}), 500
        else:
            current_app.logger.exception('Failed to list sales', extra={'company_id': cid, 'from': raw_from, 'to': raw_to, 'limit': limit})
            return jsonify({'ok': False, 'error': 'db_error', 'items': []}), 500
    except Exception:
        current_app.logger.exception('Failed to list sales', extra={'company_id': cid, 'from': raw_from, 'to': raw_to, 'limit': limit})
        return jsonify({'ok': False, 'error': 'db_error', 'items': []}), 500

    cmv_by_ticket: dict[str, float] = {}

    customers_map: dict[str, Customer] = {}
    customer_saldo_map: dict[str, float] = {}
    customer_sales_count_map: dict[str, int] = {}
    customer_clasificacion_map: dict[str, str] = {}
    customer_clasificacion_tags_map: dict[str, list[str]] = {}
    customer_clasificacion_primary_map: dict[str, str] = {}
    customer_clasificacion_primary_tag_map: dict[str, str] = {}
    try:
        cust_ids = sorted({str(getattr(r, 'customer_id', '') or '').strip() for r in rows if str(getattr(r, 'customer_id', '') or '').strip()})
        if cust_ids:
            for c in (db.session.query(Customer).filter(Customer.company_id == cid, Customer.id.in_(cust_ids)).all() or []):
                try:
                    customers_map[str(getattr(c, 'id', '') or '').strip()] = c
                except Exception:
                    continue
            for (ccid, total_due) in (
                db.session.query(Sale.customer_id, func.sum(Sale.due_amount))
                .filter(Sale.company_id == cid)
                .filter(Sale.customer_id.in_(cust_ids))
                .filter(Sale.sale_type == 'Venta')
                .filter(Sale.status != 'Reemplazada')
                .group_by(Sale.customer_id)
                .all()
            ):
                k = str(ccid or '').strip()
                if k:
                    try:
                        customer_saldo_map[k] = float(total_due or 0.0)
                    except Exception:
                        customer_saldo_map[k] = 0.0
            for (ccid, n) in (
                db.session.query(Sale.customer_id, func.count(Sale.id))
                .filter(Sale.company_id == cid)
                .filter(Sale.customer_id.in_(cust_ids))
                .filter(Sale.sale_type == 'Venta')
                .filter(Sale.status != 'Reemplazada')
                .group_by(Sale.customer_id)
                .all()
            ):
                k = str(ccid or '').strip()
                if k:
                    try:
                        customer_sales_count_map[k] = int(n or 0)
                    except Exception:
                        customer_sales_count_map[k] = 0

            crm_cfg = None
            try:
                from app.customers.routes import _load_crm_config as _load_crm_cfg
                crm_cfg = _load_crm_cfg(cid)
            except Exception:
                crm_cfg = None

            recent_days = 60
            debt_overdue_days = 30
            debt_critical_days = 60
            freq_min_purchases = 1
            best_min_purchases = 2
            inst_overdue_days = 7
            inst_critical_days = 15
            labels = {
                'best': 'Mejor cliente',
                'freq': 'Frecuente',
                'occasional': 'Ocasional',
                'inactive': 'Inactivo',
                'debtor': 'CC Vencida',
                'debtor_critical': 'CC Vencida Crítica',
                'installments_overdue': 'Sistema de Cuotas Vencido',
                'installments_critical': 'Sistema de Cuotas Crítico',
            }
            try:
                if isinstance(crm_cfg, dict):
                    recent_days = max(1, int(crm_cfg.get('recent_days') or recent_days))
                    debt_overdue_days = max(1, int(crm_cfg.get('debt_overdue_days') or debt_overdue_days))
                    debt_critical_days = max(debt_overdue_days, int(crm_cfg.get('debt_critical_days') or debt_critical_days))
                    freq_min_purchases = max(1, int(crm_cfg.get('freq_min_purchases') or freq_min_purchases))
                    best_min_purchases = max(freq_min_purchases + 1, int(crm_cfg.get('best_min_purchases') or best_min_purchases))
                    inst_overdue_days = max(1, int(crm_cfg.get('installments_overdue_days') or inst_overdue_days))
                    inst_critical_days = max(inst_overdue_days + 1, int(crm_cfg.get('installments_critical_days') or inst_critical_days))
                    lbl = crm_cfg.get('labels') if isinstance(crm_cfg.get('labels'), dict) else {}
                    for k in list(labels.keys()):
                        v = str(lbl.get(k) or '').strip()
                        if v:
                            labels[k] = v
            except Exception:
                pass

            try:
                last_sale_map: dict[str, dt_date] = {}
                for (ccid, mx) in (
                    db.session.query(Sale.customer_id, func.max(Sale.sale_date))
                    .filter(Sale.company_id == cid)
                    .filter(Sale.customer_id.in_(cust_ids))
                    .filter(Sale.sale_type == 'Venta')
                    .filter(Sale.status != 'Reemplazada')
                    .group_by(Sale.customer_id)
                    .all()
                ):
                    k = str(ccid or '').strip()
                    if k and mx:
                        last_sale_map[k] = mx

                oldest_due_map: dict[str, dt_date] = {}
                for (ccid, mn) in (
                    db.session.query(Sale.customer_id, func.min(Sale.sale_date))
                    .filter(Sale.company_id == cid)
                    .filter(Sale.customer_id.in_(cust_ids))
                    .filter(Sale.sale_type == 'Venta')
                    .filter(Sale.status != 'Reemplazada')
                    .filter(Sale.due_amount > 0)
                    .group_by(Sale.customer_id)
                    .all()
                ):
                    k = str(ccid or '').strip()
                    if k and mn:
                        oldest_due_map[k] = mn

                installments_enabled = False
                try:
                    from app.customers.routes import _installments_enabled as _inst_enabled
                    installments_enabled = bool(_inst_enabled(cid))
                except Exception:
                    installments_enabled = False

                oldest_overdue_inst_due_by_customer: dict[str, dt_date] = {}
                if installments_enabled and cust_ids:
                    try:
                        rows_inst = (
                            db.session.query(Installment, InstallmentPlan)
                            .join(InstallmentPlan, Installment.plan_id == InstallmentPlan.id)
                            .filter(Installment.company_id == cid)
                            .filter(InstallmentPlan.company_id == cid)
                            .filter(InstallmentPlan.customer_id.in_(cust_ids))
                            .filter(db.func.lower(InstallmentPlan.status) == 'activo')
                            .filter(db.func.lower(Installment.status) != 'pagada')
                            .all()
                        )
                    except Exception:
                        rows_inst = []
                    for inst, plan in (rows_inst or []):
                        ccid = str(getattr(plan, 'customer_id', '') or '').strip()
                        if not ccid:
                            continue
                        dd = getattr(inst, 'due_date', None)
                        if dd and dd < today:
                            prev = oldest_overdue_inst_due_by_customer.get(ccid)
                            if prev is None or dd < prev:
                                oldest_overdue_inst_due_by_customer[ccid] = dd

                now = dt_date.today()
                for c_id in cust_ids:
                    saldo = float(customer_saldo_map.get(c_id, 0.0) or 0.0)
                    n_sales = int(customer_sales_count_map.get(c_id, 0) or 0)
                    last_dt = last_sale_map.get(c_id)
                    oldest_due = oldest_due_map.get(c_id)
                    dias_deuda = 0
                    try:
                        if saldo > 1e-9 and oldest_due:
                            dias_deuda = max(0, int((now - oldest_due).days))
                    except Exception:
                        dias_deuda = 0

                    sc_overdue_days = 0
                    try:
                        od = oldest_overdue_inst_due_by_customer.get(c_id)
                        if od:
                            sc_overdue_days = max(0, int((now - od).days))
                    except Exception:
                        sc_overdue_days = 0

                    tags: list[str] = []
                    if installments_enabled:
                        if sc_overdue_days >= inst_critical_days:
                            tags.append('sc_critico')
                        elif sc_overdue_days >= inst_overdue_days:
                            tags.append('sc_vencido')
                    if saldo > 1e-9:
                        if dias_deuda >= debt_critical_days:
                            tags.append('cc_critica')
                        elif dias_deuda >= debt_overdue_days:
                            tags.append('cc_vencida')

                    inactive = False
                    try:
                        if not last_dt:
                            inactive = True
                        else:
                            inactive = ((now - last_dt).days > recent_days)
                    except Exception:
                        inactive = False

                    if inactive:
                        tags.append('inactivo')
                    else:
                        if n_sales >= best_min_purchases:
                            tags.append('mejor_cliente')
                        elif n_sales >= freq_min_purchases:
                            tags.append('frecuente')
                        else:
                            tags.append('ocasional')

                    lbls: list[str] = []
                    for t in tags:
                        if t == 'sc_critico':
                            lbls.append(labels.get('installments_critical') or 'Sistema de Cuotas Crítico')
                        elif t == 'sc_vencido':
                            lbls.append(labels.get('installments_overdue') or 'Sistema de Cuotas Vencido')
                        elif t == 'cc_critica':
                            lbls.append(labels.get('debtor_critical') or 'CC Vencida Crítica')
                        elif t == 'cc_vencida':
                            lbls.append(labels.get('debtor') or 'CC Vencida')
                        elif t == 'mejor_cliente':
                            lbls.append(labels.get('best') or 'Mejor cliente')
                        elif t == 'frecuente':
                            lbls.append(labels.get('freq') or 'Frecuente')
                        elif t == 'ocasional':
                            lbls.append(labels.get('occasional') or 'Ocasional')
                        elif t == 'inactivo':
                            lbls.append(labels.get('inactive') or 'Inactivo')
                    customer_clasificacion_tags_map[c_id] = tags
                    customer_clasificacion_map[c_id] = ' | '.join([x for x in lbls if x])

                    primary_order = ['sc_critico', 'sc_vencido', 'cc_critica', 'cc_vencida', 'mejor_cliente', 'frecuente', 'ocasional', 'inactivo']
                    primary_tag = ''
                    for p in primary_order:
                        if p in tags:
                            primary_tag = p
                            break
                    primary_label = ''
                    if primary_tag == 'sc_critico':
                        primary_label = labels.get('installments_critical') or 'Sistema de Cuotas Crítico'
                    elif primary_tag == 'sc_vencido':
                        primary_label = labels.get('installments_overdue') or 'Sistema de Cuotas Vencido'
                    elif primary_tag == 'cc_critica':
                        primary_label = labels.get('debtor_critical') or 'CC Vencida Crítica'
                    elif primary_tag == 'cc_vencida':
                        primary_label = labels.get('debtor') or 'CC Vencida'
                    elif primary_tag == 'mejor_cliente':
                        primary_label = labels.get('best') or 'Mejor cliente'
                    elif primary_tag == 'frecuente':
                        primary_label = labels.get('freq') or 'Frecuente'
                    elif primary_tag == 'ocasional':
                        primary_label = labels.get('occasional') or 'Ocasional'
                    elif primary_tag == 'inactivo':
                        primary_label = labels.get('inactive') or 'Inactivo'
                    customer_clasificacion_primary_tag_map[c_id] = primary_tag
                    customer_clasificacion_primary_map[c_id] = primary_label
            except Exception:
                customer_clasificacion_map = {}
                customer_clasificacion_tags_map = {}
                customer_clasificacion_primary_map = {}
                customer_clasificacion_primary_tag_map = {}
    except Exception:
        cmv_by_ticket = {}

    users_map: dict[int, str] = {}
    try:
        uids = set()
        for r in rows:
            try:
                uid = int(getattr(r, 'created_by_user_id', 0) or 0) or 0
            except Exception:
                uid = 0
            if uid:
                uids.add(int(uid))
        if uids:
            uq = db.session.query(User).filter(User.id.in_(sorted(uids)))
            try:
                uq = uq.filter((User.company_id == cid) | (User.company_id.is_(None)))
            except Exception:
                pass
            for u in (uq.all() or []):
                try:
                    name = str(getattr(u, 'display_name', '') or getattr(u, 'username', '') or getattr(u, 'email', '') or '').strip()
                    users_map[int(getattr(u, 'id', 0) or 0)] = name or ('Usuario #' + str(int(getattr(u, 'id', 0) or 0)))
                except Exception:
                    continue
    except Exception:
        users_map = {}

    by_ticket = {}
    for r in rows:
        try:
            tk = str(getattr(r, 'ticket', '') or '').strip()
            if tk:
                by_ticket[tk] = r
        except Exception:
            continue

    referenced = set()
    for r in rows:
        kind, tok = _parse_related_from_notes(getattr(r, 'notes', '') or '')
        if tok and (not _is_tmp_related_ticket(tok)):
            referenced.add(tok)

    missing = [t for t in referenced if t not in by_ticket]
    if missing:
        try:
            extra = (
                db.session.query(Sale)
                .filter(Sale.company_id == cid)
                .filter(Sale.ticket.in_(missing))
                .all()
            )
            for r in (extra or []):
                tk = str(getattr(r, 'ticket', '') or '').strip()
                if tk:
                    by_ticket[tk] = r
        except Exception:
            current_app.logger.exception('Failed to load referenced related tickets')

    exchange_groups = {}
    for r in rows:
        try:
            rt = getattr(r, 'exchange_return_total', None)
            nt = getattr(r, 'exchange_new_total', None)
            if rt is None or nt is None:
                continue
            key = (
                (r.sale_date.isoformat() if r.sale_date else ''),
                round(float(rt or 0.0), 4),
                round(float(nt or 0.0), 4),
                str(getattr(r, 'customer_id', '') or '').strip(),
                str(getattr(r, 'customer_name', '') or '').strip(),
                int(getattr(r, 'created_by_user_id', 0) or 0),
            )
            exchange_groups.setdefault(key, []).append(r)
        except Exception:
            continue

    def _pick_exchange_pair(row: Sale):
        try:
            rt = getattr(row, 'exchange_return_total', None)
            nt = getattr(row, 'exchange_new_total', None)
            if rt is None or nt is None:
                return None
            key = (
                (row.sale_date.isoformat() if row.sale_date else ''),
                round(float(rt or 0.0), 4),
                round(float(nt or 0.0), 4),
                str(getattr(row, 'customer_id', '') or '').strip(),
                str(getattr(row, 'customer_name', '') or '').strip(),
                int(getattr(row, 'created_by_user_id', 0) or 0),
            )
            group = exchange_groups.get(key) or []
            cands = [x for x in group if int(getattr(x, 'id', 0) or 0) != int(getattr(row, 'id', 0) or 0)]
            if not cands:
                return None
            pref = [x for x in cands if str(getattr(x, 'sale_type', '') or '').strip() != str(getattr(row, 'sale_type', '') or '').strip()]
            cands = pref or cands
            if getattr(row, 'created_at', None):
                cands.sort(key=lambda x: abs((x.created_at - row.created_at).total_seconds()) if getattr(x, 'created_at', None) else 999999)
            return cands[0] if cands else None
        except Exception:
            return None

    related_map = {}
    for r in rows:
        kind, tok = _parse_related_from_notes(getattr(r, 'notes', '') or '')
        rel_row = None
        if tok and (not _is_tmp_related_ticket(tok)):
            rel_row = by_ticket.get(tok)
        if not rel_row:
            rel_row = _pick_exchange_pair(r)

        if rel_row:
            rel_ticket = str(getattr(rel_row, 'ticket', '') or '').strip()
            rel_type = str(getattr(rel_row, 'sale_type', '') or '').strip()
            if _is_tmp_related_ticket(rel_ticket):
                related_map[int(r.id)] = {
                    'ticket': '',
                    'type': _related_type_slug(rel_type),
                    'label': _fallback_related_summary(rel_row),
                    'url': '',
                }
                continue
            related_map[int(r.id)] = {
                'ticket': rel_ticket,
                'type': _related_type_slug(rel_type),
                'label': _build_related_label(getattr(r, 'sale_type', ''), rel_type, rel_ticket),
                'url': '',
            }
        else:
            has_rel_hint = bool(tok) or (getattr(r, 'exchange_return_total', None) is not None and getattr(r, 'exchange_new_total', None) is not None)
            related_map[int(r.id)] = {
                'ticket': '',
                'type': ('sale' if kind == 'venta' else 'change' if kind == 'cambio' else ''),
                'label': 'Relacionado (no disponible)' if has_rel_hint else '',
                'url': '',
            }

    return jsonify({
        'ok': True,
        'items': [
            _serialize_sale(
                r,
                related=related_map.get(int(r.id)),
                users_map=users_map,
                customers_map=customers_map,
                customer_saldo_map=customer_saldo_map,
                customer_sales_count_map=customer_sales_count_map,
                customer_clasificacion_map=customer_clasificacion_map,
                customer_clasificacion_tags_map=customer_clasificacion_tags_map,
                customer_clasificacion_primary_map=customer_clasificacion_primary_map,
                customer_clasificacion_primary_tag_map=customer_clasificacion_primary_tag_map,
                cmv_by_ticket=cmv_by_ticket,
            )
            for r in rows
        ],
    })


@bp.get('/api/sales/<ticket>')
@login_required
@module_required('sales')
def get_sale(ticket):
    t = str(ticket or '').strip()
    cid = _company_id()
    row = db.session.query(Sale).filter(Sale.company_id == cid, Sale.ticket == t).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    kind, tok = _parse_related_from_notes(getattr(row, 'notes', '') or '')
    rel_row = None
    if tok and (not _is_tmp_related_ticket(tok)):
        try:
            rel_row = db.session.query(Sale).filter(Sale.company_id == cid, Sale.ticket == tok).first()
        except Exception:
            current_app.logger.exception('Failed to load related sale by ticket')
            rel_row = None
    if not rel_row:
        try:
            rt = getattr(row, 'exchange_return_total', None)
            nt = getattr(row, 'exchange_new_total', None)
            if rt is not None and nt is not None:
                qrel = (
                    db.session.query(Sale)
                    .filter(Sale.company_id == cid)
                    .filter(Sale.id != row.id)
                    .filter(Sale.sale_date == row.sale_date)
                    .filter(Sale.exchange_return_total == rt)
                    .filter(Sale.exchange_new_total == nt)
                )
                try:
                    if row.created_by_user_id:
                        qrel = qrel.filter(Sale.created_by_user_id == row.created_by_user_id)
                except Exception:
                    pass
                try:
                    if row.customer_id:
                        qrel = qrel.filter(Sale.customer_id == row.customer_id)
                    elif row.customer_name:
                        qrel = qrel.filter(Sale.customer_name == row.customer_name)
                except Exception:
                    pass
                if row.created_at:
                    try:
                        lo = row.created_at - timedelta(minutes=5)
                        hi = row.created_at + timedelta(minutes=5)
                        qrel = qrel.filter(Sale.created_at >= lo, Sale.created_at <= hi)
                    except Exception:
                        pass
                rel_row = qrel.order_by(Sale.created_at.desc(), Sale.id.desc()).first()
        except Exception:
            current_app.logger.exception('Failed to load related sale by exchange fields')
            rel_row = None

    if rel_row:
        rel_ticket = str(getattr(rel_row, 'ticket', '') or '').strip()
        rel_type = str(getattr(rel_row, 'sale_type', '') or '').strip()
        if _is_tmp_related_ticket(rel_ticket):
            related = {
                'ticket': '',
                'type': _related_type_slug(rel_type),
                'label': _fallback_related_summary(rel_row),
                'url': '',
            }
        else:
            related = {
                'ticket': rel_ticket,
                'type': _related_type_slug(rel_type),
                'label': _build_related_label(getattr(row, 'sale_type', ''), rel_type, rel_ticket),
                'url': '',
            }
    else:
        has_rel_hint = bool(tok) or (getattr(row, 'exchange_return_total', None) is not None and getattr(row, 'exchange_new_total', None) is not None)
        related = {
            'ticket': '',
            'type': ('sale' if kind == 'venta' else 'change' if kind == 'cambio' else ''),
            'label': 'Relacionado (no disponible)' if has_rel_hint else '',
            'url': '',
        }

    return jsonify({'ok': True, 'item': _serialize_sale(row, related=related)})


@bp.get('/api/products')
@login_required
@module_required('sales')
def list_products_for_sales():
    try:
        _ensure_product_columns_for_sales()
    except Exception:
        pass

    qraw = str(request.args.get('q') or '').strip()
    limit = int(request.args.get('limit') or 500)
    if limit <= 0 or limit > 5000:
        limit = 500
    offset = int(request.args.get('offset') or 0)
    if offset < 0:
        offset = 0
    cid = _company_id()
    if not cid:
        return jsonify({'ok': True, 'items': [], 'has_more': False, 'next_offset': None})
    try:
        q = (
            db.session.query(Product)
            .options(joinedload(Product.category))
            .filter(Product.company_id == cid)
            .filter(Product.active == True)  # noqa: E712
            .filter(getattr(Product, 'deleted_at', None).is_(None) if hasattr(Product, 'deleted_at') else True)
        )
        if qraw:
            like = f"%{qraw}%"
            q = q.filter(or_(Product.name.ilike(like), Product.internal_code.ilike(like), Product.barcode.ilike(like)))
        q = q.order_by(Product.name.asc(), Product.id.asc())

        rows = q.offset(offset).limit(limit + 1).all()
    except Exception:
        try:
            current_app.logger.exception('Failed to list products for sales', extra={'company_id': cid, 'q': qraw, 'limit': limit, 'offset': offset})
        except Exception:
            pass
        rows = []
    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]
    next_offset = (offset + limit) if has_more else None
    try:
        changed = _ensure_codigo_interno_for_sales(rows)
        if changed:
            db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
    return jsonify({'ok': True, 'items': [_serialize_product_for_sales(r) for r in rows], 'has_more': has_more, 'next_offset': next_offset})


@bp.get('/api/lots')
@login_required
@module_required('sales')
def list_lots_for_sales():
    limit = int(request.args.get('limit') or 10000)
    if limit <= 0 or limit > 20000:
        limit = 10000
    product_id = (request.args.get('product_id') or '').strip()
    cid = _company_id()
    if not cid:
        return jsonify({'ok': True, 'items': []})
    q = db.session.query(InventoryLot).filter(InventoryLot.company_id == cid).filter(InventoryLot.qty_available > 0)
    if product_id:
        try:
            q = q.filter(InventoryLot.product_id == int(product_id))
        except Exception:
            current_app.logger.exception('Failed to filter lots by product id')
            return jsonify({'ok': True, 'items': []})
    q = q.order_by(InventoryLot.received_at.desc(), InventoryLot.id.desc()).limit(limit)
    rows = q.all()
    return jsonify({'ok': True, 'items': [_serialize_lot_for_sales(r) for r in rows]})


@bp.get('/api/sales/debt-summary')
@login_required
@module_required('sales')
def debt_summary():
    customer_id = str(request.args.get('customer_id') or '').strip()
    customer_name = str(request.args.get('customer_name') or '').strip()

    cid = _company_id()
    if not cid:
        return jsonify({'ok': True, 'saldo': 0.0, 'dias': 0}), 200
    q = db.session.query(Sale).filter(Sale.company_id == cid).filter(Sale.due_amount > 0)
    if customer_id:
        q = q.filter(Sale.customer_id == customer_id)
    elif customer_name:
        q = q.filter(Sale.customer_name == customer_name)
    else:
        return jsonify({'ok': True, 'saldo': 0.0, 'dias': 0}), 200

    rows = q.all()
    saldo = 0.0
    last_ts = 0
    for r in rows:
        saldo += float(r.due_amount or 0.0)
        ts = _dt_to_ms(r.created_at)
        if ts and ts > last_ts:
            last_ts = ts
        try:
            if r.sale_date:
                dts = int(datetime.combine(r.sale_date, datetime.min.time()).timestamp() * 1000)
                if dts > last_ts:
                    last_ts = dts
        except Exception:
            current_app.logger.exception('Failed to compute last_ts for sales debt summary')

    dias = 0
    if saldo > 0 and last_ts:
        try:
            dias = max(0, int((datetime.utcnow().timestamp() * 1000 - last_ts) // (1000 * 60 * 60 * 24)))
        except Exception:
            current_app.logger.exception('Failed to compute dias for sales debt summary')
    return jsonify({'ok': True, 'saldo': saldo, 'dias': dias}), 200


@bp.get('/api/sales/overdue-customers')
@login_required
@module_required('sales')
def overdue_customers_count():
    days = int(request.args.get('days') or 30)
    if days <= 0 or days > 3650:
        days = 30

    cid = _company_id()
    if not cid:
        return jsonify({'ok': True, 'count': 0}), 200

    cutoff = dt_date.today() - timedelta(days=days)
    q = (
        db.session.query(Sale.customer_id, Sale.customer_name)
        .filter(Sale.company_id == cid)
        .filter(Sale.sale_type == 'Venta')
        .filter(Sale.status != 'Reemplazada')
        .filter(Sale.due_amount > 0)
        .filter(Sale.sale_date <= cutoff)
    )
    rows = q.all()
    uniq = set()
    for r in rows:
        c_id = str(getattr(r, 'customer_id', '') or '').strip()
        c_name = str(getattr(r, 'customer_name', '') or '').strip()
        key = c_id or c_name
        if key:
            uniq.add(key)

    return jsonify({'ok': True, 'count': len(uniq)}), 200


@bp.post('/api/sales/settle')
@login_required
@module_required_any('sales', 'customers')
def settle_cc_sale():
    payload = request.get_json(silent=True) or {}
    sale_id = payload.get('sale_id')
    ticket = str(payload.get('ticket') or '').strip()
    payment_method = str(payload.get('payment_method') or 'Efectivo').strip() or 'Efectivo'
    payments = None
    try:
        payments = _parse_payments_payload(payload)
    except ValueError as e:
        return jsonify({'ok': False, 'error': str(e)}), 400
    except Exception:
        payments = None
    amount_raw = payload.get('amount')

    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    _ensure_sale_employee_columns()
    _ensure_sale_ticket_numbering()

    raw_emp_id = str(payload.get('employee_id') or '').strip() or None
    raw_emp_name = str(payload.get('employee_name') or '').strip() or None
    emp_id, emp_name = _resolve_employee_fields(cid=cid, employee_id=raw_emp_id, employee_name=raw_emp_name)

    row = None
    if sale_id is not None and str(sale_id).strip() != '':
        try:
            row = db.session.get(Sale, int(sale_id))
        except Exception:
            current_app.logger.exception('Failed to retrieve sale by id')
            row = None
    if not row and ticket:
        row = db.session.query(Sale).filter(Sale.company_id == cid, Sale.ticket == ticket).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    if str(getattr(row, 'company_id', '') or '') != cid:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    try:
        if bool(getattr(row, 'is_installments', False)):
            return jsonify({'ok': False, 'error': 'installments_not_cc'}), 400
    except Exception:
        pass

    due = float(row.due_amount or 0.0)
    if due <= 0:
        return jsonify({'ok': False, 'error': 'no_due'}), 400

    pay_amount = None
    if amount_raw is not None and str(amount_raw).strip() != '':
        try:
            pay_amount = float(amount_raw)
        except Exception:
            pay_amount = None
    if pay_amount is None:
        pay_amount = abs(due)
    pay_amount = float(pay_amount or 0.0)
    if pay_amount <= 0:
        return jsonify({'ok': False, 'error': 'amount_invalid'}), 400
    if pay_amount - abs(due) > 0.00001:
        return jsonify({'ok': False, 'error': 'amount_exceeds_due'}), 400

    settle_date = _parse_date_iso(payload.get('date') or payload.get('fecha'), dt_date.today())
    base_n = _next_payment_number(cid)
    ref = str(row.ticket or '').strip()
    try:
        note_products = _products_label_from_sale(row)
    except Exception:
        note_products = 'Producto —'
    cust_txt = str(getattr(row, 'customer_name', '') or '').strip() or '—'
    if ref:
        note = f"Cobro cuenta corriente – Ticket #{ref} – {note_products} – Cliente {cust_txt}"
    else:
        note = f"Cobro cuenta corriente – {note_products} – Cliente {cust_txt}"

    attempts = 0
    while attempts < 10:
        n = base_n + attempts
        attempts += 1
        payment_ticket = _format_ticket_number(n, prefix='P')
        pay_row = Sale(
            ticket=payment_ticket,
            ticket_number=None,
            company_id=cid,
            sale_date=settle_date,
            sale_type='CobroCC',
            status='Completada',
            payment_method=payment_method,
            notes=note,
            total=abs(pay_amount),
            discount_general_pct=0.0,
            discount_general_amount=0.0,
            on_account=False,
            paid_amount=abs(pay_amount),
            due_amount=0.0,
            customer_id=row.customer_id,
            customer_name=row.customer_name,
            employee_id=emp_id,
            employee_name=emp_name,
            exchange_return_total=None,
            exchange_new_total=None,
        )
        for it in (row.items or []):
            pay_row.items.append(SaleItem(
                direction=str(getattr(it, 'direction', '') or 'out'),
                product_id=str(getattr(it, 'product_id', '') or '').strip() or None,
                product_name=str(getattr(it, 'product_name', '') or 'Producto'),
                qty=float(getattr(it, 'qty', 0.0) or 0.0),
                unit_price=float(getattr(it, 'unit_price', 0.0) or 0.0),
                discount_pct=float(getattr(it, 'discount_pct', 0.0) or 0.0),
                subtotal=float(getattr(it, 'subtotal', 0.0) or 0.0),
            ))
        try:
            from flask_login import current_user
            uid = int(getattr(current_user, 'id', 0) or 0) or None
            pay_row.created_by_user_id = uid
        except Exception:
            current_app.logger.exception('Failed to set created_by_user_id for payment sale')
            pay_row.created_by_user_id = None

        row.paid_amount = float(row.paid_amount or 0.0) + abs(pay_amount)
        remaining = abs(due) - abs(pay_amount)
        row.due_amount = float(remaining if remaining > 0.00001 else 0.0)
        row.on_account = bool(row.due_amount > 0)
        db.session.add(pay_row)
        try:
            db.session.commit()
            return jsonify({'ok': True, 'item': _serialize_sale(pay_row)})
        except IntegrityError:
            db.session.rollback()
            continue
        except Exception:
            db.session.rollback()
            current_app.logger.exception('Failed to commit payment sale')
            return jsonify({'ok': False, 'error': 'db_error'}), 400

    return jsonify({'ok': False, 'error': 'ticket_duplicate', 'message': 'No se pudo registrar el cobro: ticket duplicado.'}), 400


@bp.post('/api/exchanges')
@login_required
@module_required('sales')
def create_exchange():
    _ensure_sale_employee_columns()
    _ensure_sale_surcharge_columns()
    payload = request.get_json(silent=True) or {}
    sale_date = _parse_date_iso(payload.get('fecha') or payload.get('date'), dt_date.today())
    payment_method = str(payload.get('payment_method') or 'Efectivo').strip() or 'Efectivo'
    payments = None
    try:
        payments = _parse_payments_payload(payload)
    except ValueError as e:
        return jsonify({'ok': False, 'error': str(e)}), 400
    except Exception:
        payments = None
    notes = str(payload.get('notes') or '').strip() or None

    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    _ensure_sale_ticket_numbering()

    raw_emp_id = str(payload.get('employee_id') or '').strip() or None
    raw_emp_name = str(payload.get('employee_name') or '').strip() or None
    emp_id, emp_name = _resolve_employee_fields(cid=cid, employee_id=raw_emp_id, employee_name=raw_emp_name)

    customer_id = str(payload.get('customer_id') or '').strip() or None
    customer_name = str(payload.get('customer_name') or '').strip() or None

    return_items = payload.get('return_items')
    new_items = payload.get('new_items')
    return_items_list = return_items if isinstance(return_items, list) else []
    new_items_list = new_items if isinstance(new_items, list) else []
    if not return_items_list or not new_items_list:
        return jsonify({'ok': False, 'error': 'items_required'}), 400

    def _force_direction(items, direction: str):
        out = []
        for it in (items if isinstance(items, list) else []):
            d = it if isinstance(it, dict) else {}
            nd = dict(d)
            nd['direction'] = direction
            out.append(nd)
        return out

    # Frontend del cambio no envía 'direction'.
    # Para el impacto de inventario: devoluciones siempre 'in', nueva venta siempre 'out'.
    return_items_inv = _force_direction(return_items_list, 'in')
    new_items_inv = _force_direction(new_items_list, 'out')

    return_total = _num(payload.get('return_total'))
    new_total = _num(payload.get('new_total'))
    if return_total < 0:
        return_total = abs(return_total)
    if new_total < 0:
        new_total = abs(new_total)

    discount_general_pct = _num(payload.get('discount_general_pct'))
    discount_general_amount = _num(payload.get('discount_general_amount'))
    surcharge_general_pct = _num(payload.get('surcharge_general_pct') if payload.get('surcharge_general_pct') is not None else payload.get('general_surcharge_pct'))
    surcharge_general_amount = _num(payload.get('surcharge_general_amount') if payload.get('surcharge_general_amount') is not None else payload.get('surcharge_amount'))

    diff_to_pay = max(0.0, float(new_total or 0.0) - float(return_total or 0.0))

    # Validación pagos múltiples para el cambio: se valida contra la diferencia a abonar.
    # Si diff_to_pay == 0, permitir no enviar payments.
    if payments is not None:
        expected = float(diff_to_pay or 0.0)
        total_pays = _sum_payments(payments)
        if expected <= 0.00001:
            # Si no hay monto a cobrar, ignorar payments (pero mantenerlo permitido si llega vacío).
            if abs(float(total_pays)) > 0.01:
                return jsonify({'ok': False, 'error': 'payments_sum_mismatch'}), 400
        else:
            if abs(float(expected) - float(total_pays)) > 0.01:
                return jsonify({'ok': False, 'error': 'payments_sum_mismatch'}), 400
    on_account = bool(payload.get('on_account'))
    paid_amount = _num(payload.get('paid_amount'))
    if paid_amount < 0:
        paid_amount = 0.0
    if paid_amount > diff_to_pay:
        paid_amount = diff_to_pay
    due_amount = max(0.0, diff_to_pay - paid_amount) if on_account else 0.0
    if diff_to_pay <= 0:
        paid_amount = 0.0
        due_amount = 0.0
        on_account = False
    if not on_account:
        paid_amount = diff_to_pay
        due_amount = 0.0
    payment_method, payments = _normalize_sale_payment_fields(
        sale_type='Venta',
        on_account=bool(on_account),
        paid_amount=paid_amount,
        payment_method=payment_method,
        payments=payments,
        is_installments=False,
    )

    is_gift = bool(payload.get('is_gift'))
    gift_code = str(payload.get('gift_code') or '').strip() or None

    base_change_n = _next_change_number(cid)
    base_sale_n = _next_ticket_number(cid)

    # En una transacción: registrar 2 movimientos (Devolución + Venta)
    attempts = 0
    while attempts < 10:
        change_n = base_change_n + attempts
        sale_n = base_sale_n + attempts
        attempts += 1

        return_ticket = _format_ticket_number(change_n, 'C')
        sale_ticket = _format_ticket_number(sale_n)

        base_notes = notes
        rel_return = f"Relacionado a venta {sale_ticket}"
        rel_sale = f"Relacionado a cambio {return_ticket}"
        return_notes = (str(base_notes).strip() + ('\n' if str(base_notes).strip() else '') + rel_return) if base_notes else rel_return
        sale_notes = (str(base_notes).strip() + ('\n' if str(base_notes).strip() else '') + rel_sale) if base_notes else rel_sale

        return_row = Sale(
            ticket=return_ticket,
            ticket_number=None,
            company_id=cid,
            sale_date=sale_date,
            sale_type='Cambio',
            status='Cambio',
            payment_method=payment_method,
            notes=return_notes,
            total=-abs(return_total),
            discount_general_pct=0.0,
            discount_general_amount=0.0,
            on_account=False,
            paid_amount=0.0,
            due_amount=0.0,
            customer_id=customer_id,
            customer_name=customer_name,
            employee_id=emp_id,
            employee_name=emp_name,
            exchange_return_total=return_total,
            exchange_new_total=new_total,
        )

        paid_cash = paid_amount
        credit = min(float(return_total or 0.0), float(new_total or 0.0))
        paid_for_sale = max(0.0, min(float(new_total or 0.0), float(credit) + float(paid_cash)))
        due_for_sale = max(0.0, float(new_total or 0.0) - paid_for_sale)

        sale_row = Sale(
            ticket=sale_ticket,
            ticket_number=sale_n,
            company_id=cid,
            sale_date=sale_date,
            sale_type='Venta',
            status='Completada',
            payment_method=payment_method,
            notes=sale_notes,
            total=abs(new_total),
            discount_general_pct=discount_general_pct,
            discount_general_amount=discount_general_amount,
            general_surcharge_pct=surcharge_general_pct,
            surcharge_general_amount=surcharge_general_amount,
            on_account=(due_for_sale > 0),
            paid_amount=paid_for_sale,
            due_amount=due_for_sale,
            customer_id=customer_id,
            customer_name=customer_name,
            employee_id=emp_id,
            employee_name=emp_name,
            exchange_return_total=return_total,
            exchange_new_total=new_total,
        )

        if payments is not None:
            try:
                sale_row.payments = []
                for p in (payments or []):
                    sale_row.payments.append(SalePayment(
                        company_id=cid,
                        method=str(p.get('method') or '').strip(),
                        amount=float(p.get('amount') or 0.0),
                    ))
                if len(payments) == 1:
                    sale_row.payment_method = str(payments[0].get('method') or payment_method)
                elif len(payments) >= 2:
                    sale_row.payment_method = ' + '.join([str(x.get('method')) for x in payments])
            except Exception:
                current_app.logger.exception('Failed to attach sale payments (exchange flow)')

        try:
            from flask_login import current_user
            uid = int(getattr(current_user, 'id', 0) or 0) or None
            return_row.created_by_user_id = uid
            sale_row.created_by_user_id = uid
        except Exception:
            current_app.logger.exception('Failed to set created_by_user_id for exchange sales')
            return_row.created_by_user_id = None
            sale_row.created_by_user_id = None

        try:
            sale_row.is_gift = is_gift
            if is_gift and not gift_code:
                gift_code = _make_gift_code(sale_ticket, new_items_list)
            sale_row.gift_code = gift_code
        except Exception:
            current_app.logger.exception('Failed to apply gift_code for sale (exchange flow)')

        for it in return_items_list:
            d = it if isinstance(it, dict) else {}
            return_row.items.append(SaleItem(
                direction='in',
                product_id=str(d.get('product_id') or '').strip() or None,
                product_name=str(d.get('nombre') or d.get('product_name') or 'Producto').strip() or 'Producto',
                qty=_num(d.get('cantidad') if d.get('cantidad') is not None else d.get('qty')),
                unit_price=_num(d.get('precio') if d.get('precio') is not None else d.get('unit_price')),
                discount_pct=_num(d.get('descuento') if d.get('descuento') is not None else d.get('discount_pct')),
                subtotal=_num(d.get('subtotal')),
            ))

        for it in new_items_list:
            d = it if isinstance(it, dict) else {}
            sale_row.items.append(SaleItem(
                direction='out',
                product_id=str(d.get('product_id') or '').strip() or None,
                product_name=str(d.get('nombre') or d.get('product_name') or 'Producto').strip() or 'Producto',
                qty=_num(d.get('cantidad') if d.get('cantidad') is not None else d.get('qty')),
                unit_price=_num(d.get('precio') if d.get('precio') is not None else d.get('unit_price')),
                discount_pct=_num(d.get('descuento') if d.get('descuento') is not None else d.get('discount_pct')),
                subtotal=_num(d.get('subtotal')),
            ))

        db.session.add(return_row)
        db.session.add(sale_row)
        try:
            db.session.flush()
            _apply_inventory_for_sale(sale_ticket=return_row.ticket, sale_date=sale_date, items=return_items_inv)
            _apply_inventory_for_sale(sale_ticket=sale_row.ticket, sale_date=sale_date, items=new_items_inv)
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            continue
        except ValueError as e:
            db.session.rollback()
            current_app.logger.exception('Failed to create exchange: stock insufficient')
            return jsonify({'ok': False, 'error': 'stock_insufficient', 'message': str(e)}), 400
        except Exception:
            db.session.rollback()
            current_app.logger.exception('Failed to create exchange: db error')
            return jsonify({'ok': False, 'error': 'db_error'}), 400

        related_for_return = {
            'ticket': sale_row.ticket,
            'type': _related_type_slug(str(getattr(sale_row, 'sale_type', '') or '').strip()),
            'label': _build_related_label('Cambio', str(getattr(sale_row, 'sale_type', '') or '').strip(), sale_row.ticket),
            'url': '',
        }
        related_for_sale = {
            'ticket': return_row.ticket,
            'type': _related_type_slug(str(getattr(return_row, 'sale_type', '') or '').strip()),
            'label': _build_related_label('Venta', str(getattr(return_row, 'sale_type', '') or '').strip(), return_row.ticket),
            'url': '',
        }

        return jsonify({
            'ok': True,
            'return_ticket': return_row.ticket,
            'new_ticket': sale_row.ticket,
            'items': {
                'return': _serialize_sale(return_row, related=related_for_return),
                'sale': _serialize_sale(sale_row, related=related_for_sale),
            }
        })

    return jsonify({'ok': False, 'error': 'ticket_duplicate', 'message': 'No se pudo registrar el cambio: ticket duplicado.'}), 400


def _next_ticket():
    """Secuencia numérica para ventas/pagos: #0001, #0002, ... (ignora #Cxxxx)."""
    try:
        cid = _company_id()
        if not cid:
            return '#0001'
        rows = db.session.query(Sale.ticket).filter(Sale.company_id == cid).filter(Sale.ticket.like('#%')).all()
        max_n = 0
        for (t,) in (rows or []):
            s = str(t or '').strip()
            if not s.startswith('#') or s.startswith('#C'):
                continue
            digits = ''.join([ch for ch in s[1:] if ch.isdigit()])
            if not digits:
                continue
            try:
                max_n = max(max_n, int(digits))
            except Exception:
                current_app.logger.exception('Failed to parse ticket digits')
                continue
        return '#' + str(max_n + 1).zfill(4)
    except Exception:
        current_app.logger.exception('Failed to generate next ticket')
        return '#0001'


def _num(v):
    if v is None:
        return 0.0
    try:
        return float(v)
    except Exception:
        try:
            current_app.logger.exception('Failed to convert value to float')
        except Exception:
            pass
        return 0.0


def _last_cash_event_sale(cid: str, d: dt_date, shift_code: str = 'turno_1'):
    try:
        start_dt, end_dt, _ = _get_shift_window(cid, d, shift_code)
        q = (
            db.session.query(func.max(Sale.created_at))
            .outerjoin(SalePayment, and_(SalePayment.company_id == cid, SalePayment.sale_id == Sale.id))
            .filter(Sale.company_id == cid)
            .filter(Sale.status.notin_(['Reemplazada', 'voided', 'Anulado']))
            .filter(Sale.sale_type.in_(['Venta', 'CobroVenta', 'CobroCC', 'CobroCuota', 'Cambio', 'Devolucion', 'Devolución', 'Pago']))
            .filter(
                or_(
                    SalePayment.method == 'cash',
                    Sale.payment_method == 'Efectivo',
                )
            )
        )
        q = _apply_dt_window(q, Sale.created_at, start_dt, end_dt)
        return q.scalar()
    except Exception:
        return None


def _last_cash_event_expense(cid: str, d: dt_date, shift_code: str = 'turno_1'):
    try:
        start_dt, end_dt, _ = _get_shift_window(cid, d, shift_code)
        return (
            db.session.query(func.max(Expense.created_at))
            .filter(Expense.company_id == cid)
            .filter(Expense.payment_method == 'Efectivo')
            .filter(Expense.created_at >= start_dt)
            .filter(Expense.created_at < end_dt)
            .scalar()
        )
    except Exception:
        return None


def _last_cash_event_withdrawal(cid: str, d: dt_date, shift_code: str = 'turno_1'):
    try:
        from app.models import CashWithdrawal
        start_dt, end_dt, _ = _get_shift_window(cid, d, shift_code)
        return (
            db.session.query(func.max(CashWithdrawal.fecha_registro))
            .filter(CashWithdrawal.company_id == cid)
            .filter(CashWithdrawal.fecha_registro >= start_dt)
            .filter(CashWithdrawal.fecha_registro < end_dt)
            .scalar()
        )
    except Exception:
        return None


def _cash_expected_now(cid: str, d: dt_date, shift_code: str = 'turno_1') -> float:
    ventas = _cash_sales_total(cid, d, shift_code)
    egresos = _cash_expenses_total(cid, d, shift_code)
    retiros = _cash_withdrawals_total(cid, d, shift_code)
    total = float((ventas or 0.0) - (egresos or 0.0) - (retiros or 0.0))
    try:
        current_app.logger.info(
            'cash_count expected_now cid=%s date=%s ventas_efectivo=%s egresos_efectivo=%s retiros_efectivo=%s total=%s',
            str(cid),
            (d.isoformat() if d else None),
            float(ventas or 0.0),
            float(egresos or 0.0),
            float(retiros or 0.0),
            float(total or 0.0),
        )
    except Exception:
        pass
    return total


def _last_cash_event_now(cid: str, d: dt_date, shift_code: str = 'turno_1'):
    a = _last_cash_event_sale(cid, d, shift_code)
    b = _last_cash_event_expense(cid, d, shift_code)
    c = _last_cash_event_withdrawal(cid, d, shift_code)
    candidates = [x for x in [a, b, c] if x is not None]
    if not candidates:
        return None
    try:
        return max(candidates)
    except Exception:
        return None


def _int_or_none(v):
    try:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        return int(s)
    except Exception:
        current_app.logger.exception('Failed to convert value to int')
        return None


def _apply_inventory_for_sale(*, sale_ticket: str, sale_date: dt_date, items: List[Dict[str, Any]]):
    """Aplica impacto de inventario según los items (direction out/in).

    - out: consume FIFO en InventoryLot (qty_available)
    - in: crea un lote nuevo (devolución) y suma stock
    """
    cid = _company_id()
    for it in (items if isinstance(items, list) else []):
        d = it if isinstance(it, dict) else {}
        direction = str(d.get('direction') or 'out').strip().lower() or 'out'
        pid = _int_or_none(d.get('product_id'))
        if not pid:
            continue

        qty = _num(d.get('cantidad') if d.get('cantidad') is not None else d.get('qty'))
        if qty <= 0:
            continue

        prod = db.session.get(Product, pid)
        if not prod or not prod.active:
            continue
        if cid and str(getattr(prod, 'company_id', '') or '') != cid:
            continue

        if bool(getattr(prod, 'stock_ilimitado', False)):
            # Stock ilimitado: no afecta lotes, pero registramos CMV por referencia para reportes.
            ref_cost = getattr(prod, 'costo_unitario_referencia', None)
            unit_cost = float(ref_cost) if ref_cost is not None else 0.0
            db.session.add(InventoryMovement(
                company_id=cid,
                movement_date=sale_date,
                type='sale',
                sale_ticket=sale_ticket,
                product_id=pid,
                lot_id=None,
                qty_delta=-qty,
                unit_cost=unit_cost,
                total_cost=qty * unit_cost,
            ))
            continue

        if direction == 'in':
            # Devolución: entra stock. Creamos lote propio para trazabilidad.
            last_cost = (
                db.session.query(InventoryLot.unit_cost)
                .filter(InventoryLot.company_id == cid)
                .filter(InventoryLot.product_id == pid)
                .order_by(InventoryLot.received_at.desc(), InventoryLot.id.desc())
                .first()
            )
            unit_cost = float(last_cost[0]) if last_cost and last_cost[0] is not None else 0.0
            lot = InventoryLot(
                company_id=cid,
                product_id=pid,
                qty_initial=qty,
                qty_available=qty,
                unit_cost=unit_cost,
                received_at=datetime.utcnow(),
                origin_sale_ticket=sale_ticket,
            )
            db.session.add(lot)
            db.session.flush()
            db.session.add(InventoryMovement(
                company_id=cid,
                movement_date=sale_date,
                type='return',
                sale_ticket=sale_ticket,
                product_id=pid,
                lot_id=lot.id,
                qty_delta=qty,
                unit_cost=unit_cost,
                total_cost=qty * unit_cost,
            ))
            continue

        # direction out: consume FIFO
        remaining = qty
        lots = (
            db.session.query(InventoryLot)
            .filter(InventoryLot.company_id == cid)
            .filter(InventoryLot.product_id == pid)
            .filter(InventoryLot.qty_available > 0)
            .order_by(InventoryLot.received_at.asc(), InventoryLot.id.asc())
            .with_for_update()
            .all()
        )
        total_available = sum(float(l.qty_available or 0) for l in lots)
        if total_available + 1e-9 < remaining:
            raise ValueError(f"Stock insuficiente para {prod.name} (disponible: {total_available})")

        for lot in lots:
            if remaining <= 0:
                break
            avail = float(lot.qty_available or 0)
            if avail <= 0:
                continue
            take = avail if avail <= remaining else remaining
            lot.qty_available = avail - take
            remaining -= take
            unit_cost = float(lot.unit_cost or 0)
            db.session.add(InventoryMovement(
                company_id=cid,
                movement_date=sale_date,
                type='sale',
                sale_ticket=sale_ticket,
                product_id=pid,
                lot_id=lot.id,
                qty_delta=-take,
                unit_cost=unit_cost,
                total_cost=take * unit_cost,
            ))


def _revert_inventory_for_ticket(ticket: str):
    """Revierte movimientos y lotes asociados a un ticket."""
    t = str(ticket or '').strip()
    if not t:
        return

    cid = _company_id()
    if not cid:
        return
    movs = (
        db.session.query(InventoryMovement)
        .filter(InventoryMovement.company_id == cid)
        .filter(InventoryMovement.sale_ticket == t)
        .order_by(InventoryMovement.id.asc())
        .with_for_update()
        .all()
    )
    for m in movs:
        if m.lot_id:
            lot = db.session.get(InventoryLot, int(m.lot_id))
            if lot and str(getattr(lot, 'company_id', '') or '') == cid:
                lot.qty_available = float(lot.qty_available or 0) - float(m.qty_delta or 0)
                # Si era lote creado por devolución de este ticket y queda vacío, lo eliminamos.
                if (lot.origin_sale_ticket or '') == t and float(lot.qty_available or 0) <= 1e-9:
                    db.session.delete(lot)
        db.session.delete(m)


def _revert_installment_payment_by_sale_id(*, cid: str, paid_sale_id: int):
    try:
        sid = int(paid_sale_id or 0)
    except Exception:
        sid = 0
    if sid <= 0:
        return

    rows = (
        db.session.query(Installment)
        .filter(Installment.company_id == cid)
        .filter(Installment.paid_sale_id == sid)
        .with_for_update()
        .all()
    )
    for it in (rows or []):
        try:
            it.status = 'pendiente'
        except Exception:
            pass
        try:
            it.paid_at = None
        except Exception:
            pass
        try:
            it.paid_payment_method = None
        except Exception:
            pass
        try:
            it.paid_sale_id = None
        except Exception:
            pass


def _delete_sale_full(*, cid: str, ticket: str, visited: set[str]):
    t = str(ticket or '').strip()
    if not t:
        return
    if t in visited:
        return
    visited.add(t)

    row = (
        db.session.query(Sale)
        .filter(Sale.company_id == cid, Sale.ticket == t)
        .with_for_update()
        .first()
    )
    if not row:
        return

    try:
        st = str(getattr(row, 'sale_type', '') or '').strip()
        notes = str(getattr(row, 'notes', '') or '')
        if st in ('AjusteInvCosto', 'IngresoAjusteInv') or ('AdjustmentId:' in notes):
            return
    except Exception:
        return

    related_ticket = ''
    try:
        note_rel = str(getattr(row, 'notes', '') or '').strip()
        if note_rel:
            mrel = re.search(r"Relacionado\s+a\s+(?:venta|cambio)\s+([^\n\r]+)", note_rel, re.IGNORECASE)
            if mrel and mrel.group(1):
                related_ticket = str(mrel.group(1)).strip()
    except Exception:
        current_app.logger.exception('Failed to parse related ticket from notes')
        related_ticket = ''

    # Eliminar en forma bidireccional: si borran un Cobro* desde Movimientos,
    # también debe borrarse la Venta/Cambio referenciado por nota "Ticket #XXXX".
    try:
        st_row = str(getattr(row, 'sale_type', '') or '').strip()
    except Exception:
        st_row = ''
    if st_row in ('CobroVenta', 'CobroCC', 'CobroCuota'):
        try:
            note = str(getattr(row, 'notes', '') or '')
            m = re.search(r"Ticket\s*(?:original\s*)?#\s*(#+?\w+)", note, re.IGNORECASE)
            ref = (m.group(1) if (m and m.group(1)) else '').strip()
        except Exception:
            ref = ''
        if ref:
            ref = '#' + ref.lstrip('#')
        if ref and ref != t:
            related_ticket = ref

    try:
        if str(getattr(row, 'sale_type', '') or '').strip() == 'CobroCC':
            note = str(getattr(row, 'notes', '') or '').strip()
            ref = ''
            m = re.search(r"Ticket\s+([^\)\n\r]+)", note)
            if m and m.group(1):
                ref = str(m.group(1)).strip()
            if ref:
                orig = db.session.query(Sale).filter(Sale.company_id == cid, Sale.ticket == ref).with_for_update().first()
                if orig:
                    amt = abs(float(getattr(row, 'total', 0.0) or 0.0))
                    orig.paid_amount = max(0.0, float(orig.paid_amount or 0.0) - amt)
                    orig.due_amount = max(0.0, float(orig.due_amount or 0.0) + amt)
                    orig.on_account = bool(orig.due_amount and float(orig.due_amount or 0.0) > 0)
                    prev = str(orig.notes or '').strip()
                    extra = f"Cobro CC revertido por eliminación de {t}".strip()
                    orig.notes = (prev + ('\n' if prev else '') + extra) if extra else (prev or None)
    except Exception:
        current_app.logger.exception('Failed to revert CobroCC side-effects')

    try:
        _revert_installment_payment_by_sale_id(cid=cid, paid_sale_id=int(getattr(row, 'id', 0) or 0))
    except Exception:
        current_app.logger.exception('Failed to revert CobroCuota side-effects')

    paid_sale_ids: set[int] = set()
    try:
        plans = (
            db.session.query(InstallmentPlan)
            .filter(InstallmentPlan.company_id == cid)
            .filter(or_(InstallmentPlan.sale_id == row.id, InstallmentPlan.sale_ticket == t))
            .with_for_update()
            .all()
        )
    except Exception:
        plans = []

    for plan in (plans or []):
        insts = (
            db.session.query(Installment)
            .filter(Installment.company_id == cid)
            .filter(Installment.plan_id == plan.id)
            .with_for_update()
            .all()
        )
        for it in (insts or []):
            try:
                psid = int(getattr(it, 'paid_sale_id', 0) or 0)
            except Exception:
                psid = 0
            if psid > 0:
                paid_sale_ids.add(psid)
            db.session.delete(it)
        db.session.delete(plan)

    # Si esta venta tiene cobros asociados (tickets CobroVenta/CobroCC/CobroCuota), eliminarlos también.
    # El vínculo es por nota: "... Ticket #<ref> ...".
    try:
        st_row = str(getattr(row, 'sale_type', '') or '').strip()
        if st_row not in ('CobroVenta', 'CobroCC', 'CobroCuota'):
            t_norm = str(t or '').strip()
            if t_norm and not t_norm.startswith('#'):
                t_norm = '#' + t_norm
            payments = (
                db.session.query(Sale)
                .filter(Sale.company_id == cid)
                .filter(Sale.sale_type.in_(['CobroVenta', 'CobroCC', 'CobroCuota']))
                .filter(Sale.notes.isnot(None))
                .filter(Sale.notes.ilike('%Ticket%'))
                .with_for_update()
                .all()
            )
            for pay in (payments or []):
                try:
                    note = str(getattr(pay, 'notes', '') or '')
                    m = re.search(r"Ticket\s*(?:original\s*)?#\s*(#+?\w+)", note, re.IGNORECASE)
                    ref = (m.group(1) if (m and m.group(1)) else '').strip()
                except Exception:
                    ref = ''

                if ref:
                    ref = '#' + ref.lstrip('#')

                if not ref or ref != t_norm:
                    continue

                pt = str(getattr(pay, 'ticket', '') or '').strip()
                if pt and pt != t:
                    visited.add(pt)
                db.session.delete(pay)
    except Exception:
        current_app.logger.exception('Failed to delete associated payment tickets')

    if related_ticket and related_ticket != t:
        try:
            _delete_sale_full(cid=cid, ticket=related_ticket, visited=visited)
        except Exception:
            current_app.logger.exception('Failed to delete related ticket')

    try:
        _revert_inventory_for_ticket(t)
    except Exception:
        current_app.logger.exception('Failed to revert inventory for ticket')

    for psid in sorted(paid_sale_ids):
        if psid <= 0:
            continue
        if int(getattr(row, 'id', 0) or 0) == psid:
            continue
        pay_row = db.session.get(Sale, psid)
        if not pay_row:
            continue
        if str(getattr(pay_row, 'company_id', '') or '') != cid:
            continue
        try:
            _revert_installment_payment_by_sale_id(cid=cid, paid_sale_id=psid)
        except Exception:
            pass
        try:
            _revert_inventory_for_ticket(str(getattr(pay_row, 'ticket', '') or '').strip())
        except Exception:
            pass
        db.session.delete(pay_row)

    db.session.delete(row)


def _mark_sale_replaced(*, ticket: str, replaced_by: str):
    t = str(ticket or '').strip()
    if not t:
        return
    cid = _company_id()
    if not cid:
        return
    row = db.session.query(Sale).filter(Sale.company_id == cid, Sale.ticket == t).first()
    if not row:
        return
    row.status = 'Reemplazada'
    note = str(row.notes or '').strip()
    extra = f"Reemplazada por {replaced_by}" if replaced_by else 'Reemplazada'
    row.notes = (note + ('\n' if note else '') + extra) if extra else (note or None)


def _next_exchange_ticket() -> str:
    """Secuencia independiente para cambios: #C0001, #C0002, ..."""
    try:
        cid = _company_id()
        if not cid:
            return '#C0001'
        rows = db.session.query(Sale.ticket).filter(Sale.company_id == cid).filter(Sale.ticket.like('#C%')).all()
        max_n = 0
        for (t,) in (rows or []):
            s = str(t or '').strip()
            if not s.startswith('#C'):
                continue
            digits = ''.join([ch for ch in s[2:] if ch.isdigit()])
            if not digits:
                continue
            try:
                max_n = max(max_n, int(digits))
            except Exception:
                current_app.logger.exception('Failed to parse exchange ticket digits')
                continue
        return '#C' + str(max_n + 1).zfill(4)
    except Exception:
        current_app.logger.exception('Failed to generate next exchange ticket')
        return '#C0001'


@bp.post('/api/sales')
@login_required
@module_required('sales')
def create_sale():
    _ensure_sale_employee_columns()
    _ensure_sale_surcharge_columns()
    _ensure_sale_cmv_flags_columns()
    _ensure_installment_plan_columns()
    _ensure_sale_payments_table()
    payload = request.get_json(silent=True) or {}
    sale_date = _parse_date_iso(payload.get('fecha') or payload.get('date'), dt_date.today())
    sale_type = str(payload.get('type') or 'Venta').strip() or 'Venta'
    status = str(payload.get('status') or ('Cambio' if sale_type == 'Cambio' else 'Completada')).strip() or 'Completada'
    payment_method = str(payload.get('payment_method') or 'Efectivo').strip() or 'Efectivo'

    payments = None
    try:
        payments = _parse_payments_payload(payload)
    except ValueError as e:
        return jsonify({'ok': False, 'error': str(e)}), 400
    except Exception:
        payments = None

    inst_raw = payload.get('installments')
    inst = inst_raw if isinstance(inst_raw, dict) else {}
    inst_enabled = bool(inst.get('enabled'))
    inst_mode = str(inst.get('mode') or inst.get('installments_mode') or inst.get('installmentsMode') or 'fixed').strip().lower() or 'fixed'

    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    _ensure_sale_ticket_numbering()

    raw_emp_id = str(payload.get('employee_id') or '').strip() or None
    raw_emp_name = str(payload.get('employee_name') or '').strip() or None
    emp_id, emp_name = _resolve_employee_fields(cid=cid, employee_id=raw_emp_id, employee_name=raw_emp_name)

    items = payload.get('items')
    items_list = items if isinstance(items, list) else []

    is_gift = bool(payload.get('is_gift'))
    gift_code_raw = str(payload.get('gift_code') or '').strip() or None

    total_amount = _num(payload.get('total'))
    discount_general_pct = _num(payload.get('discount_general_pct'))
    discount_general_amount = _num(payload.get('discount_general_amount'))
    surcharge_general_pct = _num(payload.get('surcharge_general_pct') if payload.get('surcharge_general_pct') is not None else payload.get('general_surcharge_pct'))
    surcharge_general_amount = _num(payload.get('surcharge_general_amount') if payload.get('surcharge_general_amount') is not None else payload.get('surcharge_amount'))

    customer_id = str(payload.get('customer_id') or '').strip() or None
    customer_name = str(payload.get('customer_name') or '').strip() or None

    exchange_return_total = (None if payload.get('exchange_return_total') is None else _num(payload.get('exchange_return_total')))
    exchange_new_total = (None if payload.get('exchange_new_total') is None else _num(payload.get('exchange_new_total')))

    on_account = bool(payload.get('on_account'))
    paid_amount = _num(payload.get('paid_amount'))
    due_amount = _num(payload.get('due_amount'))

    # Validación pagos múltiples
    if payments is not None and str(sale_type or '').strip() == 'Venta':
        expected = float(total_amount or 0.0)
        if bool(on_account) or bool(inst_enabled):
            expected = float(paid_amount or 0.0)
        total_pays = _sum_payments(payments)
        if abs(float(expected) - float(total_pays)) > 0.01:
            return jsonify({'ok': False, 'error': 'payments_sum_mismatch'}), 400

    # Normalización:
    # - Movimientos (Ingresos) se basa en tickets CobroVenta/CobroCC/CobroCuota.
    # - Para ventas comunes (contado) necesitamos paid_amount > 0 para generar CobroVenta.
    try:
        st_norm = str(sale_type or '').strip()
    except Exception:
        st_norm = str(sale_type)
    if st_norm == 'Venta' and (not inst_enabled):
        # Si NO es cuenta corriente, forzar venta pagada completa.
        # El frontend a veces envía paid_amount=0/due_amount=0 para contado.
        if not on_account:
            paid_amount = float(total_amount or 0.0)
            due_amount = 0.0
        # Si es cuenta corriente pero no envió due_amount coherente, recalcular.
        else:
            try:
                paid_amount = max(0.0, min(float(paid_amount or 0.0), float(total_amount or 0.0)))
            except Exception:
                paid_amount = 0.0
            try:
                due_amount = max(0.0, float(total_amount or 0.0) - float(paid_amount or 0.0))
            except Exception:
                due_amount = 0.0

    payment_method, payments = _normalize_sale_payment_fields(
        sale_type=st_norm,
        on_account=bool(on_account),
        paid_amount=paid_amount,
        payment_method=payment_method,
        payments=payments,
        is_installments=bool(inst_enabled),
    )

    start_date = None
    interval_days = 30
    inst_count = 1
    first_payment_method = payment_method
    amounts: list[float] = []
    base_each = 0.0
    amount_per_period = 0.0

    if inst_enabled:
        _ensure_installments_tables()
        bs = BusinessSettings.get_for_company(cid)
        if not bs or not bool(getattr(bs, 'habilitar_sistema_cuotas', False)):
            return jsonify({'ok': False, 'error': 'installments_disabled'}), 400

        if inst_mode != 'indefinite':
            try:
                inst_count = int(inst.get('installments_count') or inst.get('installmentsCount') or inst.get('count') or 1)
            except Exception:
                inst_count = 1
            if inst_count < 1:
                inst_count = 1
            if inst_count > 24:
                return jsonify({'ok': False, 'error': 'installments_invalid', 'message': 'Máximo 24 cuotas.'}), 400

        try:
            interval_days = int(inst.get('interval_days') or inst.get('intervalDays') or 30)
        except Exception:
            interval_days = 30
        # Regla de negocio: intervalo válido 1..60 (default 30)
        if interval_days < 1:
            interval_days = 1
        if interval_days > 60:
            interval_days = 60

        start_date = _parse_date_iso(inst.get('start_date') or inst.get('startDate') or payload.get('fecha') or payload.get('date'), sale_date)
        first_payment_method = str(inst.get('first_payment_method') or inst.get('firstPaymentMethod') or payment_method).strip() or payment_method

        if inst_mode == 'indefinite':
            # Cuotas indefinidas = cobros recurrentes del total del carrito (sin división en cuotas).
            # El monto por período no se toma del frontend.
            amount_per_period = float(total_amount)
            base_each = float(amount_per_period)
            amounts = [float(amount_per_period)]

            on_account = True
            paid_amount = float(amount_per_period)
            due_amount = 0.0
        else:
            try:
                total_cents = int(round(float(total_amount or 0.0) * 100))
            except Exception:
                total_cents = 0
            base = total_cents // int(inst_count or 1)
            rem = total_cents - base * int(inst_count or 1)
            for i in range(int(inst_count or 1)):
                cents = base + (1 if i < rem else 0)
                amounts.append(float(cents) / 100.0)
            base_each = float(amounts[0] if amounts else 0.0)

            on_account = True
            paid_amount = float(amounts[0] if amounts else 0.0)
            due_amount = float(max(0.0, float(total_amount or 0.0) - float(paid_amount or 0.0)))

        if not customer_id and not customer_name:
            return jsonify({'ok': False, 'error': 'customer_required'}), 400

    row = None
    payment_sale = None
    cash_payment_sale = None
    attempted_ticket = None
    suggested_ticket = None

    attempts = 0
    max_attempts = 30
    base_n = _next_ticket_number(cid)
    while attempts < max_attempts:
        n = int(base_n or 1) + int(attempts)
        attempts += 1
        tk = _format_ticket_number(n)
        attempted_ticket = tk

        row = Sale(
            ticket=tk,
            ticket_number=n,
            company_id=cid,
            sale_date=sale_date,
            sale_type=sale_type,
            status=status,
            payment_method=payment_method,
            notes=str(payload.get('notes') or '').strip() or None,
            total=total_amount,
            discount_general_pct=discount_general_pct,
            discount_general_amount=discount_general_amount,
            general_surcharge_pct=surcharge_general_pct,
            surcharge_general_amount=surcharge_general_amount,
            on_account=bool(on_account),
            paid_amount=paid_amount,
            due_amount=due_amount,
            customer_id=customer_id,
            customer_name=customer_name,
            employee_id=emp_id,
            employee_name=emp_name,
            exchange_return_total=exchange_return_total,
            exchange_new_total=exchange_new_total,
        )

        if payments is not None and str(sale_type or '').strip() == 'Venta':
            try:
                row.payments = []
                for p in (payments or []):
                    row.payments.append(SalePayment(
                        company_id=cid,
                        method=str(p.get('method') or '').strip(),
                        amount=float(p.get('amount') or 0.0),
                    ))
                if len(payments) == 1:
                    row.payment_method = str(payments[0].get('method') or payment_method)
                elif len(payments) >= 2:
                    row.payment_method = ' + '.join([str(x.get('method')) for x in payments])
            except Exception:
                current_app.logger.exception('Failed to attach sale payments')
        if inst_enabled:
            try:
                row.is_installments = True
            except Exception:
                pass

        try:
            row.is_gift = is_gift
            gift_code = gift_code_raw
            if is_gift and not gift_code:
                gift_code = _make_gift_code(row.ticket, items_list)
            row.gift_code = gift_code
        except Exception:
            current_app.logger.exception('Failed to apply gift_code for sale')

        try:
            from flask_login import current_user
            row.created_by_user_id = int(getattr(current_user, 'id', 0) or 0) or None
        except Exception:
            current_app.logger.exception('Failed to set created_by_user_id for sale')
            row.created_by_user_id = None

        for it in items_list:
            d = it if isinstance(it, dict) else {}
            row.items.append(SaleItem(
                direction=str(d.get('direction') or 'out').strip() or 'out',
                product_id=str(d.get('product_id') or '').strip() or None,
                product_name=str(d.get('nombre') or d.get('product_name') or 'Producto').strip() or 'Producto',
                qty=_num(d.get('cantidad') if d.get('cantidad') is not None else d.get('qty')),
                unit_price=_num(d.get('precio') if d.get('precio') is not None else d.get('unit_price')),
                discount_pct=_num(d.get('descuento') if d.get('descuento') is not None else d.get('discount_pct')),
                subtotal=_num(d.get('subtotal')),
            ))

        db.session.add(row)
        try:
            db.session.flush()
        except IntegrityError:
            try:
                db.session.rollback()
            except Exception:
                pass
            row = None
            try:
                _ensure_sale_ticket_numbering()
            except Exception:
                pass
            try:
                base_n = _next_ticket_number(cid)
                suggested_ticket = _format_ticket_number(int(base_n or 1))
            except Exception:
                base_n = int(base_n or 1) + 1
                suggested_ticket = _format_ticket_number(int(base_n or 1))
            attempts = 0
            continue
        except Exception as e:
            current_app.logger.exception('Failed to flush sale')
            db.session.rollback()
            return jsonify({'ok': False, 'error': 'db_error', 'message': str(e)}), 400

        if inst_enabled:
            plan = InstallmentPlan(
                company_id=cid,
                sale_id=row.id,
                sale_ticket=row.ticket,
                customer_id=str(customer_id or customer_name or '').strip(),
                customer_name=str(customer_name or '').strip() or None,
                start_date=start_date or sale_date,
                interval_days=int(interval_days),
                installments_count=int(inst_count),
                is_indefinite=bool(inst_mode == 'indefinite'),
                amount_per_period=float(amount_per_period or 0.0),
                mode=('indefinite' if inst_mode == 'indefinite' else 'fixed'),
                total_amount=float(total_amount or 0.0),
                installment_amount=float(base_each or 0.0),
                first_payment_method=str(first_payment_method or '').strip() or None,
                status='activo',
            )
            db.session.add(plan)
            try:
                db.session.flush()
            except Exception as e:
                current_app.logger.exception('Failed to create installment plan')
                db.session.rollback()
                return jsonify({'ok': False, 'error': 'installments_plan_failed', 'message': str(e)}), 400

            inst_rows = []
            if inst_mode == 'indefinite':
                # Solo generamos el primer cobro y un próximo vencimiento a futuro.
                first_due = (start_date or sale_date)
                next_due = first_due + timedelta(days=int(interval_days))
                first_inst = Installment(
                    company_id=cid,
                    plan_id=plan.id,
                    installment_number=1,
                    due_date=first_due,
                    amount=float(amount_per_period or 0.0),
                    status='pagada',
                )
                first_inst.paid_at = datetime.utcnow()
                first_inst.paid_payment_method = str(first_payment_method)
                db.session.add(first_inst)
                inst_rows.append(first_inst)

                next_inst = Installment(
                    company_id=cid,
                    plan_id=plan.id,
                    installment_number=2,
                    due_date=next_due,
                    amount=float(amount_per_period or 0.0),
                    status='pendiente',
                )
                db.session.add(next_inst)
                inst_rows.append(next_inst)
            else:
                for i in range(1, int(inst_count) + 1):
                    due_d = (start_date or sale_date) + timedelta(days=(i - 1) * int(interval_days))
                    ir = Installment(
                        company_id=cid,
                        plan_id=plan.id,
                        installment_number=i,
                        due_date=due_d,
                        amount=float(amounts[i - 1] if i - 1 < len(amounts) else 0.0),
                        status=('pagada' if i == 1 else 'pendiente'),
                    )
                    if i == 1:
                        ir.paid_at = datetime.utcnow()
                        ir.paid_payment_method = str(first_payment_method)
                    db.session.add(ir)
                    inst_rows.append(ir)
            try:
                db.session.flush()
            except Exception as e:
                current_app.logger.exception('Failed to create installments')
                db.session.rollback()
                return jsonify({'ok': False, 'error': 'installments_create_failed', 'message': str(e)}), 400

            # Create payment ticket for first installment (cash impact)
            pay_base = _next_payment_number(cid)
            pay_attempts = 0
            payment_sale = None
            while pay_attempts < 10 and payment_sale is None:
                pay_n = int(pay_base) + int(pay_attempts)
                pay_attempts += 1
                pay_ticket = _format_ticket_number(pay_n, prefix='P')

                try:
                    note = _format_installment_payment_note(plan, inst_rows[0] if inst_rows else None)
                except Exception:
                    note = f"Cobro cuota N° 1 – Plan #{int(getattr(plan, 'id', 0) or 0)} – Cliente {str(getattr(row, 'customer_name', '') or '').strip() or '—'} – Ticket original #{row.ticket}"

                payment_sale = Sale(
                    ticket=pay_ticket,
                    ticket_number=None,
                    company_id=cid,
                    sale_date=sale_date,
                    sale_type='CobroCuota',
                    status='Completada',
                    payment_method=first_payment_method,
                    notes=note,
                    total=float(amounts[0] if amounts else 0.0),
                    discount_general_pct=0.0,
                    discount_general_amount=0.0,
                    on_account=False,
                    paid_amount=float(amounts[0] if amounts else 0.0),
                    due_amount=0.0,
                    customer_id=row.customer_id,
                    customer_name=_resolve_customer_display_name(cid, str(getattr(row, 'customer_id', '') or '').strip() or None, str(getattr(row, 'customer_name', '') or '').strip() or None),
                    employee_id=emp_id,
                    employee_name=emp_name,
                    exchange_return_total=None,
                    exchange_new_total=None,
                )
                try:
                    from flask_login import current_user
                    payment_sale.created_by_user_id = int(getattr(current_user, 'id', 0) or 0) or None
                except Exception:
                    payment_sale.created_by_user_id = None

                nested = None
                try:
                    nested = db.session.begin_nested()
                except Exception:
                    nested = None
                db.session.add(payment_sale)
                try:
                    db.session.flush()
                    if nested:
                        try:
                            nested.commit()
                        except Exception:
                            pass
                    break
                except IntegrityError:
                    try:
                        if nested:
                            nested.rollback()
                    except Exception:
                        pass
                    payment_sale = None
                    continue

            if not payment_sale:
                db.session.rollback()
                return jsonify({'ok': False, 'error': 'installments_payment_failed'}), 400

            try:
                inst_rows[0].paid_sale_id = int(payment_sale.id)
            except Exception:
                pass

        try:
            _apply_inventory_for_sale(sale_ticket=row.ticket, sale_date=sale_date, items=items_list)
        except ValueError as e:
            db.session.rollback()
            return jsonify({'ok': False, 'error': 'stock_insufficient', 'message': str(e)}), 400
        except Exception as e:
            current_app.logger.exception('Failed to apply inventory for sale')
            db.session.rollback()
            return jsonify({'ok': False, 'error': 'inventory_apply_failed', 'message': str(e)}), 400

        try:
            incomplete, reason = _compute_sale_cmv_incomplete(cid=cid, items=items_list)
            row.cmv_incomplete = bool(incomplete)
            row.cmv_incomplete_reason = str(reason or '').strip() or None
        except Exception:
            try:
                row.cmv_incomplete = False
                row.cmv_incomplete_reason = None
            except Exception:
                pass

        try:
            db.session.commit()
        except Exception as e:
            current_app.logger.exception('Failed to commit sale')
            db.session.rollback()
            return jsonify({'ok': False, 'error': 'db_error', 'message': str(e)}), 400

        try:
            if str(getattr(row, 'sale_type', '') or '').strip() == 'Venta':
                is_inst = bool(getattr(row, 'is_installments', False))
                is_cc = bool(getattr(row, 'on_account', False)) or float(getattr(row, 'due_amount', 0.0) or 0.0) > 0
                paid_now = float(getattr(row, 'paid_amount', 0.0) or 0.0)
                if paid_now > 1e-9:
                    if is_cc and (not is_inst):
                        settle_date = row.sale_date or dt_date.today()
                        base_n2 = _next_payment_number(cid)
                        ref = str(row.ticket or '').strip()
                        try:
                            note_products = _products_label_from_sale(row)
                        except Exception:
                            note_products = 'Producto —'
                        cust_txt = str(getattr(row, 'customer_name', '') or '').strip() or '—'
                        if ref:
                            note = f"Cobro cuenta corriente – Ticket #{ref} – {note_products} – Cliente {cust_txt}"
                        else:
                            note = f"Cobro cuenta corriente – {note_products} – Cliente {cust_txt}"
                        pay_attempts = 0
                        while pay_attempts < 10 and cash_payment_sale is None:
                            pn = int(base_n2) + int(pay_attempts)
                            pay_attempts += 1
                            pay_ticket = _format_ticket_number(pn, prefix='P')
                            ps = Sale(
                                ticket=pay_ticket,
                                ticket_number=None,
                                company_id=cid,
                                sale_date=settle_date,
                                sale_type='CobroCC',
                                status='Completada',
                                payment_method=str(getattr(row, 'payment_method', '') or 'Efectivo'),
                                notes=note,
                                total=abs(paid_now),
                                discount_general_pct=0.0,
                                discount_general_amount=0.0,
                                on_account=False,
                                paid_amount=abs(paid_now),
                                due_amount=0.0,
                                customer_id=row.customer_id,
                                customer_name=row.customer_name,
                                employee_id=getattr(row, 'employee_id', None),
                                employee_name=getattr(row, 'employee_name', None),
                                exchange_return_total=None,
                                exchange_new_total=None,
                            )
                            try:
                                for it in (row.items or []):
                                    db.session.add(SaleItem(
                                        company_id=cid,
                                        sale=ps,
                                        direction=str(getattr(it, 'direction', '') or 'out'),
                                        product_id=str(getattr(it, 'product_id', '') or '').strip() or None,
                                        product_name=str(getattr(it, 'product_name', '') or 'Producto'),
                                        qty=float(getattr(it, 'qty', 0.0) or 0.0),
                                        unit_price=float(getattr(it, 'unit_price', 0.0) or 0.0),
                                        discount_pct=float(getattr(it, 'discount_pct', 0.0) or 0.0),
                                        subtotal=float(getattr(it, 'subtotal', 0.0) or 0.0),
                                    ))
                            except Exception:
                                current_app.logger.exception('Failed to copy items to initial CobroCC')
                            try:
                                ps.created_by_user_id = int(getattr(row, 'created_by_user_id', 0) or 0) or None
                            except Exception:
                                ps.created_by_user_id = None
                            db.session.add(ps)
                            try:
                                db.session.commit()
                                cash_payment_sale = ps
                                break
                            except IntegrityError:
                                db.session.rollback()
                                continue
                            except Exception:
                                db.session.rollback()
                                current_app.logger.exception('Failed to commit initial CobroCC')
                                cash_payment_sale = None
                                break

                    if (not is_inst) and (not is_cc):
                        settle_date = row.sale_date or dt_date.today()
                        base_n2 = _next_payment_number(cid)
                        ref = str(row.ticket or '').strip()
                        try:
                            note_products = _products_label_from_sale(row)
                        except Exception:
                            note_products = 'Producto —'
                        cust_txt = str(getattr(row, 'customer_name', '') or '').strip() or '—'
                        if ref:
                            note = f"Cobro venta completa – Ticket #{ref} – {note_products} – Cliente {cust_txt}"
                        else:
                            note = f"Cobro venta completa – {note_products} – Cliente {cust_txt}"
                        pay_attempts = 0
                        while pay_attempts < 10 and cash_payment_sale is None:
                            pn = int(base_n2) + int(pay_attempts)
                            pay_attempts += 1
                            pay_ticket = _format_ticket_number(pn, prefix='P')
                            ps = Sale(
                                ticket=pay_ticket,
                                ticket_number=None,
                                company_id=cid,
                                sale_date=settle_date,
                                sale_type='CobroVenta',
                                status='Completada',
                                payment_method=str(getattr(row, 'payment_method', '') or 'Efectivo'),
                                notes=note,
                                total=abs(paid_now),
                                discount_general_pct=0.0,
                                discount_general_amount=0.0,
                                on_account=False,
                                paid_amount=abs(paid_now),
                                due_amount=0.0,
                                customer_id=row.customer_id,
                                customer_name=row.customer_name,
                                employee_id=getattr(row, 'employee_id', None),
                                employee_name=getattr(row, 'employee_name', None),
                                exchange_return_total=None,
                                exchange_new_total=None,
                            )
                            try:
                                for it in (row.items or []):
                                    db.session.add(SaleItem(
                                        company_id=cid,
                                        sale=ps,
                                        direction=str(getattr(it, 'direction', '') or 'out'),
                                        product_id=str(getattr(it, 'product_id', '') or '').strip() or None,
                                        product_name=str(getattr(it, 'product_name', '') or 'Producto'),
                                        qty=float(getattr(it, 'qty', 0.0) or 0.0),
                                        unit_price=float(getattr(it, 'unit_price', 0.0) or 0.0),
                                        discount_pct=float(getattr(it, 'discount_pct', 0.0) or 0.0),
                                        subtotal=float(getattr(it, 'subtotal', 0.0) or 0.0),
                                    ))
                            except Exception:
                                current_app.logger.exception('Failed to copy items to CobroVenta')
                            try:
                                ps.created_by_user_id = int(getattr(row, 'created_by_user_id', 0) or 0) or None
                            except Exception:
                                ps.created_by_user_id = None
                            db.session.add(ps)
                            try:
                                db.session.commit()
                                cash_payment_sale = ps
                                break
                            except IntegrityError:
                                db.session.rollback()
                                continue
                            except Exception:
                                db.session.rollback()
                                current_app.logger.exception('Failed to commit CobroVenta')
                                cash_payment_sale = None
                                break
        except Exception:
            current_app.logger.exception('Failed to create cash payment sale for created sale')
            try:
                db.session.rollback()
            except Exception:
                pass

        if inst_enabled:
            payload_out = {'ok': True, 'item': _serialize_sale(row), 'payment': _serialize_sale(payment_sale)}
            if cash_payment_sale is not None:
                payload_out['cash_payment'] = _serialize_sale(cash_payment_sale)
            return jsonify(payload_out), 201
        payload_out = {'ok': True, 'item': _serialize_sale(row)}
        if cash_payment_sale is not None:
            payload_out['cash_payment'] = _serialize_sale(cash_payment_sale)
        return jsonify(payload_out), 201

    if suggested_ticket is None:
        try:
            suggested_ticket = _format_ticket_number(int(_next_ticket_number(cid) or 1))
        except Exception:
            suggested_ticket = None
    return jsonify({
        'ok': False,
        'error': 'ticket_duplicate',
        'message': 'No se pudo registrar la venta: ticket duplicado.',
        'attempted_ticket': attempted_ticket,
        'suggested_ticket': suggested_ticket,
    }), 409


@bp.get('/api/installment-plans')
@login_required
@module_required_any('sales', 'customers')
def list_installment_plans():
    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400
    bs = BusinessSettings.get_for_company(cid)
    if not bs or not bool(getattr(bs, 'habilitar_sistema_cuotas', False)):
        return jsonify({'ok': False, 'error': 'installments_disabled'}), 400
    try:
        limit = int(request.args.get('limit') or 300)
    except Exception:
        limit = 300
    if limit <= 0 or limit > 20000:
        limit = 300
    status = str(request.args.get('status') or '').strip().lower()
    customer_id = str(request.args.get('customer_id') or '').strip()
    q = (
        db.session.query(InstallmentPlan)
        .options(selectinload(InstallmentPlan.sale).selectinload(Sale.items))
        .filter(InstallmentPlan.company_id == cid)
    )
    if status:
        q = q.filter(func.lower(InstallmentPlan.status) == status)
    if customer_id:
        q = q.filter(InstallmentPlan.customer_id == customer_id)
    q = q.order_by(InstallmentPlan.id.desc()).limit(limit)
    rows = q.all()
    items = []
    for p in (rows or []):
        try:
            is_indef = bool(getattr(p, 'is_indefinite', False)) or (str(getattr(p, 'mode', '') or '').strip().lower() == 'indefinite')
        except Exception:
            is_indef = False
        unlimited_price = None
        if is_indef:
            try:
                unlimited_price = _resolve_unlimited_plan_price(cid=cid, plan=p)
            except Exception:
                unlimited_price = None
        try:
            paid = 0.0
            pending = 0.0
            next_due = None
            for it in (getattr(p, 'installments', None) or []):
                st = str(getattr(it, 'status', '') or '').strip().lower()
                amt = float(getattr(it, 'amount', 0.0) or 0.0)
                if unlimited_price is not None and st != 'pagada':
                    amt = float(unlimited_price)
                if st == 'pagada':
                    paid += amt
                else:
                    pending += amt
                    dd = getattr(it, 'due_date', None)
                    if dd and (next_due is None or dd < next_due):
                        next_due = dd
        except Exception:
            paid = 0.0
            pending = 0.0
            next_due = None
        try:
            products_label = _products_label_from_sale(getattr(p, 'sale', None))
        except Exception:
            products_label = 'Producto —'

        items.append({
            'id': p.id,
            'sale_id': p.sale_id,
            'sale_ticket': p.sale_ticket or '',
            'customer_id': p.customer_id or '',
            'customer_name': p.customer_name or '',
            'products_label': products_label,
            'start_date': p.start_date.isoformat() if p.start_date else None,
            'interval_days': p.interval_days,
            'installments_count': p.installments_count,
            'is_indefinite': bool(getattr(p, 'is_indefinite', False)),
            'amount_per_period': float(unlimited_price if unlimited_price is not None else (getattr(p, 'amount_per_period', 0.0) or 0.0)),
            'mode': str(getattr(p, 'mode', '') or 'fixed'),
            'total_amount': p.total_amount,
            'installment_amount': float(unlimited_price if unlimited_price is not None else (getattr(p, 'installment_amount', 0.0) or 0.0)),
            'first_payment_method': p.first_payment_method or '',
            'status': p.status,
            'paid_amount': paid,
            'pending_amount': pending,
            'next_due_date': next_due.isoformat() if next_due else None,
            'created_at': _dt_to_ms(p.created_at),
            'updated_at': _dt_to_ms(p.updated_at),
        })
    return jsonify({'ok': True, 'items': items})


def _ensure_installments_enabled(cid: str) -> bool:
    try:
        bs = BusinessSettings.get_for_company(cid)
        return bool(bs and bool(getattr(bs, 'habilitar_sistema_cuotas', False)))
    except Exception:
        return False


def _pay_installment_row(cid: str, inst_row: Installment, pay_date: dt_date, payment_method: str, emp_id: str = None, emp_name: str = None):
    if not inst_row:
        return None, 'not_found'
    if str(getattr(inst_row, 'company_id', '') or '') != cid:
        return None, 'not_found'
    if str(getattr(inst_row, 'status', '') or '').strip().lower() == 'pagada':
        return None, 'already_paid'

    plan = db.session.query(InstallmentPlan).filter(InstallmentPlan.company_id == cid, InstallmentPlan.id == inst_row.plan_id).first()
    if not plan:
        return None, 'plan_not_found'
    if str(getattr(plan, 'status', '') or '').strip().lower() not in ('activo', 'active', 'activa'):
        return None, 'plan_inactive'

    is_indef = False
    try:
        is_indef = bool(getattr(plan, 'is_indefinite', False)) or (str(getattr(plan, 'mode', '') or '').strip().lower() == 'indefinite')
    except Exception:
        is_indef = False

    # Cuotas ilimitadas: el monto se deriva del precio vigente del producto.
    if is_indef:
        try:
            price = _resolve_unlimited_plan_price(cid=cid, plan=plan)
        except Exception:
            price = None
        if price is not None and float(price) > 0:
            try:
                inst_row.amount = float(price)
            except Exception:
                pass

    _ensure_sale_ticket_numbering()
    base_n = _next_ticket_number(cid)
    attempts = 0
    payment_sale = None
    while attempts < 10 and payment_sale is None:
        n = base_n + attempts
        attempts += 1
        tk = _format_ticket_number(n)
        note = _format_installment_payment_note(plan, inst_row)

        cust_id = str(getattr(plan, 'customer_id', None) or '').strip() or None
        cust_name = _resolve_customer_display_name(cid, cust_id, str(getattr(plan, 'customer_name', None) or '').strip() or None)

        amount = float(getattr(inst_row, 'amount', 0.0) or 0.0)
        payment_type = 'CobroCuota'
        if is_indef:
            payment_type = 'Venta'

        payment_sale = Sale(
            ticket=tk,
            ticket_number=n,
            company_id=cid,
            sale_date=pay_date,
            sale_type=payment_type,
            status='Completada',
            payment_method=payment_method,
            notes=note,
            total=float(amount or 0.0),
            discount_general_pct=0.0,
            discount_general_amount=0.0,
            on_account=False,
            paid_amount=float(amount or 0.0),
            due_amount=0.0,
            customer_id=cust_id,
            customer_name=cust_name,
            employee_id=emp_id,
            employee_name=emp_name,
            exchange_return_total=None,
            exchange_new_total=None,
        )
        try:
            from flask_login import current_user
            payment_sale.created_by_user_id = int(getattr(current_user, 'id', 0) or 0) or None
        except Exception:
            payment_sale.created_by_user_id = None

        db.session.add(payment_sale)
        try:
            src_sale = None
            try:
                sid = int(getattr(plan, 'sale_id', 0) or 0)
            except Exception:
                sid = 0
            if sid > 0:
                src_sale = db.session.query(Sale).options(selectinload(Sale.items)).filter(Sale.company_id == cid, Sale.id == sid).first()
            if src_sale:
                for it in (src_sale.items or []):
                    db.session.add(SaleItem(
                        company_id=cid,
                        sale=payment_sale,
                        direction=str(getattr(it, 'direction', '') or 'out'),
                        product_id=str(getattr(it, 'product_id', '') or '').strip() or None,
                        product_name=str(getattr(it, 'product_name', '') or 'Producto'),
                        qty=float(getattr(it, 'qty', 0.0) or 0.0),
                        unit_price=float(getattr(it, 'unit_price', 0.0) or 0.0),
                        discount_pct=float(getattr(it, 'discount_pct', 0.0) or 0.0),
                        subtotal=float(getattr(it, 'subtotal', 0.0) or 0.0),
                    ))
        except Exception:
            current_app.logger.exception('Failed to copy items to installment payment sale')
        try:
            db.session.flush()
        except IntegrityError:
            try:
                db.session.rollback()
            except Exception:
                pass
            payment_sale = None
            continue

    if not payment_sale:
        return None, 'ticket_duplicate'

    inst_row.status = 'pagada'
    inst_row.paid_at = datetime.utcnow()
    inst_row.paid_payment_method = payment_method
    try:
        inst_row.paid_sale_id = int(payment_sale.id)
    except Exception:
        inst_row.paid_sale_id = None

    try:
        all_paid = True
        for it in (plan.installments or []):
            if str(getattr(it, 'status', '') or '').strip().lower() != 'pagada':
                all_paid = False
                break
        if all_paid and not (bool(getattr(plan, 'is_indefinite', False)) or (str(getattr(plan, 'mode', '') or '').strip().lower() == 'indefinite')):
            plan.status = 'pagado'
    except Exception:
        pass

    # En modo indefinido, aseguramos que exista el próximo vencimiento.
    try:
        is_indef = bool(getattr(plan, 'is_indefinite', False)) or (str(getattr(plan, 'mode', '') or '').strip().lower() == 'indefinite')
        if is_indef:
            interval_days = int(getattr(plan, 'interval_days', 30) or 30)
            if interval_days < 1:
                interval_days = 1
            last_n = 0
            last_due = None
            pending_exists = False
            for it in (plan.installments or []):
                n = int(getattr(it, 'installment_number', 0) or 0)
                if n > last_n:
                    last_n = n
                    last_due = getattr(it, 'due_date', None)
                if str(getattr(it, 'status', '') or '').strip().lower() != 'pagada':
                    pending_exists = True

            if not pending_exists and last_due:
                next_due = last_due + timedelta(days=interval_days)
                try:
                    amt = _resolve_unlimited_plan_price(cid=cid, plan=plan)
                except Exception:
                    amt = None
                if amt is None:
                    amt = float(getattr(plan, 'amount_per_period', 0.0) or getattr(plan, 'installment_amount', 0.0) or 0.0)
                if amt > 0:
                    db.session.add(Installment(
                        company_id=cid,
                        plan_id=plan.id,
                        installment_number=int(last_n + 1),
                        due_date=next_due,
                        amount=amt,
                        status='pendiente',
                    ))
    except Exception:
        pass

    return payment_sale, None


@bp.get('/api/installment-plans/<int:plan_id>')
@login_required
@module_required_any('sales', 'customers')
def get_installment_plan(plan_id: int):
    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400
    pid = int(plan_id or 0)
    if pid <= 0:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    row = (
        db.session.query(InstallmentPlan)
        .options(selectinload(InstallmentPlan.sale).selectinload(Sale.items))
        .filter(InstallmentPlan.company_id == cid, InstallmentPlan.id == pid)
        .first()
    )
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    try:
        products_label = _products_label_from_sale(getattr(row, 'sale', None))
    except Exception:
        products_label = 'Producto —'

    unlimited_price = None
    try:
        is_indef = bool(getattr(row, 'is_indefinite', False)) or (str(getattr(row, 'mode', '') or '').strip().lower() == 'indefinite')
    except Exception:
        is_indef = False
    if is_indef:
        try:
            unlimited_price = _resolve_unlimited_plan_price(cid=cid, plan=row)
        except Exception:
            unlimited_price = None
    insts = []
    for it in sorted((row.installments or []), key=lambda x: int(getattr(x, 'installment_number', 0) or 0)):
        st = str(getattr(it, 'status', '') or '').strip().lower()
        amt = getattr(it, 'amount', None)
        if unlimited_price is not None and st != 'pagada':
            amt = float(unlimited_price)
        insts.append({
            'id': it.id,
            'installment_number': it.installment_number,
            'due_date': it.due_date.isoformat() if it.due_date else None,
            'amount': amt,
            'status': it.status,
            'paid_at': it.paid_at.isoformat() if it.paid_at else None,
            'paid_payment_method': it.paid_payment_method or '',
            'paid_sale_id': it.paid_sale_id,
        })
    item = {
        'id': row.id,
        'sale_id': row.sale_id,
        'sale_ticket': row.sale_ticket or '',
        'customer_id': row.customer_id or '',
        'customer_name': row.customer_name or '',
        'products_label': products_label,
        'start_date': row.start_date.isoformat() if row.start_date else None,
        'interval_days': row.interval_days,
        'installments_count': row.installments_count,
        'is_indefinite': bool(getattr(row, 'is_indefinite', False)),
        'amount_per_period': float(unlimited_price if unlimited_price is not None else (getattr(row, 'amount_per_period', 0.0) or 0.0)),
        'mode': str(getattr(row, 'mode', '') or 'fixed'),
        'total_amount': row.total_amount,
        'installment_amount': float(unlimited_price if unlimited_price is not None else (getattr(row, 'installment_amount', 0.0) or 0.0)),
        'first_payment_method': row.first_payment_method or '',
        'status': row.status,
        'created_at': _dt_to_ms(row.created_at),
        'updated_at': _dt_to_ms(row.updated_at),
        'installments': insts,
    }
    return jsonify({'ok': True, 'item': item})


@bp.post('/api/installments/<int:installment_id>/pay')
@login_required
@module_required_any('sales', 'customers')
def pay_installment(installment_id: int):
    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400
    if not _ensure_installments_enabled(cid):
        return jsonify({'ok': False, 'error': 'installments_disabled'}), 400

    iid = int(installment_id or 0)
    if iid <= 0:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    inst_row = db.session.query(Installment).filter(Installment.company_id == cid, Installment.id == iid).first()
    if not inst_row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    payload = request.get_json(silent=True) or {}
    pay_date = _parse_date_iso(payload.get('date') or payload.get('fecha'), dt_date.today())
    payment_method = str(payload.get('payment_method') or payload.get('forma_pago') or 'Efectivo').strip() or 'Efectivo'

    plan = None
    try:
        plan = (
            db.session.query(InstallmentPlan)
            .options(selectinload(InstallmentPlan.sale).selectinload(Sale.items))
            .filter(InstallmentPlan.company_id == cid, InstallmentPlan.id == inst_row.plan_id)
            .first()
        )
    except Exception:
        plan = None

    is_indef = False
    try:
        is_indef = bool(plan and (bool(getattr(plan, 'is_indefinite', False)) or (str(getattr(plan, 'mode', '') or '').strip().lower() == 'indefinite')))
    except Exception:
        is_indef = False

    if is_indef and plan:
        try:
            price = _resolve_unlimited_plan_price(cid=cid, plan=plan)
        except Exception:
            price = None
        if price is not None and float(price) > 0:
            inst_row.amount = float(price)
    elif (payload.get('amount') is not None and str(payload.get('amount')).strip() != ''):
        try:
            amt = float(payload.get('amount'))
        except Exception:
            amt = None
        if amt is not None and amt > 0:
            inst_row.amount = float(amt)

    _ensure_sale_ticket_numbering()
    raw_emp_id = str(payload.get('employee_id') or '').strip() or None
    raw_emp_name = str(payload.get('employee_name') or '').strip() or None
    emp_id, emp_name = _resolve_employee_fields(cid=cid, employee_id=raw_emp_id, employee_name=raw_emp_name)

    payment_sale, err = _pay_installment_row(cid=cid, inst_row=inst_row, pay_date=pay_date, payment_method=payment_method, emp_id=emp_id, emp_name=emp_name)
    if err:
        return jsonify({'ok': False, 'error': err}), 400

    try:
        db.session.commit()
    except Exception as e:
        current_app.logger.exception('Failed to commit installment payment')
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error', 'message': str(e)}), 400

    return jsonify({'ok': True, 'payment': _serialize_sale(payment_sale)})


@bp.post('/api/installment-plans/<int:plan_id>/pay')
@login_required
@module_required_any('sales', 'customers')
def pay_installment_plan(plan_id: int):
    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400
    if not _ensure_installments_enabled(cid):
        return jsonify({'ok': False, 'error': 'installments_disabled'}), 400

    pid = int(plan_id or 0)
    if pid <= 0:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    plan = db.session.query(InstallmentPlan).filter(InstallmentPlan.company_id == cid, InstallmentPlan.id == pid).first()
    if not plan:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    if str(getattr(plan, 'status', '') or '').strip().lower() not in ('activo', 'active', 'activa'):
        return jsonify({'ok': False, 'error': 'plan_inactive'}), 400

    payload = request.get_json(silent=True) or {}
    pay_date = _parse_date_iso(payload.get('date') or payload.get('fecha'), dt_date.today())
    payment_method = str(payload.get('payment_method') or payload.get('forma_pago') or plan.first_payment_method or 'Efectivo').strip() or 'Efectivo'
    try:
        count = int(payload.get('count') or 1)
    except Exception:
        count = 1
    if count < 1:
        count = 1
    if count > 24:
        count = 24

    raw_emp_id = str(payload.get('employee_id') or '').strip() or None
    raw_emp_name = str(payload.get('employee_name') or '').strip() or None
    emp_id, emp_name = _resolve_employee_fields(cid=cid, employee_id=raw_emp_id, employee_name=raw_emp_name)

    pending = [it for it in sorted((plan.installments or []), key=lambda x: int(getattr(x, 'installment_number', 0) or 0))
               if str(getattr(it, 'status', '') or '').strip().lower() != 'pagada']
    if not pending:
        return jsonify({'ok': False, 'error': 'no_pending'}), 400

    paid_sales = []
    pay_n = 0
    for it in pending:
        if pay_n >= count:
            break
        sale, err = _pay_installment_row(cid=cid, inst_row=it, pay_date=pay_date, payment_method=payment_method, emp_id=emp_id, emp_name=emp_name)
        if err:
            db.session.rollback()
            return jsonify({'ok': False, 'error': err}), 400
        paid_sales.append(_serialize_sale(sale))
        pay_n += 1

    try:
        db.session.commit()
    except Exception as e:
        current_app.logger.exception('Failed to commit installment plan payment')
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error', 'message': str(e)}), 400

    return jsonify({'ok': True, 'payments': paid_sales})
@login_required
@module_required_any('sales', 'customers')
def cancel_installment_plan(plan_id: int):
    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400
    if not _ensure_installments_enabled(cid):
        return jsonify({'ok': False, 'error': 'installments_disabled'}), 400

    pid = int(plan_id or 0)
    if pid <= 0:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    plan = db.session.query(InstallmentPlan).filter(InstallmentPlan.company_id == cid, InstallmentPlan.id == pid).first()
    if not plan:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    st = str(getattr(plan, 'status', '') or '').strip().lower()
    if st in ('cancelado', 'cancelada'):
        return jsonify({'ok': True, 'item': {'id': plan.id, 'status': plan.status}})
    if st in ('pagado', 'paid'):
        return jsonify({'ok': False, 'error': 'plan_paid'}), 400

    # Decidir si se puede borrar sin romper historial: si hay cuotas pagadas, mantener el plan cancelado.
    has_paid = False
    try:
        for it in (getattr(plan, 'installments', None) or []):
            st_it = str(getattr(it, 'status', '') or '').strip().lower()
            if st_it == 'pagada':
                has_paid = True
                break
    except Exception:
        has_paid = True

    try:
        if has_paid:
            # Keep paid installments for history; mark plan as cancelled.
            plan.status = 'cancelado'
        else:
            # Borrar cuotas y plan
            try:
                for it in (getattr(plan, 'installments', None) or []):
                    db.session.delete(it)
            except Exception:
                # Fallback por si la relación no está cargada
                db.session.query(Installment).filter(Installment.company_id == cid, Installment.plan_id == plan.id).delete(synchronize_session=False)
            db.session.delete(plan)

        db.session.commit()
    except Exception as e:
        current_app.logger.exception('Failed to cancel/delete installment plan')
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error', 'message': str(e)}), 400

    return jsonify({'ok': True, 'item': {'id': pid, 'status': 'cancelado', 'deleted': (not has_paid)}})


@bp.put('/api/sales/<ticket>')
@login_required
@module_required('sales')
def update_sale(ticket):
    _ensure_sale_employee_columns()
    _ensure_sale_surcharge_columns()
    _ensure_sale_payments_table()
    t = str(ticket or '').strip()
    cid = _company_id()
    row = db.session.query(Sale).filter(Sale.company_id == cid, Sale.ticket == t).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    try:
        if str(getattr(row, 'sale_type', '') or '').strip() == 'Cambio':
            return jsonify({'ok': False, 'error': 'locked', 'message': 'Los tickets de cambio son de solo lectura.'}), 403
    except Exception:
        pass

    try:
        st = str(getattr(row, 'sale_type', '') or '').strip()
        notes = str(getattr(row, 'notes', '') or '')
        if st in ('AjusteInvCosto', 'IngresoAjusteInv') or ('AdjustmentId:' in notes):
            return jsonify({'ok': False, 'error': 'locked'}), 400
    except Exception:
        pass

    payload = request.get_json(silent=True) or {}
    sale_date = _parse_date_iso(payload.get('fecha') or payload.get('date'), row.sale_date)
    sale_type = str(payload.get('type') or row.sale_type).strip() or row.sale_type
    status = str(payload.get('status') or row.status).strip() or row.status
    payment_method = str(payload.get('payment_method') or row.payment_method).strip() or row.payment_method
    payments = None
    try:
        payments = _parse_payments_payload(payload)
    except ValueError as e:
        return jsonify({'ok': False, 'error': str(e)}), 400
    except Exception:
        payments = None

    # Revertimos impacto de inventario anterior para recalcular con los nuevos items
    try:
        _revert_inventory_for_ticket(t)
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'inventory_revert_failed'}), 400

    row.sale_date = sale_date
    row.sale_type = sale_type
    row.status = status
    row.payment_method = payment_method
    row.notes = str(payload.get('notes') or '').strip() or None
    row.total = _num(payload.get('total'))
    row.discount_general_pct = _num(payload.get('discount_general_pct'))
    row.discount_general_amount = _num(payload.get('discount_general_amount'))
    try:
        row.general_surcharge_pct = _num(payload.get('surcharge_general_pct') if payload.get('surcharge_general_pct') is not None else payload.get('general_surcharge_pct'))
    except Exception:
        row.general_surcharge_pct = _num(payload.get('general_surcharge_pct'))
    try:
        row.surcharge_general_amount = _num(payload.get('surcharge_general_amount') if payload.get('surcharge_general_amount') is not None else payload.get('surcharge_amount'))
    except Exception:
        row.surcharge_general_amount = _num(payload.get('surcharge_general_amount'))
    row.on_account = bool(payload.get('on_account'))
    row.paid_amount = _num(payload.get('paid_amount'))
    row.due_amount = _num(payload.get('due_amount'))
    row.customer_id = str(payload.get('customer_id') or '').strip() or None
    row.customer_name = str(payload.get('customer_name') or '').strip() or None

    try:
        st_norm = str(sale_type or '').strip()
    except Exception:
        st_norm = str(sale_type or '')
    if st_norm == 'Venta':
        if not bool(row.on_account):
            row.paid_amount = float(row.total or 0.0)
            row.due_amount = 0.0
        else:
            try:
                row.paid_amount = max(0.0, min(float(row.paid_amount or 0.0), float(row.total or 0.0)))
            except Exception:
                row.paid_amount = 0.0
            try:
                row.due_amount = max(0.0, float(row.total or 0.0) - float(row.paid_amount or 0.0))
            except Exception:
                row.due_amount = 0.0

    payment_method, payments = _normalize_sale_payment_fields(
        sale_type=st_norm,
        on_account=bool(row.on_account),
        paid_amount=row.paid_amount,
        payment_method=payment_method,
        payments=payments,
        is_installments=bool(getattr(row, 'is_installments', False)),
    )
    row.payment_method = payment_method

    raw_emp_id = str(payload.get('employee_id') or '').strip() or None
    raw_emp_name = str(payload.get('employee_name') or '').strip() or None
    emp_id, emp_name = _resolve_employee_fields(cid=cid, employee_id=raw_emp_id, employee_name=raw_emp_name)
    row.employee_id = emp_id
    row.employee_name = emp_name

    row.exchange_return_total = (None if payload.get('exchange_return_total') is None else _num(payload.get('exchange_return_total')))
    row.exchange_new_total = (None if payload.get('exchange_new_total') is None else _num(payload.get('exchange_new_total')))

    if str(sale_type or '').strip() == 'Venta':
        expected = float(row.total or 0.0)
        if bool(row.on_account):
            expected = float(row.paid_amount or 0.0)
        total_pays = _sum_payments(payments)
        if abs(float(expected) - float(total_pays)) > 0.01:
            db.session.rollback()
            return jsonify({'ok': False, 'error': 'payments_sum_mismatch'}), 400

        try:
            db.session.query(SalePayment).filter(SalePayment.company_id == cid, SalePayment.sale_id == row.id).delete()
            for p in (payments or []):
                db.session.add(SalePayment(
                    company_id=cid,
                    sale_id=row.id,
                    method=str(p.get('method') or '').strip(),
                    amount=float(p.get('amount') or 0.0),
                ))
            if len(payments) == 1:
                row.payment_method = str(payments[0].get('method') or payment_method)
            elif len(payments) >= 2:
                row.payment_method = ' + '.join([str(x.get('method')) for x in payments])
            elif not payments:
                row.payment_method = None
        except Exception:
            current_app.logger.exception('Failed to update sale payments')

    is_gift = bool(payload.get('is_gift'))
    gift_code = str(payload.get('gift_code') or '').strip() or None
    try:
        row.is_gift = is_gift
        if is_gift and not gift_code:
            items = payload.get('items')
            items_list = items if isinstance(items, list) else []
            gift_code = _make_gift_code(row.ticket, items_list)
        row.gift_code = gift_code
    except Exception:
        current_app.logger.exception('Failed to apply gift_code while updating sale')

    row.items = []
    items = payload.get('items')
    items_list = items if isinstance(items, list) else []
    for it in items_list:
        d = it if isinstance(it, dict) else {}
        row.items.append(SaleItem(
            direction=str(d.get('direction') or 'out').strip() or 'out',
            product_id=str(d.get('product_id') or '').strip() or None,
            product_name=str(d.get('nombre') or d.get('product_name') or 'Producto').strip() or 'Producto',
            qty=_num(d.get('cantidad') if d.get('cantidad') is not None else d.get('qty')),
            unit_price=_num(d.get('precio') if d.get('precio') is not None else d.get('unit_price')),
            discount_pct=_num(d.get('descuento') if d.get('descuento') is not None else d.get('discount_pct')),
            subtotal=_num(d.get('subtotal')),
        ))

    try:
        db.session.flush()
        try:
            _apply_inventory_for_sale(sale_ticket=t, sale_date=sale_date, items=items_list)
        except IntegrityError:
            db.session.rollback()
            return jsonify({'ok': False, 'error': 'ticket_duplicate', 'message': 'Ticket duplicado.'}), 400
    except Exception as e:
        current_app.logger.exception('Failed to update sale')
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error', 'message': str(e)}), 400
    try:
        db.session.commit()
    except Exception as e:
        current_app.logger.exception('Failed to commit sale update')
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error', 'message': str(e)}), 400
    return jsonify({'ok': True, 'item': _serialize_sale(row)})


@bp.delete('/api/sales/<ticket>')
@login_required
@module_required('sales')
def delete_sale(ticket):
    t = str(ticket or '').strip()
    cid = _company_id()
    row = db.session.query(Sale).filter(Sale.company_id == cid, Sale.ticket == t).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found', 'message': 'Ticket no encontrado.'}), 404

    try:
        if str(getattr(row, 'sale_type', '') or '').strip() == 'Cambio':
            return jsonify({'ok': False, 'error': 'locked', 'message': 'Los tickets de cambio son de solo lectura.'}), 403
    except Exception:
        pass

    try:
        st = str(getattr(row, 'sale_type', '') or '').strip()
        notes = str(getattr(row, 'notes', '') or '')
        if st in ('AjusteInvCosto', 'IngresoAjusteInv') or ('AdjustmentId:' in notes):
            return jsonify({'ok': False, 'error': 'locked', 'message': 'Este ticket no se puede eliminar.'}), 400
    except Exception:
        pass
    try:
        visited: set[str] = set()
        _delete_sale_full(cid=cid, ticket=t, visited=visited)
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error', 'message': 'No se pudo eliminar el ticket.'}), 400
    return ('', 204)


@bp.get('/api/cash-count')
@login_required
@module_required('sales')
def get_cash_count():
    _ensure_cash_count_snapshot_column()
    raw = (request.args.get('date') or '').strip()
    try:
        d = dt_date.fromisoformat(raw) if raw else dt_date.today()
    except Exception:
        d = dt_date.today()

    cid = _company_id()
    if not cid:
        return jsonify({'ok': True, 'item': None})

    shift_enabled = _cash_count_shift_enabled(cid)
    shift_code = _normalize_cash_shift(request.args.get('shift') or request.args.get('shift_code'), shift_enabled)
    _, _, shift_info = _get_shift_window(cid, d, shift_code)

    row = (
        db.session.query(CashCount)
        .filter(CashCount.company_id == cid, CashCount.count_date == d, db.func.coalesce(CashCount.shift_code, 'turno_1') == shift_code)
        .first()
    )
    if not row:
        return jsonify({'ok': True, 'item': None})

    return jsonify({
        'ok': True,
        'item': {
            'date': row.count_date.isoformat(),
            'employee_id': row.employee_id,
            'employee_name': row.employee_name,
            'shift_code': getattr(row, 'shift_code', None) or ('turno_1' if shift_enabled else 'turno_unico'),
            'shift_label': ('Turno único' if not getattr(row, 'shift_code', None) else shift_info.get('label')),
            'shift_display': shift_info.get('display'),
            'opening_amount': row.opening_amount,
            'cash_day_amount': row.cash_day_amount,
            'closing_amount': row.closing_amount,
            'difference_amount': row.difference_amount,
            'updated_at': row.updated_at.isoformat() if row.updated_at else None,
        }
    })


@bp.post('/api/cash-count')
@login_required
@module_required('sales')
def save_cash_count():
    _ensure_cash_count_snapshot_column()
    payload = request.get_json(silent=True) or {}

    raw = str(payload.get('date') or payload.get('fecha') or '').strip()
    try:
        d = dt_date.fromisoformat(raw) if raw else dt_date.today()
    except Exception:
        d = dt_date.today()

    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    shift_enabled = _cash_count_shift_enabled(cid)
    shift_code = _normalize_cash_shift(payload.get('shift') or payload.get('shift_code'), shift_enabled)
    _, _, shift_info = _get_shift_window(cid, d, shift_code)

    def num(v):
        try:
            return float(v)
        except Exception:
            return 0.0

    opening_in = payload.get('opening_amount', None)
    cash_day_in = payload.get('cash_day_amount', None)
    closing_in = payload.get('closing_amount', None)

    opening = num(opening_in) if opening_in not in (None, '') else None
    cash_day = num(cash_day_in) if cash_day_in not in (None, '') else None
    closing = num(closing_in) if closing_in not in (None, '') else None

    employee_id_raw = payload.get('employee_id', None)
    employee_name_raw = payload.get('employee_name', None)
    employee_id = str(employee_id_raw or '').strip() or None if employee_id_raw not in (None, '') else None
    employee_name = str(employee_name_raw or '').strip() or None if employee_name_raw not in (None, '') else None

    if not employee_id:
        return jsonify({'ok': False, 'error': 'employee_required', 'message': 'Debés seleccionar un responsable de caja para guardar el arqueo.'}), 400

    row = (
        db.session.query(CashCount)
        .filter(CashCount.company_id == cid, CashCount.count_date == d, db.func.coalesce(CashCount.shift_code, 'turno_1') == shift_code)
        .first()
    )
    if not row:
        row = CashCount(count_date=d, company_id=cid, shift_code=shift_code)
        db.session.add(row)

    try:
        row.shift_code = shift_code
    except Exception:
        pass

    if 'employee_id' in payload and employee_id_raw not in (None, ''):
        row.employee_id = employee_id
    if 'employee_name' in payload and employee_name_raw not in (None, ''):
        row.employee_name = employee_name
    if 'opening_amount' in payload and opening is not None:
        row.opening_amount = opening
    if 'cash_day_amount' in payload and cash_day is not None:
        row.cash_day_amount = cash_day
        try:
            row.efectivo_calculado_snapshot = float(cash_day)
        except Exception:
            row.efectivo_calculado_snapshot = None
    if 'closing_amount' in payload and closing is not None:
        row.closing_amount = closing

    try:
        opening_final = float(getattr(row, 'opening_amount', 0.0) or 0.0)
    except Exception:
        opening_final = 0.0
    try:
        cash_day_final = float(getattr(row, 'cash_day_amount', 0.0) or 0.0)
    except Exception:
        cash_day_final = 0.0
    try:
        closing_final = float(getattr(row, 'closing_amount', 0.0) or 0.0)
    except Exception:
        closing_final = 0.0
    row.difference_amount = (opening_final + cash_day_final) - closing_final

    cash_expected = _cash_expected_now(cid, d, shift_code)
    last_event = _last_cash_event_now(cid, d, shift_code)
    try:
        row.cash_expected_at_save = float(cash_expected)
    except Exception:
        row.cash_expected_at_save = None
    try:
        row.last_cash_event_at_save = last_event
    except Exception:
        row.last_cash_event_at_save = None

    has_employee = bool(str(getattr(row, 'employee_id', '') or '').strip())
    has_cash_snapshot = row.cash_expected_at_save is not None
    has_closing = bool((getattr(row, 'closing_amount', 0.0) or 0.0) > 0)
    if has_employee and has_cash_snapshot and has_closing:
        row.status = 'final'
        if not getattr(row, 'done_at', None):
            row.done_at = datetime.utcnow()
    else:
        row.status = 'draft'
    try:
        from flask_login import current_user
        row.created_by_user_id = int(getattr(current_user, 'id', 0) or 0) or None
    except Exception:
        row.created_by_user_id = None

    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        try:
            existing = (
                db.session.query(CashCount)
                .filter(CashCount.company_id == cid, CashCount.count_date == d, db.func.coalesce(CashCount.shift_code, 'turno_1') == shift_code)
                .first()
            )
            if existing is None and (not shift_enabled) and shift_code == 'turno_1':
                existing = db.session.query(CashCount).filter(CashCount.company_id == cid, CashCount.count_date == d).first()
        except Exception:
            existing = None

        if existing:
            try:
                existing.company_id = cid
            except Exception:
                pass
            try:
                existing.shift_code = shift_code
            except Exception:
                pass
            existing.employee_id = employee_id
            existing.employee_name = employee_name
            existing.opening_amount = opening
            existing.cash_day_amount = cash_day
            existing.closing_amount = closing
            existing.difference_amount = row.difference_amount
            existing.created_by_user_id = row.created_by_user_id
            try:
                existing.cash_expected_at_save = row.cash_expected_at_save
                existing.last_cash_event_at_save = row.last_cash_event_at_save
                existing.status = row.status
                existing.done_at = row.done_at
                existing.efectivo_calculado_snapshot = row.efectivo_calculado_snapshot
            except Exception:
                pass
            try:
                db.session.commit()
                row = existing
            except Exception:
                db.session.rollback()
                current_app.logger.exception('Failed to commit existing cash_count row after IntegrityError fallback')
                return jsonify({'ok': False, 'error': 'db_error'}), 400
        else:
            return jsonify({'ok': False, 'error': 'already_exists', 'message': 'Ya existe un arqueo para esa fecha.'}), 400
    except Exception:
        current_app.logger.exception('Failed to save cash_count')
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400

    return jsonify({'ok': True, 'item': {'date': row.count_date.isoformat(), 'shift_code': getattr(row, 'shift_code', None) or 'turno_1', 'shift_label': shift_info.get('label'), 'shift_display': shift_info.get('display'), 'difference_amount': row.difference_amount}})


def _round2(v: float) -> float:
    try:
        return float(f"{float(v or 0.0):.2f}")
    except Exception:
        return 0.0


def _cash_sales_total(cid: str, d: dt_date, shift_code: str = 'turno_1') -> float:
    try:
        start_dt, end_dt, _ = _get_shift_window(cid, d, shift_code)
        # Fuente de verdad: SalePayment.method (pagos mixtos). Para ventas legacy sin SalePayment,
        # usar Sale.payment_method == 'Efectivo'.
        q_pay = (
            db.session.query(func.coalesce(func.sum(SalePayment.amount), 0.0))
            .select_from(Sale)
            .join(SalePayment, and_(SalePayment.company_id == cid, SalePayment.sale_id == Sale.id))
            .filter(Sale.company_id == cid)
            .filter(Sale.status == 'Completada')
            .filter(Sale.sale_type.in_(['Venta', 'CobroVenta', 'CobroCC', 'CobroCuota']))
            .filter(SalePayment.method == 'cash')
        )
        q_pay = _apply_dt_window(q_pay, Sale.created_at, start_dt, end_dt)
        total_pay = float(q_pay.scalar() or 0.0)

        q_legacy = (
            db.session.query(func.coalesce(func.sum(Sale.total), 0.0))
            .outerjoin(SalePayment, and_(SalePayment.company_id == cid, SalePayment.sale_id == Sale.id))
            .filter(Sale.company_id == cid)
            .filter(Sale.status == 'Completada')
            .filter(Sale.sale_type.in_(['Venta', 'CobroVenta', 'CobroCC', 'CobroCuota']))
            .filter(SalePayment.id.is_(None))
            .filter(Sale.payment_method == 'Efectivo')
        )
        q_legacy = _apply_dt_window(q_legacy, Sale.created_at, start_dt, end_dt)
        total_legacy = float(q_legacy.scalar() or 0.0)
        return float(total_pay + total_legacy)
    except Exception:
        return 0.0


def _cash_expenses_total(cid: str, d: dt_date, shift_code: str = 'turno_1') -> float:
    try:
        start_dt, end_dt, _ = _get_shift_window(cid, d, shift_code)
        q = (
            db.session.query(func.coalesce(func.sum(Expense.amount), 0.0))
            .filter(Expense.company_id == cid)
            .filter(Expense.payment_method == 'Efectivo')
        )
        q = q.filter(Expense.created_at >= start_dt).filter(Expense.created_at < end_dt)
        # Algunos schemas tienen status / tipo. Si existen, filtramos.
        try:
            if hasattr(Expense, 'status'):
                q = q.filter(Expense.status == 'completado')
        except Exception:
            pass
        try:
            if hasattr(Expense, 'expense_type'):
                q = q.filter(func.lower(Expense.expense_type) == 'egreso')
        except Exception:
            pass
        return float(q.scalar() or 0.0)
    except Exception:
        return 0.0


def _cash_withdrawals_total(cid: str, d: dt_date, shift_code: str = 'turno_1') -> float:
    try:
        from app.models import CashWithdrawal
        start_dt, end_dt, _ = _get_shift_window(cid, d, shift_code)
        q = (
            db.session.query(func.coalesce(func.sum(CashWithdrawal.monto), 0.0))
            .filter(CashWithdrawal.company_id == cid)
            .filter(CashWithdrawal.fecha_registro >= start_dt)
            .filter(CashWithdrawal.fecha_registro < end_dt)
        )
        return float(q.scalar() or 0.0)
    except Exception:
        return 0.0


@bp.get('/api/cash-count/calc')
@login_required
@module_required('sales')
def cash_count_calc_api():
    raw = (request.args.get('date') or '').strip()
    try:
        d = dt_date.fromisoformat(raw) if raw else dt_date.today()
    except Exception:
        d = dt_date.today()

    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    shift_enabled = _cash_count_shift_enabled(cid)
    shift_code = _normalize_cash_shift(request.args.get('shift') or request.args.get('shift_code'), shift_enabled)
    _, _, shift_info = _get_shift_window(cid, d, shift_code)

    ventas = _cash_sales_total(cid, d, shift_code)
    egresos = _cash_expenses_total(cid, d, shift_code)
    retiros = _cash_withdrawals_total(cid, d, shift_code)
    total = float((ventas or 0.0) - (retiros or 0.0) - (egresos or 0.0))

    try:
        current_app.logger.info(
            'cash_count calc_api cid=%s date=%s ventas_efectivo=%s retiros_efectivo=%s egresos_efectivo=%s total=%s',
            str(cid),
            (d.isoformat() if d else None),
            float(ventas or 0.0),
            float(retiros or 0.0),
            float(egresos or 0.0),
            float(total or 0.0),
        )
    except Exception:
        pass

    return jsonify({
        'ok': True,
        'date': d.isoformat(),
        'company_id': str(cid),
        'shift_code': shift_code,
        'shift_label': shift_info.get('label'),
        'shift_display': shift_info.get('display'),
        'ventas_efectivo': _round2(ventas),
        'retiros_efectivo': _round2(retiros),
        'egresos_efectivo': _round2(egresos),
        'efectivo_dia_calculado': _round2(total),
    })


@bp.get('/api/cash-register/status')
@login_required
@module_required('sales')
def cash_register_status():
    _ensure_cash_count_snapshot_column()
    raw = (request.args.get('date') or '').strip()
    try:
        d = dt_date.fromisoformat(raw) if raw else dt_date.today()
    except Exception:
        d = dt_date.today()

    cid = _company_id()
    if not cid:
        return jsonify({'ok': True, 'is_done': False, 'has_cash_count': False})

    row = db.session.query(CashCount).filter(CashCount.company_id == cid, CashCount.count_date == d).first()
    if not row:
        return jsonify({'ok': True, 'is_done': False, 'has_cash_count': False})

    snap = getattr(row, 'efectivo_calculado_snapshot', None)
    if snap is None:
        snap = getattr(row, 'cash_day_amount', None)
    try:
        snap_v = float(snap or 0.0)
    except Exception:
        snap_v = 0.0

    ventas = _cash_sales_total(cid, d)
    egresos = _cash_expenses_total(cid, d)
    retiros = _cash_withdrawals_total(cid, d)
    actual = ventas - egresos - retiros

    is_done = _round2(actual) == _round2(snap_v)
    return jsonify({
        'ok': True,
        'has_cash_count': True,
        'is_done': bool(is_done),
        'date': d.isoformat(),
        'efectivo_actual_calculado': _round2(actual),
        'efectivo_calculado_snapshot': _round2(snap_v),
    })


@bp.get('/api/cash-count/status')
@login_required
@module_required('sales')
def cash_count_status():
    _ensure_cash_count_snapshot_column()
    raw = (request.args.get('date') or '').strip()
    try:
        d = dt_date.fromisoformat(raw) if raw else dt_date.today()
    except Exception:
        d = dt_date.today()

    cid = _company_id()
    if not cid:
        return jsonify({'ok': True, 'date': d.isoformat(), 'has_record': False, 'status': 'none', 'is_valid': False, 'should_show': 'pendiente'})

    shift_enabled = _cash_count_shift_enabled(cid)
    shift_code = _normalize_cash_shift(request.args.get('shift') or request.args.get('shift_code'), shift_enabled)
    _, _, shift_info = _get_shift_window(cid, d, shift_code)

    row = (
        db.session.query(CashCount)
        .filter(CashCount.company_id == cid, CashCount.count_date == d, db.func.coalesce(CashCount.shift_code, 'turno_1') == shift_code)
        .first()
    )
    if not row:
        return jsonify({'ok': True, 'date': d.isoformat(), 'has_record': False, 'status': 'none', 'is_valid': False, 'should_show': 'pendiente', 'shift_code': shift_code, 'shift_label': shift_info.get('label'), 'shift_display': shift_info.get('display')})

    expected_now = _cash_expected_now(cid, d, shift_code)
    last_now = _last_cash_event_now(cid, d, shift_code)

    expected_save = getattr(row, 'cash_expected_at_save', None)
    if expected_save is None:
        expected_save = getattr(row, 'efectivo_calculado_snapshot', None)
    if expected_save is None:
        expected_save = getattr(row, 'cash_day_amount', None)

    try:
        expected_save_v = float(expected_save or 0.0)
    except Exception:
        expected_save_v = 0.0

    last_save = getattr(row, 'last_cash_event_at_save', None)

    st = str(getattr(row, 'status', '') or '').strip().lower()
    if st not in ('draft', 'final'):
        st = 'draft'

    same_cash = _round2(expected_now) == _round2(expected_save_v)
    time_ok = True
    if last_now is not None and last_save is not None:
        try:
            time_ok = last_now <= last_save
        except Exception:
            time_ok = False
    elif last_now is not None and last_save is None:
        time_ok = False

    is_valid = bool(st == 'final' and same_cash and time_ok)
    should_show = 'realizado' if is_valid else 'pendiente'

    return jsonify({
        'ok': True,
        'date': d.isoformat(),
        'has_record': True,
        'status': st,
        'is_valid': is_valid,
        'should_show': should_show,
        'apertura': float(getattr(row, 'opening_amount', 0.0) or 0.0),
        'cierre': float(getattr(row, 'closing_amount', 0.0) or 0.0),
        'responsable_id': getattr(row, 'employee_id', None),
        'cash_expected_now': _round2(expected_now),
        'cash_expected_at_save': _round2(expected_save_v),
        'last_cash_event_now': (last_now.isoformat() if last_now else None),
        'last_cash_event_at_save': (last_save.isoformat() if last_save else None),
        'done_at': (getattr(row, 'done_at', None).isoformat() if getattr(row, 'done_at', None) else None),
        'shift_code': getattr(row, 'shift_code', None) or 'turno_1',
        'shift_label': shift_info.get('label'),
        'shift_display': shift_info.get('display'),
    })
