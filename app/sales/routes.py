from datetime import date as dt_date, datetime, timedelta
from typing import Any, Dict, List, Optional
import re
import uuid
import json
import math
import os
import unicodedata

from flask import current_app, g, jsonify, render_template, request, url_for
from flask_login import login_required

from sqlalchemy import func, inspect, text, and_, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload, joinedload

from app import db
from app.models import BusinessSettings, CashCount, Category, Customer, Employee, Expense, Installment, InstallmentPlan, InventoryLot, InventoryMovement, Product, Sale, SaleItem, User
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


def _ensure_cash_count_snapshot_column() -> None:
    try:
        engine = db.engine
        insp = inspect(engine)
        if 'cash_count' not in set(insp.get_table_names() or []):
            return
        cols = {str(c.get('name') or '') for c in (insp.get_columns('cash_count') or [])}
        stmts = []
        if 'efectivo_calculado_snapshot' not in cols:
            if str(engine.url.drivername).startswith('sqlite'):
                stmts.append('ALTER TABLE cash_count ADD COLUMN efectivo_calculado_snapshot FLOAT')
            else:
                stmts.append('ALTER TABLE cash_count ADD COLUMN IF NOT EXISTS efectivo_calculado_snapshot DOUBLE PRECISION')
        if 'cash_expected_at_save' not in cols:
            if str(engine.url.drivername).startswith('sqlite'):
                stmts.append('ALTER TABLE cash_count ADD COLUMN cash_expected_at_save FLOAT')
            else:
                stmts.append('ALTER TABLE cash_count ADD COLUMN IF NOT EXISTS cash_expected_at_save DOUBLE PRECISION')
        if 'last_cash_event_at_save' not in cols:
            if str(engine.url.drivername).startswith('sqlite'):
                stmts.append('ALTER TABLE cash_count ADD COLUMN last_cash_event_at_save DATETIME')
            else:
                stmts.append('ALTER TABLE cash_count ADD COLUMN IF NOT EXISTS last_cash_event_at_save TIMESTAMP')
        if 'status' not in cols:
            if str(engine.url.drivername).startswith('sqlite'):
                stmts.append("ALTER TABLE cash_count ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'draft'")
            else:
                stmts.append("ALTER TABLE cash_count ADD COLUMN IF NOT EXISTS status VARCHAR(16) NOT NULL DEFAULT 'draft'")
        if 'done_at' not in cols:
            if str(engine.url.drivername).startswith('sqlite'):
                stmts.append('ALTER TABLE cash_count ADD COLUMN done_at DATETIME')
            else:
                stmts.append('ALTER TABLE cash_count ADD COLUMN IF NOT EXISTS done_at TIMESTAMP')
        if not stmts:
            return
        with engine.begin() as conn:
            for sql in stmts:
                conn.execute(text(sql))
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
        return str(getattr(g, 'company_id', '') or '').strip()
    except Exception:
        return ''


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
    return render_template('sales/list.html', title='Ventas')


def _serialize_sale(row: Sale, related: dict = None, users_map: dict | None = None):
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

    return {
        'id': row.id,
        'ticket': row.ticket,
        'display_ticket': display_ticket,
        'ticket_number': getattr(row, 'ticket_number', None) or None,
        'fecha': row.sale_date.isoformat() if row.sale_date else '',
        'type': row.sale_type,
        'status': row.status,
        'payment_method': row.payment_method,
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
        'customer_id': cust_id or '',
        'customer_name': cust_name or '',
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
                'product_id': it.product_id or '',
                'nombre': it.product_name or 'Producto',
                'precio': it.unit_price,
                'cantidad': it.qty,
                'descuento': it.discount_pct,
                'subtotal': it.subtotal,
                'direction': it.direction,
            }
            for it in (row.items or [])
        ],
    }


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
        'description': (p.description or ''),
        'sale_price': p.sale_price,
        'category_id': p.category_id,
        'category': cat,
        'category_name': (cat.get('name') if isinstance(cat, dict) else ''),
        'active': bool(p.active),
        'image_url': _image_url(p),
    }


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
    raw_from = (request.args.get('from') or '').strip()
    raw_to = (request.args.get('to') or '').strip()
    include_replaced = str(request.args.get('include_replaced') or '').strip() in ('1', 'true', 'True')
    exclude_cc = str(request.args.get('exclude_cc') or '').strip() in ('1', 'true', 'True')
    limit = int(request.args.get('limit') or 300)
    if limit <= 0 or limit > 20000:
        limit = 300

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
    q = (
        db.session.query(Sale)
        .options(selectinload(Sale.items))
        .filter(Sale.company_id == cid)
    )
    if d_from:
        q = q.filter(Sale.sale_date >= d_from)
    if d_to:
        q = q.filter(Sale.sale_date <= d_to)
    if not include_replaced:
        q = q.filter(Sale.status != 'Reemplazada')
    if exclude_cc:
        q = q.filter(Sale.sale_type != 'CobroCC')
    q = q.order_by(Sale.sale_date.desc(), Sale.id.desc()).limit(limit)
    rows = q.all()

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

    return jsonify({'ok': True, 'items': [_serialize_sale(r, related=related_map.get(int(r.id)), users_map=users_map) for r in rows]})


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
    q = (
        db.session.query(Product)
        .options(joinedload(Product.category))
        .filter(Product.company_id == cid)
        .filter(Product.active == True)  # noqa: E712
    )
    if qraw:
        like = f"%{qraw}%"
        q = q.filter(or_(Product.name.ilike(like), Product.internal_code.ilike(like), Product.barcode.ilike(like)))
    q = q.order_by(Product.name.asc(), Product.id.asc())

    rows = q.offset(offset).limit(limit + 1).all()
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
    payload = request.get_json(silent=True) or {}
    sale_date = _parse_date_iso(payload.get('fecha') or payload.get('date'), dt_date.today())
    payment_method = str(payload.get('payment_method') or 'Efectivo').strip() or 'Efectivo'
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

    diff_to_pay = max(0.0, float(new_total or 0.0) - float(return_total or 0.0))
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
            discount_general_pct=0.0,
            discount_general_amount=0.0,
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
    try:
        return float(v)
    except Exception:
        current_app.logger.exception('Failed to convert value to float')
        return 0.0


def _last_cash_event_sale(cid: str, d: dt_date):
    try:
        return (
            db.session.query(func.max(Sale.updated_at))
            .filter(Sale.company_id == cid)
            .filter(Sale.sale_date == d)
            .filter(Sale.payment_method == 'Efectivo')
            .filter(Sale.status != 'Reemplazada')
            .filter(Sale.sale_type.in_(['CobroVenta', 'CobroCC', 'CobroCuota', 'Cambio', 'Devolucion', 'Devolución', 'Pago']))
            .scalar()
        )
    except Exception:
        return None


def _last_cash_event_expense(cid: str, d: dt_date):
    try:
        return (
            db.session.query(func.max(Expense.updated_at))
            .filter(Expense.company_id == cid)
            .filter(Expense.expense_date == d)
            .filter(Expense.payment_method == 'Efectivo')
            .scalar()
        )
    except Exception:
        return None


def _last_cash_event_withdrawal(cid: str, d: dt_date):
    try:
        from app.models import CashWithdrawal
        return (
            db.session.query(func.max(CashWithdrawal.updated_at))
            .filter(CashWithdrawal.company_id == cid)
            .filter(CashWithdrawal.fecha_imputacion == d)
            .scalar()
        )
    except Exception:
        return None


def _cash_expected_now(cid: str, d: dt_date) -> float:
    ventas = _cash_sales_total(cid, d)
    egresos = _cash_expenses_total(cid, d)
    retiros = _cash_withdrawals_total(cid, d)
    return float((ventas or 0.0) - (egresos or 0.0) - (retiros or 0.0))


def _last_cash_event_now(cid: str, d: dt_date):
    a = _last_cash_event_sale(cid, d)
    b = _last_cash_event_expense(cid, d)
    c = _last_cash_event_withdrawal(cid, d)
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
                    m = re.search(r"Ticket\s*(?:original\s*)?#\s*(#?\w+)", note, re.IGNORECASE)
                    ref = (m.group(1) if (m and m.group(1)) else '').strip()
                except Exception:
                    ref = ''

                if ref and not ref.startswith('#'):
                    ref = '#' + ref

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
    _ensure_installment_plan_columns()
    payload = request.get_json(silent=True) or {}
    sale_date = _parse_date_iso(payload.get('fecha') or payload.get('date'), dt_date.today())
    sale_type = str(payload.get('type') or 'Venta').strip() or 'Venta'
    status = str(payload.get('status') or ('Cambio' if sale_type == 'Cambio' else 'Completada')).strip() or 'Completada'
    payment_method = str(payload.get('payment_method') or 'Efectivo').strip() or 'Efectivo'

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

    customer_id = str(payload.get('customer_id') or '').strip() or None
    customer_name = str(payload.get('customer_name') or '').strip() or None

    exchange_return_total = (None if payload.get('exchange_return_total') is None else _num(payload.get('exchange_return_total')))
    exchange_new_total = (None if payload.get('exchange_new_total') is None else _num(payload.get('exchange_new_total')))

    on_account = bool(payload.get('on_account'))
    paid_amount = _num(payload.get('paid_amount'))
    due_amount = _num(payload.get('due_amount'))

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
    attempts = 0
    base_n = _next_ticket_number(cid)
    while attempts < 10:
        n = base_n + attempts
        attempts += 1
        tk = _format_ticket_number(n)

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
            db.session.rollback()
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
            return jsonify(payload_out)
        payload_out = {'ok': True, 'item': _serialize_sale(row)}
        if cash_payment_sale is not None:
            payload_out['cash_payment'] = _serialize_sale(cash_payment_sale)
        return jsonify(payload_out)

    return jsonify({'ok': False, 'error': 'ticket_duplicate', 'message': 'No se pudo registrar la venta: ticket duplicado.'}), 400


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


@bp.post('/api/installment-plans/<int:plan_id>/cancel')
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

    # Registrar en legajo: hoy el legajo muestra "Observaciones internas" desde Customer.notes.
    try:
        cust_id = str(getattr(plan, 'customer_id', '') or '').strip()
    except Exception:
        cust_id = ''

    try:
        cust_name = str(getattr(plan, 'customer_name', '') or '').strip()
    except Exception:
        cust_name = ''

    try:
        ticket_txt = str(getattr(plan, 'sale_ticket', '') or '').strip()
    except Exception:
        ticket_txt = ''

    try:
        plan_mode = str(getattr(plan, 'mode', '') or '').strip()
    except Exception:
        plan_mode = ''

    # Decidir si se puede borrar sin romper historial: si hay cuotas pagadas, mantener el plan cancelado.
    has_paid = False
    try:
        for it in (getattr(plan, 'installments', None) or []):
            st_it = str(getattr(it, 'status', '') or '').strip().lower()
            if st_it == 'pagada':
                has_paid = True
                break
            if getattr(it, 'paid_sale_id', None) is not None:
                has_paid = True
                break
    except Exception:
        has_paid = True

    try:
        cust_row = None
        if cust_id:
            cust_row = db.session.query(Customer).filter(Customer.company_id == cid, Customer.id == cust_id).first()

        stamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M')
        base = f"[{stamp}] Plan de cuotas cancelado"
        if ticket_txt:
            base += f" (Ticket {ticket_txt})"
        if plan_mode:
            base += f" · Modo: {plan_mode}"
        if has_paid:
            base += " · Se mantiene registro (había cuotas pagadas)."
        else:
            base += " · Plan eliminado."

        if cust_row is not None:
            prev = str(getattr(cust_row, 'notes', '') or '').strip()
            cust_row.notes = (prev + ('\n' if prev else '') + base).strip()
        else:
            # Si por algún motivo no se puede resolver el cliente, no fallar la cancelación.
            pass
    except Exception:
        current_app.logger.exception('Failed to append installment cancel note to customer notes')

    try:
        if has_paid:
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
    t = str(ticket or '').strip()
    cid = _company_id()
    row = db.session.query(Sale).filter(Sale.company_id == cid, Sale.ticket == t).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

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
    row.on_account = bool(payload.get('on_account'))
    row.paid_amount = _num(payload.get('paid_amount'))
    row.due_amount = _num(payload.get('due_amount'))
    row.customer_id = str(payload.get('customer_id') or '').strip() or None
    row.customer_name = str(payload.get('customer_name') or '').strip() or None

    raw_emp_id = str(payload.get('employee_id') or '').strip() or None
    raw_emp_name = str(payload.get('employee_name') or '').strip() or None
    emp_id, emp_name = _resolve_employee_fields(cid=cid, employee_id=raw_emp_id, employee_name=raw_emp_name)
    row.employee_id = emp_id
    row.employee_name = emp_name

    row.exchange_return_total = (None if payload.get('exchange_return_total') is None else _num(payload.get('exchange_return_total')))
    row.exchange_new_total = (None if payload.get('exchange_new_total') is None else _num(payload.get('exchange_new_total')))

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

    row = db.session.query(CashCount).filter(CashCount.company_id == cid, CashCount.count_date == d).first()
    if not row:
        return jsonify({'ok': True, 'item': None})

    return jsonify({
        'ok': True,
        'item': {
            'date': row.count_date.isoformat(),
            'employee_id': row.employee_id,
            'employee_name': row.employee_name,
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

    row = db.session.query(CashCount).filter(CashCount.company_id == cid, CashCount.count_date == d).first()
    if not row:
        # Compat: algunas DB viejas (SQLite) pueden tener UNIQUE(count_date) y no (company_id, count_date)
        # En ese caso, si existe un arqueo para la fecha, hay que actualizarlo en vez de insertar.
        row = db.session.query(CashCount).filter(CashCount.count_date == d).first()
        if not row:
            row = CashCount(count_date=d, company_id=cid)
            db.session.add(row)
        else:
            try:
                row.company_id = cid
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

    cash_expected = _cash_expected_now(cid, d)
    last_event = _last_cash_event_now(cid, d)
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
        # Compat fuerte: si la DB tiene UNIQUE(count_date) y estamos intentando insertar,
        # reintentar como UPDATE del registro existente para esa fecha.
        db.session.rollback()
        try:
            existing = db.session.query(CashCount).filter(CashCount.count_date == d).first()
        except Exception:
            existing = None

        if existing:
            try:
                existing.company_id = cid
            except Exception:
                pass
            existing.employee_id = employee_id
            existing.employee_name = employee_name
            existing.opening_amount = opening
            existing.cash_day_amount = cash_day
            existing.closing_amount = closing
            existing.difference_amount = diff
            existing.created_by_user_id = row.created_by_user_id
            try:
                db.session.commit()
                row = existing
            except Exception:
                db.session.rollback()
                return jsonify({'ok': False, 'error': 'db_error'}), 400
        else:
            return jsonify({'ok': False, 'error': 'already_exists', 'message': 'Ya existe un arqueo para esa fecha.'}), 400
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400

    return jsonify({'ok': True, 'item': {'date': row.count_date.isoformat(), 'difference_amount': row.difference_amount}})


def _round2(v: float) -> float:
    try:
        return float(f"{float(v or 0.0):.2f}")
    except Exception:
        return 0.0


def _cash_sales_total(cid: str, d: dt_date) -> float:
    try:
        q = (
            db.session.query(func.coalesce(func.sum(Sale.total), 0.0))
            .filter(Sale.company_id == cid)
            .filter(Sale.sale_date == d)
            .filter(Sale.payment_method == 'Efectivo')
            .filter(Sale.status != 'Reemplazada')
            .filter(Sale.sale_type.in_(['CobroVenta', 'CobroCC', 'CobroCuota', 'Cambio', 'Devolucion', 'Devolución', 'Pago']))
        )
        return float(q.scalar() or 0.0)
    except Exception:
        return 0.0


def _cash_expenses_total(cid: str, d: dt_date) -> float:
    try:
        q = (
            db.session.query(func.coalesce(func.sum(Expense.amount), 0.0))
            .filter(Expense.company_id == cid)
            .filter(Expense.expense_date == d)
            .filter(Expense.payment_method == 'Efectivo')
        )
        return float(q.scalar() or 0.0)
    except Exception:
        return 0.0


def _cash_withdrawals_total(cid: str, d: dt_date) -> float:
    try:
        from app.models import CashWithdrawal
        q = (
            db.session.query(func.coalesce(func.sum(CashWithdrawal.monto), 0.0))
            .filter(CashWithdrawal.company_id == cid)
            .filter(CashWithdrawal.fecha_imputacion == d)
        )
        return float(q.scalar() or 0.0)
    except Exception:
        return 0.0


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

    row = db.session.query(CashCount).filter(CashCount.company_id == cid, CashCount.count_date == d).first()
    if not row:
        return jsonify({'ok': True, 'date': d.isoformat(), 'has_record': False, 'status': 'none', 'is_valid': False, 'should_show': 'pendiente'})

    expected_now = _cash_expected_now(cid, d)
    last_now = _last_cash_event_now(cid, d)

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
    })
