import json
from functools import wraps
from datetime import date as dt_date
from datetime import timedelta
from uuid import uuid4

from flask import abort, g, jsonify, render_template, request, redirect, url_for
from flask_login import login_required, current_user
from sqlalchemy import or_

from app import db
from app.models import Expense, Supplier
from app.permissions import module_required
from app.tenancy import is_impersonating
from app.suppliers import bp


def _serialize_supplier(row: Supplier):
    try:
        cats = json.loads(row.categories_json or '[]')
        cats = cats if isinstance(cats, list) else []
    except Exception:
        cats = []
    return {
        'id': row.id,
        'name': row.name or '',
        'supplier_type': row.supplier_type or 'Services',
        'status': row.status or 'Active',
        'categories': cats,
        'invoice_type': row.invoice_type or '',
        'default_payment_method': row.default_payment_method or '',
        'payment_terms': row.payment_terms or '',
        'contact_person': row.contact_person or '',
        'phone': row.phone or '',
        'email': row.email or '',
        'address': row.address or '',
        'preferred_contact_channel': row.preferred_contact_channel or '',
        'notes': row.notes or '',
        'meta_json': row.meta_json or ''
    }


def _get_company_id() -> str | None:
    try:
        cid = str(getattr(g, 'company_id', '') or '').strip()
        return cid or None
    except Exception:
        return None


def _apply_supplier_payload(row: Supplier, payload: dict):
    row.name = str(payload.get('name') or row.name or '').strip() or row.name
    row.supplier_type = str(payload.get('supplier_type') or payload.get('type') or row.supplier_type or 'Services').strip() or 'Services'
    row.status = str(payload.get('status') or row.status or 'Active').strip() or 'Active'

    cats = payload.get('categories')
    if cats is not None:
        if not isinstance(cats, list):
            cats = []
        try:
            row.categories_json = json.dumps([str(x) for x in cats], ensure_ascii=False)
        except Exception:
            row.categories_json = '[]'

    row.invoice_type = str(payload.get('invoice_type') or '').strip() or None
    row.default_payment_method = str(payload.get('default_payment_method') or '').strip() or None
    row.payment_terms = str(payload.get('payment_terms') or '').strip() or None
    row.contact_person = str(payload.get('contact_person') or '').strip() or None
    row.preferred_contact_channel = str(payload.get('preferred_contact_channel') or '').strip() or None
    row.phone = str(payload.get('phone') or '').strip() or None
    row.email = str(payload.get('email') or '').strip() or None
    row.address = str(payload.get('address') or '').strip() or None
    row.notes = str(payload.get('notes') or '').strip() or None
    if payload.get('meta_json') is not None:
        row.meta_json = str(payload.get('meta_json') or '').strip() or None


def _load_meta_obj(raw: str) -> dict:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else {}
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _save_meta_obj(row: Expense, meta_obj: dict) -> None:
    try:
        row.meta_json = json.dumps(meta_obj or {}, ensure_ascii=False)
    except Exception:
        row.meta_json = None


def _days(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


def _month_add(d: dt_date, months: int) -> dt_date:
    if not d:
        return d
    m = int(months or 0)
    y = d.year + (d.month - 1 + m) // 12
    mm = (d.month - 1 + m) % 12 + 1
    last_day = 28
    for dd in (31, 30, 29, 28):
        try:
            dt_date(y, mm, dd)
            last_day = dd
            break
        except Exception:
            continue
    day = min(d.day, last_day)
    return dt_date(y, mm, day)


def _money(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def _parse_iso_date_safe(raw: str):
    s = str(raw or '').strip()
    if not s:
        return None
    try:
        return dt_date.fromisoformat(s)
    except Exception:
        return None


def _compute_cc_schedule(expense_date: dt_date, amount: float, terms_days: int, installments: int, paid_total: float):
    amount = _money(amount, 0.0)
    terms_days = max(1, _days(terms_days, 30) or 30)
    installments = max(1, _days(installments, 1) or 1)

    first_due = (expense_date or dt_date.today()) + timedelta(days=terms_days)
    per = (amount / installments) if installments else amount
    schedule = []
    remaining_pay = max(0.0, _money(paid_total, 0.0))

    for i in range(installments):
        due_date = _month_add(first_due, i)
        inst_amount = per if i < (installments - 1) else (amount - per * (installments - 1))
        inst_amount = round(_money(inst_amount, 0.0), 2)
        paid_here = min(inst_amount, remaining_pay)
        remaining_pay = max(0.0, remaining_pay - paid_here)
        rem = round(inst_amount - paid_here, 2)
        schedule.append({
            'n': i + 1,
            'due_date': due_date.isoformat() if due_date else '',
            'amount': inst_amount,
            'paid': round(paid_here, 2),
            'remaining': rem,
            'status': 'paid' if rem <= 0 else 'open'
        })

    return {
        'first_due_date': first_due.isoformat() if first_due else '',
        'schedule': schedule
    }


def _build_cc_transactions(expense_date: dt_date, amount: float, payments: list):
    out = [{
        'kind': 'compra',
        'date': expense_date.isoformat() if expense_date else '',
        'amount': round(_money(amount, 0.0), 2),
        'payment_method': 'Cuenta corriente'
    }]
    pay_list = payments if isinstance(payments, list) else []
    normalized = []
    for p in pay_list:
        if not isinstance(p, dict):
            continue
        normalized.append({
            'kind': 'pago',
            'id': str(p.get('id') or '').strip(),
            'date': str(p.get('date') or '').strip(),
            'amount': round(_money(p.get('amount'), 0.0), 2),
            'payment_method': str(p.get('payment_method') or '').strip() or 'Efectivo'
        })
    normalized.sort(key=lambda x: str(x.get('date') or ''))
    out.extend(normalized)
    return out


def _is_payment_expense(meta_obj: dict) -> bool:
    ref = meta_obj.get('origin_ref') if isinstance(meta_obj, dict) else None
    if isinstance(ref, dict) and str(ref.get('kind') or '').strip() == 'supplier_cc_payment':
        return True
    return False


@bp.get('/api/suppliers/<supplier_id>/cc-ledger')
@login_required
@module_required('suppliers')
def supplier_cc_ledger_api(supplier_id):
    try:
        company_id = _get_company_id()
        if not company_id:
            return jsonify({'ok': False, 'error': 'no_company'}), 400

        sid = str(supplier_id or '').strip()
        supplier = db.session.get(Supplier, sid)
        if (not supplier) or (str(getattr(supplier, 'company_id', '') or '').strip() != company_id):
            return jsonify({'ok': False, 'error': 'not_found'}), 404

        sname = str(supplier.name or '').strip()

        rows = (
            db.session.query(Expense)
            .filter(
                Expense.company_id == company_id,
                or_(
                    Expense.supplier_id == sid,
                    (Expense.supplier_id.is_(None) & (Expense.supplier_name.ilike(sname)))
                )
            )
            .order_by(Expense.expense_date.desc(), Expense.created_at.desc())
            .limit(5000)
            .all()
        )

        today = dt_date.today()
        items = []
        total_due = 0.0
        overdue_due = 0.0
        next_due_date = None
        next_due_amount = 0.0

        for r in rows:
            meta_obj = _load_meta_obj(r.meta_json or '')
            if _is_payment_expense(meta_obj):
                continue

            cc = meta_obj.get('supplier_cc') if isinstance(meta_obj, dict) else None
            if not isinstance(cc, dict) or not cc.get('enabled'):
                continue

            amount = _money(r.amount, 0.0)
            terms_days = _days(cc.get('terms_days'), 30)
            installments = _days(cc.get('installments'), 1)
            payments = cc.get('payments') if isinstance(cc.get('payments'), list) else []
            paid_total = sum([_money(p.get('amount'), 0.0) for p in payments if isinstance(p, dict)])
            paid_total = min(amount, paid_total)
            remaining = max(0.0, amount - paid_total)

            transactions = _build_cc_transactions(r.expense_date, amount, payments)

            sched_info = _compute_cc_schedule(r.expense_date, amount, terms_days, installments, paid_total)
            schedule = sched_info['schedule']

            has_overdue = False
            for inst in schedule:
                if inst.get('status') != 'open':
                    continue
                due_dt = _parse_iso_date_safe(inst.get('due_date'))
                rem_inst = _money(inst.get('remaining'), 0.0)
                if due_dt and due_dt < today and rem_inst > 0:
                    has_overdue = True
                    overdue_due += rem_inst
                if due_dt and rem_inst > 0:
                    if next_due_date is None or due_dt < next_due_date:
                        next_due_date = due_dt
                        next_due_amount = rem_inst

            total_due += remaining

            status = 'paid'
            if remaining > 0:
                status = 'overdue' if has_overdue else 'open'

            items.append({
                'expense_id': r.id,
                'date': r.expense_date.isoformat() if r.expense_date else '',
                'category': r.category or '',
                'note': r.note or (r.description or ''),
                'amount': amount,
                'terms_days': terms_days,
                'installments': installments,
                'paid_total': round(paid_total, 2),
                'remaining': round(remaining, 2),
                'first_due_date': sched_info.get('first_due_date') or '',
                'status': status,
                'schedule': schedule,
                'transactions': transactions,
            })

        return jsonify({
            'ok': True,
            'supplier': {
                'id': supplier.id,
                'name': supplier.name or ''
            },
            'summary': {
                'total_due': round(total_due, 2),
                'overdue_due': round(overdue_due, 2),
                'next_due_date': next_due_date.isoformat() if next_due_date else '',
                'next_due_amount': round(next_due_amount, 2) if next_due_date else 0.0,
                'open_items': len([x for x in items if _money(x.get('remaining'), 0.0) > 0])
            },
            'items': items
        })
    except Exception as e:
        return jsonify({'ok': False, 'error': 'server_error', 'message': str(e)}), 500


@bp.post('/api/suppliers/<supplier_id>/cc-payments')
@login_required
@module_required('suppliers')
def supplier_cc_payment_api(supplier_id):
    company_id = _get_company_id()
    if not company_id:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    sid = str(supplier_id or '').strip()
    supplier = db.session.get(Supplier, sid)
    if (not supplier) or (str(getattr(supplier, 'company_id', '') or '').strip() != company_id):
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    sname = str(supplier.name or '').strip()

    payload = request.get_json(silent=True) or {}
    expense_id = str(payload.get('expense_id') or '').strip()
    method = str(payload.get('payment_method') or 'Efectivo').strip() or 'Efectivo'
    amount = _money(payload.get('amount'), 0.0)
    if not expense_id:
        return jsonify({'ok': False, 'error': 'missing_expense_id'}), 400
    if amount <= 0:
        return jsonify({'ok': False, 'error': 'invalid_amount'}), 400
    if method not in ['Efectivo', 'Transferencia', 'Débito', 'Crédito']:
        return jsonify({'ok': False, 'error': 'invalid_method'}), 400

    row = db.session.get(Expense, expense_id)
    if (not row) or (str(getattr(row, 'company_id', '') or '').strip() != company_id):
        return jsonify({'ok': False, 'error': 'expense_not_found'}), 404
    row_sid = str(row.supplier_id or '').strip()
    row_name = str(row.supplier_name or '').strip()
    if row_sid != sid:
        if row_sid:
            return jsonify({'ok': False, 'error': 'expense_not_found'}), 404
        if not sname or row_name.lower() != sname.lower():
            return jsonify({'ok': False, 'error': 'expense_not_found'}), 404

    meta_obj = _load_meta_obj(row.meta_json or '')
    cc = meta_obj.get('supplier_cc') if isinstance(meta_obj, dict) else None
    if not isinstance(cc, dict) or not cc.get('enabled'):
        return jsonify({'ok': False, 'error': 'not_cc_expense'}), 400

    amount_total = _money(row.amount, 0.0)
    payments = cc.get('payments') if isinstance(cc.get('payments'), list) else []
    paid_total = sum([_money(p.get('amount'), 0.0) for p in payments if isinstance(p, dict)])
    remaining = max(0.0, amount_total - paid_total)
    if remaining <= 0:
        return jsonify({'ok': False, 'error': 'already_paid'}), 400
    pay_amount = min(remaining, amount)

    pay_id = uuid4().hex
    payment_entry = {
        'id': pay_id,
        'date': (dt_date.today()).isoformat(),
        'amount': round(pay_amount, 2),
        'payment_method': method
    }
    payments.append(payment_entry)
    cc['payments'] = payments
    meta_obj['supplier_cc'] = cc

    _save_meta_obj(row, meta_obj)

    pay_exp_id = uuid4().hex
    pay_exp = Expense(
        id=pay_exp_id,
        company_id=company_id,
        expense_date=dt_date.today(),
        payment_method=method,
        amount=round(pay_amount, 2),
        category='Pago cuenta corriente proveedor',
        supplier_id=sid,
        supplier_name=supplier.name or '',
        note='Pago CC · ' + (supplier.name or 'Proveedor'),
        description=None,
        origin='manual'
    )
    pay_meta = {
        'supplier_cc_payment': True,
        'origin_ref': {
            'kind': 'supplier_cc_payment',
            'supplier_id': sid,
            'supplier_name': supplier.name or '',
            'expense_id': expense_id,
            'payment_id': pay_id
        }
    }
    _save_meta_obj(pay_exp, pay_meta)

    db.session.add(pay_exp)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400

    return jsonify({'ok': True, 'payment': payment_entry, 'payment_expense_id': pay_exp_id})



@bp.route('/')
@bp.route('/index')
@login_required
@module_required('suppliers')
def index():
    """Gestor de proveedores."""
    company_id = _get_company_id()
    if not company_id:
        return render_template('suppliers/index.html', title='Proveedores', suppliers=[])
    try:
        rows = (
            db.session.query(Supplier)
            .filter(Supplier.company_id == company_id)
            .order_by(Supplier.updated_at.desc(), Supplier.created_at.desc())
            .limit(5000)
            .all()
        )
        suppliers = [_serialize_supplier(r) for r in rows]
    except Exception:
        suppliers = []
    return render_template('suppliers/index.html', title='Proveedores', suppliers=suppliers)


@bp.route('/new')
@login_required
@module_required('suppliers')
def new():
    return render_template('suppliers/new.html', title='Nuevo proveedor')


@bp.get('/api/suppliers')
@login_required
@module_required('suppliers')
def list_suppliers_api():
    q = (request.args.get('q') or '').strip().lower()
    limit = int(request.args.get('limit') or 5000)
    if limit <= 0 or limit > 10000:
        limit = 5000

    company_id = _get_company_id()
    if not company_id:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    query = db.session.query(Supplier).filter(Supplier.company_id == company_id)
    if q:
        like = f"%{q}%"
        query = query.filter(Supplier.name.ilike(like))
    rows = query.order_by(Supplier.updated_at.desc(), Supplier.created_at.desc()).limit(limit).all()
    return jsonify({'ok': True, 'items': [_serialize_supplier(r) for r in rows]})


@bp.get('/api/suppliers/<supplier_id>')
@login_required
@module_required('suppliers')
def get_supplier_api(supplier_id):
    company_id = _get_company_id()
    if not company_id:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    sid = str(supplier_id or '').strip()
    row = db.session.get(Supplier, sid)
    if (not row) or (str(getattr(row, 'company_id', '') or '').strip() != company_id):
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    return jsonify({'ok': True, 'item': _serialize_supplier(row)})


@bp.post('/api/suppliers')
@login_required
@module_required('suppliers')
def create_supplier_api():
    company_id = _get_company_id()
    if not company_id:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    payload = request.get_json(silent=True) or {}
    sid = str(payload.get('id') or '').strip() or uuid4().hex
    row = db.session.get(Supplier, sid)
    if row:
        return jsonify({'ok': False, 'error': 'already_exists'}), 400
    row = Supplier(id=sid, company_id=company_id, name=str(payload.get('name') or '').strip() or 'Proveedor')
    _apply_supplier_payload(row, payload)
    db.session.add(row)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_supplier(row)})


@bp.put('/api/suppliers/<supplier_id>')
@login_required
@module_required('suppliers')
def update_supplier_api(supplier_id):
    company_id = _get_company_id()
    if not company_id:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    sid = str(supplier_id or '').strip()
    row = db.session.get(Supplier, sid)
    if (not row) or (str(getattr(row, 'company_id', '') or '').strip() != company_id):
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    payload = request.get_json(silent=True) or {}
    _apply_supplier_payload(row, payload)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_supplier(row)})


@bp.delete('/api/suppliers/<supplier_id>')
@login_required
@module_required('suppliers')
def delete_supplier_api(supplier_id):
    company_id = _get_company_id()
    if not company_id:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    sid = str(supplier_id or '').strip()
    row = db.session.get(Supplier, sid)
    if (not row) or (str(getattr(row, 'company_id', '') or '').strip() != company_id):
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    try:
        db.session.delete(row)
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True})


@bp.post('/api/suppliers/bulk')
@login_required
@module_required('suppliers')
def upsert_suppliers_bulk():
    company_id = _get_company_id()
    if not company_id:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    payload = request.get_json(silent=True) or {}
    items = payload.get('items')
    items_list = items if isinstance(items, list) else []
    out = []
    for it in items_list:
        d = it if isinstance(it, dict) else {}
        sid = str(d.get('id') or '').strip() or uuid4().hex
        row = db.session.get(Supplier, sid)
        if row and (str(getattr(row, 'company_id', '') or '').strip() != company_id):
            sid = uuid4().hex
            row = None
        if not row:
            row = Supplier(id=sid, company_id=company_id, name=str(d.get('name') or '').strip() or 'Proveedor')
            db.session.add(row)
        _apply_supplier_payload(row, d)
        out.append(row)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'items': [_serialize_supplier(r) for r in out]})
