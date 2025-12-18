from datetime import date as dt_date
from uuid import uuid4
import json

from flask import jsonify, render_template, request
from flask_login import login_required

from app import db
from app.models import Expense, ExpenseCategory
from app.permissions import module_required
from app.expenses import bp


def _parse_date_iso(raw, default=None):
    s = str(raw or '').strip()
    if not s:
        return default
    try:
        return dt_date.fromisoformat(s)
    except Exception:
        return default


def _serialize_expense(row: Expense):
    return {
        'id': row.id,
        'fecha': row.expense_date.isoformat() if row.expense_date else '',
        'categoria': row.category or '',
        'valor': row.amount,
        'nota': row.note or (row.description or ''),
        'forma_pago': row.payment_method or 'Efectivo',
        'supplier_id': row.supplier_id or '',
        'supplier_name': row.supplier_name or '',
        'type': row.expense_type or '',
        'frequency': row.frequency or '',
        'employee_id': row.employee_id or '',
        'employee_name': row.employee_name or '',
        'period_from': row.period_from.isoformat() if row.period_from else '',
        'period_to': row.period_to.isoformat() if row.period_to else '',
        'meta_json': row.meta_json or '',
        'origin': row.origin or 'manual',
    }


@bp.route("/")
@bp.route("/index")
@login_required
@module_required('expenses')
def index():
    """Listado básico de gastos (dummy)."""
    expenses = []
    return render_template("expenses/list.html", title="Gastos", expenses=expenses)


@bp.route("/new")
@login_required
@module_required('expenses')
def new():
    return render_template("expenses/new.html", title="Nuevo gasto", today=dt_date.today().isoformat())


def _num(v, default=0.0):
    try:
        if v is None or v == '':
            return default
        return float(v)
    except Exception:
        return default


def _apply_expense_payload(row: Expense, payload: dict):
    row.expense_date = _parse_date_iso(payload.get('fecha') or payload.get('expense_date'), row.expense_date or dt_date.today())
    row.payment_method = str(payload.get('forma_pago') or payload.get('payment_method') or row.payment_method or 'Efectivo').strip() or 'Efectivo'
    row.amount = _num(payload.get('valor') if payload.get('valor') is not None else payload.get('amount'), 0.0)
    row.category = str(payload.get('categoria') or payload.get('category') or '').strip() or None

    row.note = str(payload.get('nota') or payload.get('note') or '').strip() or None
    row.description = str(payload.get('description') or '').strip() or None

    row.supplier_id = str(payload.get('supplier_id') or '').strip() or None
    row.supplier_name = str(payload.get('supplier_name') or payload.get('proveedor') or '').strip() or None

    row.expense_type = str(payload.get('type') or payload.get('expense_type') or '').strip() or None
    row.frequency = str(payload.get('frequency') or '').strip() or None

    row.employee_id = str(payload.get('employee_id') or '').strip() or None
    row.employee_name = str(payload.get('employee_name') or '').strip() or None

    row.period_from = _parse_date_iso(payload.get('period_from') or payload.get('periodo_desde'), None)
    row.period_to = _parse_date_iso(payload.get('period_to') or payload.get('periodo_hasta'), None)

    meta = payload.get('meta')
    if meta is not None and isinstance(meta, (dict, list)):
        try:
            row.meta_json = json.dumps(meta, ensure_ascii=False)
        except Exception:
            row.meta_json = None
    elif payload.get('meta_json') is not None:
        row.meta_json = str(payload.get('meta_json') or '').strip() or None


def _serialize_expense_category(row: ExpenseCategory):
    return {
        'id': row.id,
        'name': row.name or '',
    }


def _apply_expense_category_payload(row: ExpenseCategory, payload: dict):
    row.name = str(payload.get('name') or payload.get('categoria') or payload.get('category') or row.name or '').strip() or row.name


@bp.get('/api/expenses')
@login_required
@module_required('expenses')
def list_expenses_api():
    raw_from = (request.args.get('from') or '').strip()
    raw_to = (request.args.get('to') or '').strip()
    category = (request.args.get('category') or '').strip()
    limit = int(request.args.get('limit') or 5000)
    if limit <= 0 or limit > 10000:
        limit = 5000

    d_from = _parse_date_iso(raw_from, None)
    d_to = _parse_date_iso(raw_to, None)

    q = db.session.query(Expense)
    if d_from:
        q = q.filter(Expense.expense_date >= d_from)
    if d_to:
        q = q.filter(Expense.expense_date <= d_to)
    if category:
        q = q.filter(Expense.category == category)

    rows = q.order_by(Expense.expense_date.desc()).limit(limit).all()
    return jsonify({'ok': True, 'items': [_serialize_expense(r) for r in rows]})


@bp.get('/api/expenses/<expense_id>')
@login_required
@module_required('expenses')
def get_expense_api(expense_id):
    eid = str(expense_id or '').strip()
    row = db.session.get(Expense, eid)
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    return jsonify({'ok': True, 'item': _serialize_expense(row)})


@bp.post('/api/expenses')
@login_required
@module_required('expenses')
def create_expense_api():
    payload = request.get_json(silent=True) or {}
    eid = str(payload.get('id') or '').strip() or uuid4().hex

    row = db.session.get(Expense, eid)
    if row:
        return jsonify({'ok': False, 'error': 'already_exists'}), 400

    row = Expense(id=eid, expense_date=dt_date.today())
    _apply_expense_payload(row, payload)
    db.session.add(row)

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_expense(row)})


@bp.put('/api/expenses/<expense_id>')
@login_required
@module_required('expenses')
def update_expense_api(expense_id):
    eid = str(expense_id or '').strip()
    row = db.session.get(Expense, eid)
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    payload = request.get_json(silent=True) or {}
    _apply_expense_payload(row, payload)

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_expense(row)})


@bp.delete('/api/expenses/<expense_id>')
@login_required
@module_required('expenses')
def delete_expense_api(expense_id):
    eid = str(expense_id or '').strip()
    row = db.session.get(Expense, eid)
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    try:
        db.session.delete(row)
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True})


@bp.post('/api/expenses/bulk')
@login_required
@module_required('expenses')
def upsert_expenses_bulk():
    payload = request.get_json(silent=True) or {}
    items = payload.get('items')
    items_list = items if isinstance(items, list) else []

    out = []
    for it in items_list:
        d = it if isinstance(it, dict) else {}
        eid = str(d.get('id') or '').strip() or uuid4().hex
        row = db.session.get(Expense, eid)
        if not row:
            row = Expense(id=eid, expense_date=dt_date.today())
            db.session.add(row)
        _apply_expense_payload(row, d)
        out.append(row)

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'items': [_serialize_expense(r) for r in out]})


@bp.get('/api/categories')
@login_required
@module_required('expenses')
def list_expense_categories_api():
    q = (request.args.get('q') or '').strip().lower()
    limit = int(request.args.get('limit') or 5000)
    if limit <= 0 or limit > 10000:
        limit = 5000
    query = db.session.query(ExpenseCategory)
    if q:
        like = f"%{q}%"
        query = query.filter(ExpenseCategory.name.ilike(like))
    rows = query.order_by(ExpenseCategory.name.asc()).limit(limit).all()
    return jsonify({'ok': True, 'items': [_serialize_expense_category(r) for r in rows]})


@bp.get('/api/categories/<category_id>')
@login_required
@module_required('expenses')
def get_expense_category_api(category_id):
    cid = str(category_id or '').strip()
    row = db.session.get(ExpenseCategory, cid)
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    return jsonify({'ok': True, 'item': _serialize_expense_category(row)})


@bp.post('/api/categories')
@login_required
@module_required('expenses')
def create_expense_category_api():
    payload = request.get_json(silent=True) or {}
    cid = str(payload.get('id') or '').strip() or uuid4().hex
    row = db.session.get(ExpenseCategory, cid)
    if row:
        return jsonify({'ok': False, 'error': 'already_exists'}), 400
    name = str(payload.get('name') or payload.get('categoria') or '').strip() or 'Categoría'
    row = ExpenseCategory(id=cid, name=name)
    _apply_expense_category_payload(row, payload)
    db.session.add(row)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_expense_category(row)})


@bp.put('/api/categories/<category_id>')
@login_required
@module_required('expenses')
def update_expense_category_api(category_id):
    cid = str(category_id or '').strip()
    row = db.session.get(ExpenseCategory, cid)
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    payload = request.get_json(silent=True) or {}
    _apply_expense_category_payload(row, payload)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_expense_category(row)})


@bp.delete('/api/categories/<category_id>')
@login_required
@module_required('expenses')
def delete_expense_category_api(category_id):
    cid = str(category_id or '').strip()
    row = db.session.get(ExpenseCategory, cid)
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    try:
        db.session.delete(row)
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True})


@bp.post('/api/categories/bulk')
@login_required
@module_required('expenses')
def upsert_expense_categories_bulk():
    payload = request.get_json(silent=True) or {}
    items = payload.get('items')
    items_list = items if isinstance(items, list) else []
    out = []
    for it in items_list:
        d = it if isinstance(it, dict) else {}
        cid = str(d.get('id') or '').strip() or uuid4().hex
        row = db.session.get(ExpenseCategory, cid)
        if not row:
            row = ExpenseCategory(id=cid, name=str(d.get('name') or d.get('categoria') or 'Categoría').strip() or 'Categoría')
            db.session.add(row)
        _apply_expense_category_payload(row, d)
        out.append(row)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'items': [_serialize_expense_category(r) for r in out]})
