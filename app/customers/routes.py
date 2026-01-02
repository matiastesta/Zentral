from datetime import date as dt_date
from datetime import datetime
from uuid import uuid4

from flask import jsonify, render_template, request
from flask_login import login_required

from app import db
from app.models import Customer
from app.permissions import module_required, module_required_any
from app.customers import bp


def _company_id() -> str:
    try:
        from flask import g
        return str(getattr(g, 'company_id', '') or '').strip()
    except Exception:
        return ''


def _dt_to_ms(dt):
    try:
        return int(dt.timestamp() * 1000) if dt else None
    except Exception:
        return None


def _parse_date_iso(raw, default=None):
    s = str(raw or '').strip()
    if not s:
        return default
    try:
        return dt_date.fromisoformat(s)
    except Exception:
        return default


def _serialize_customer(row: Customer):
    full_name = (str(row.first_name or '').strip() + ' ' + str(row.last_name or '').strip()).strip()
    return {
        'id': row.id,
        'first_name': row.first_name or '',
        'last_name': row.last_name or '',
        'name': (row.name or full_name or '').strip(),
        'email': row.email or '',
        'phone': row.phone or '',
        'birthday': row.birthday.isoformat() if row.birthday else '',
        'address': row.address or '',
        'notes': row.notes or '',
        'status': row.status or 'activo',
        'created_at': _dt_to_ms(row.created_at),
        'updated_at': _dt_to_ms(row.updated_at),
    }


@bp.route("/")
@bp.route("/index")
@login_required
@module_required('customers')
def index():
    """Listado básico de clientes (dummy)."""
    return render_template("customers/list.html", title="Clientes")


@bp.get('/api/customers')
@login_required
@module_required('customers')
def list_customers_api():
    qraw = (request.args.get('q') or '').strip()
    limit = int(request.args.get('limit') or 500)
    if limit <= 0 or limit > 5000:
        limit = 500
    offset = int(request.args.get('offset') or 0)
    if offset < 0:
        offset = 0

    company_id = _company_id()
    if not company_id:
        return jsonify({'ok': True, 'items': [], 'has_more': False, 'next_offset': None})

    query = db.session.query(Customer).filter(Customer.company_id == company_id)
    if qraw:
        like = f"%{qraw}%"
        query = query.filter(
            (Customer.name.ilike(like))
            | (Customer.first_name.ilike(like))
            | (Customer.last_name.ilike(like))
            | (Customer.email.ilike(like))
            | (Customer.phone.ilike(like))
        )
    query = query.order_by(Customer.updated_at.desc(), Customer.created_at.desc(), Customer.id.asc())
    rows = query.offset(offset).limit(limit + 1).all()
    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]
    next_offset = (offset + limit) if has_more else None
    return jsonify({'ok': True, 'items': [_serialize_customer(r) for r in rows], 'has_more': has_more, 'next_offset': next_offset})


@bp.get('/api/customers/<customer_id>')
@login_required
@module_required('customers')
def get_customer_api(customer_id):
    cid = str(customer_id or '').strip()
    company_id = _company_id()
    if not company_id:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    row = db.session.query(Customer).filter(Customer.company_id == company_id, Customer.id == cid).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    return jsonify({'ok': True, 'item': _serialize_customer(row)})


def _apply_customer_payload(row: Customer, payload: dict):
    first_name = str(payload.get('first_name') or payload.get('nombre') or '').strip() or None
    last_name = str(payload.get('last_name') or payload.get('apellido') or '').strip() or None
    name = str(payload.get('name') or '').strip() or None
    email = str(payload.get('email') or '').strip() or None
    phone = str(payload.get('phone') or payload.get('telefono') or '').strip() or None
    birthday = _parse_date_iso(payload.get('birthday') or payload.get('fecha_cumpleanos'), None)
    address = str(payload.get('address') or payload.get('direccion') or '').strip() or None
    notes = str(payload.get('notes') or payload.get('observaciones') or '').strip() or None
    status = str(row.status or 'activo').strip() or 'activo'

    row.first_name = first_name
    row.last_name = last_name
    row.email = email
    row.phone = phone
    row.birthday = birthday
    row.address = address
    row.notes = notes
    row.status = status

    if not name:
        full = (str(first_name or '').strip() + ' ' + str(last_name or '').strip()).strip()
        name = full or None
    row.name = name


@bp.post('/api/customers')
@login_required
@module_required('customers')
def create_customer_api():
    payload = request.get_json(silent=True) or {}
    cid = str(payload.get('id') or '').strip() or uuid4().hex

    company_id = _company_id()
    if not company_id:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    row = db.session.query(Customer).filter(Customer.company_id == company_id, Customer.id == cid).first()
    if row:
        return jsonify({'ok': False, 'error': 'already_exists'}), 400

    row = Customer(id=cid, company_id=company_id)
    _apply_customer_payload(row, payload)

    db.session.add(row)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_customer(row)})


@bp.put('/api/customers/<customer_id>')
@login_required
@module_required('customers')
def update_customer_api(customer_id):
    cid = str(customer_id or '').strip()
    company_id = _company_id()
    if not company_id:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    row = db.session.query(Customer).filter(Customer.company_id == company_id, Customer.id == cid).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    payload = request.get_json(silent=True) or {}
    _apply_customer_payload(row, payload)

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_customer(row)})


@bp.delete('/api/customers/<customer_id>')
@login_required
@module_required('customers')
def delete_customer_api(customer_id):
    cid = str(customer_id or '').strip()
    company_id = _company_id()
    if not company_id:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    row = db.session.query(Customer).filter(Customer.company_id == company_id, Customer.id == cid).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    try:
        db.session.delete(row)
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True})


@bp.post('/api/customers/bulk')
@login_required
@module_required('customers')
def upsert_customers_bulk():
    payload = request.get_json(silent=True) or {}
    items = payload.get('items')
    items_list = items if isinstance(items, list) else []

    company_id = _company_id()
    if not company_id:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    out = []
    for it in items_list:
        d = it if isinstance(it, dict) else {}
        cid = str(d.get('id') or '').strip() or uuid4().hex
        row = db.session.query(Customer).filter(Customer.company_id == company_id, Customer.id == cid).first()
        if not row:
            row = Customer(id=cid, company_id=company_id)
            db.session.add(row)
        _apply_customer_payload(row, d)
        out.append(row)

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'items': [_serialize_customer(r) for r in out]})
