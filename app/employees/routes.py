from datetime import date as dt_date
from uuid import uuid4

from flask import jsonify, render_template, request
from flask_login import login_required

from app import db
from app.models import Employee
from app.permissions import module_required
from app.employees import bp


def _parse_date_iso(raw, default=None):
    s = str(raw or '').strip()
    if not s:
        return default
    try:
        return dt_date.fromisoformat(s)
    except Exception:
        return default


def _serialize_employee(row: Employee):
    full_name = (str(row.first_name or '').strip() + ' ' + str(row.last_name or '').strip()).strip()
    return {
        'id': row.id,
        'first_name': row.first_name or '',
        'last_name': row.last_name or '',
        'name': (row.name or full_name or '').strip(),
        'hire_date': row.hire_date.isoformat() if row.hire_date else '',
        'default_payment_method': row.default_payment_method or '',
        'contract_type': row.contract_type or '',
        'status': row.status or ('Active' if row.active else 'Inactive'),
        'role': row.role or '',
        'birth_date': row.birth_date.isoformat() if row.birth_date else '',
        'document_id': row.document_id or '',
        'phone': row.phone or '',
        'email': row.email or '',
        'address': row.address or '',
        'reference_salary': row.reference_salary,
        'notes': row.notes or '',
        'active': bool(row.active),
    }


@bp.route('/')
@bp.route('/index')
@login_required
@module_required('employees')
def index():
    return render_template('employees/index.html', title='Empleados')


@bp.route('/new')
@login_required
@module_required('employees')
def new():
    return render_template('employees/new.html', title='Nuevo empleado', today=dt_date.today().isoformat())


def _apply_employee_payload(row: Employee, payload: dict):
    first_name = str(payload.get('first_name') or '').strip() or None
    last_name = str(payload.get('last_name') or '').strip() or None
    name = str(payload.get('name') or '').strip() or None
    hire_date = _parse_date_iso(payload.get('hire_date'), None)
    default_payment_method = str(payload.get('default_payment_method') or '').strip() or None
    contract_type = str(payload.get('contract_type') or '').strip() or None
    status = str(payload.get('status') or row.status or 'Active').strip() or 'Active'
    role = str(payload.get('role') or '').strip() or None
    birth_date = _parse_date_iso(payload.get('birth_date'), None)
    document_id = str(payload.get('document_id') or '').strip() or None
    phone = str(payload.get('phone') or '').strip() or None
    email = str(payload.get('email') or '').strip() or None
    address = str(payload.get('address') or '').strip() or None
    reference_salary = payload.get('reference_salary')
    try:
        reference_salary = float(reference_salary) if reference_salary is not None and reference_salary != '' else None
    except Exception:
        reference_salary = None
    notes = str(payload.get('notes') or '').strip() or None

    row.first_name = first_name
    row.last_name = last_name
    if not name:
        full = (str(first_name or '').strip() + ' ' + str(last_name or '').strip()).strip()
        name = full or None
    row.name = name

    row.hire_date = hire_date
    row.default_payment_method = default_payment_method
    row.contract_type = contract_type
    row.status = status
    row.role = role
    row.birth_date = birth_date
    row.document_id = document_id
    row.phone = phone
    row.email = email
    row.address = address
    row.reference_salary = reference_salary
    row.notes = notes

    if payload.get('active') is not None:
        row.active = bool(payload.get('active'))
    else:
        row.active = (status != 'Inactive')


@bp.get('/api/employees')
@login_required
@module_required('employees')
def list_employees_api():
    q = (request.args.get('q') or '').strip().lower()
    limit = int(request.args.get('limit') or 2000)
    if limit <= 0 or limit > 5000:
        limit = 2000

    query = db.session.query(Employee)
    if q:
        like = f"%{q}%"
        query = query.filter(
            (Employee.name.ilike(like))
            | (Employee.first_name.ilike(like))
            | (Employee.last_name.ilike(like))
            | (Employee.email.ilike(like))
            | (Employee.phone.ilike(like))
            | (Employee.document_id.ilike(like))
            | (Employee.role.ilike(like))
        )

    rows = query.order_by(Employee.updated_at.desc(), Employee.created_at.desc()).limit(limit).all()
    return jsonify({'ok': True, 'items': [_serialize_employee(r) for r in rows]})


@bp.get('/api/employees/<employee_id>')
@login_required
@module_required('employees')
def get_employee_api(employee_id):
    eid = str(employee_id or '').strip()
    row = db.session.get(Employee, eid)
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    return jsonify({'ok': True, 'item': _serialize_employee(row)})


@bp.post('/api/employees')
@login_required
@module_required('employees')
def create_employee_api():
    payload = request.get_json(silent=True) or {}
    eid = str(payload.get('id') or '').strip() or uuid4().hex

    row = db.session.get(Employee, eid)
    if row:
        return jsonify({'ok': False, 'error': 'already_exists'}), 400

    row = Employee(id=eid)
    _apply_employee_payload(row, payload)
    db.session.add(row)

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_employee(row)})


@bp.put('/api/employees/<employee_id>')
@login_required
@module_required('employees')
def update_employee_api(employee_id):
    eid = str(employee_id or '').strip()
    row = db.session.get(Employee, eid)
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    payload = request.get_json(silent=True) or {}
    _apply_employee_payload(row, payload)

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_employee(row)})


@bp.delete('/api/employees/<employee_id>')
@login_required
@module_required('employees')
def delete_employee_api(employee_id):
    eid = str(employee_id or '').strip()
    row = db.session.get(Employee, eid)
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    try:
        db.session.delete(row)
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True})


@bp.post('/api/employees/bulk')
@login_required
@module_required('employees')
def upsert_employees_bulk():
    payload = request.get_json(silent=True) or {}
    items = payload.get('items')
    items_list = items if isinstance(items, list) else []

    out = []
    for it in items_list:
        d = it if isinstance(it, dict) else {}
        eid = str(d.get('id') or '').strip() or uuid4().hex
        row = db.session.get(Employee, eid)
        if not row:
            row = Employee(id=eid)
            db.session.add(row)
        _apply_employee_payload(row, d)
        out.append(row)

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'items': [_serialize_employee(r) for r in out]})
