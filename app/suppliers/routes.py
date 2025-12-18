import json
from uuid import uuid4

from flask import jsonify, render_template, request
from flask_login import login_required

from app import db
from app.models import Supplier
from app.permissions import module_required
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



@bp.route('/')
@bp.route('/index')
@login_required
@module_required('suppliers')
def index():
    """Gestor de proveedores (dummy)."""
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
    query = db.session.query(Supplier)
    if q:
        like = f"%{q}%"
        query = query.filter(Supplier.name.ilike(like))
    rows = query.order_by(Supplier.updated_at.desc(), Supplier.created_at.desc()).limit(limit).all()
    return jsonify({'ok': True, 'items': [_serialize_supplier(r) for r in rows]})


@bp.get('/api/suppliers/<supplier_id>')
@login_required
@module_required('suppliers')
def get_supplier_api(supplier_id):
    sid = str(supplier_id or '').strip()
    row = db.session.get(Supplier, sid)
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    return jsonify({'ok': True, 'item': _serialize_supplier(row)})


@bp.post('/api/suppliers')
@login_required
@module_required('suppliers')
def create_supplier_api():
    payload = request.get_json(silent=True) or {}
    sid = str(payload.get('id') or '').strip() or uuid4().hex
    row = db.session.get(Supplier, sid)
    if row:
        return jsonify({'ok': False, 'error': 'already_exists'}), 400
    row = Supplier(id=sid, name=str(payload.get('name') or '').strip() or 'Proveedor')
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
    sid = str(supplier_id or '').strip()
    row = db.session.get(Supplier, sid)
    if not row:
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
    sid = str(supplier_id or '').strip()
    row = db.session.get(Supplier, sid)
    if not row:
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
    payload = request.get_json(silent=True) or {}
    items = payload.get('items')
    items_list = items if isinstance(items, list) else []
    out = []
    for it in items_list:
        d = it if isinstance(it, dict) else {}
        sid = str(d.get('id') or '').strip() or uuid4().hex
        row = db.session.get(Supplier, sid)
        if not row:
            row = Supplier(id=sid, name=str(d.get('name') or '').strip() or 'Proveedor')
            db.session.add(row)
        _apply_supplier_payload(row, d)
        out.append(row)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'items': [_serialize_supplier(r) for r in out]})
