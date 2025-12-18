from datetime import date as dt_date, datetime

from flask import jsonify, render_template, request
from flask_login import login_required
from sqlalchemy import or_
from typing import Optional

from app import db
from app.models import Category, InventoryLot, InventoryMovement, Product
from app.permissions import module_required
from app.inventory import bp


@bp.route('/')
@bp.route('/index')
@login_required
@module_required('inventory')
def index():
    """Inventario avanzado."""
    items = []
    return render_template('inventory/index.html', title='Inventario', items=items)


def _parse_date_iso(raw, fallback=None):
    try:
        return dt_date.fromisoformat(str(raw).strip())
    except Exception:
        return fallback


def _num(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def _serialize_category(cat: Optional[Category]):
    if not cat:
        return None
    return {
        'id': cat.id,
        'name': cat.name,
        'parent_id': cat.parent_id,
    }


def _serialize_product(p: Product):
    return {
        'id': p.id,
        'name': p.name,
        'description': p.description or '',
        'category_id': p.category_id,
        'category': _serialize_category(p.category) if p.category else None,
        'sale_price': p.sale_price,
        'internal_code': p.internal_code or '',
        'barcode': p.barcode or '',
        'unit_name': p.unit_name or '',
        'uses_lots': bool(p.uses_lots),
        'method': p.method or 'FIFO',
        'min_stock': p.min_stock,
        'active': bool(p.active),
    }


def _serialize_lot(l: InventoryLot):
    return {
        'id': l.id,
        'product_id': l.product_id,
        'product_name': (l.product.name if l.product else ''),
        'qty_initial': l.qty_initial,
        'qty_available': l.qty_available,
        'unit_cost': l.unit_cost,
        'received_at': l.received_at.isoformat() if l.received_at else None,
        'supplier_name': l.supplier_name or '',
        'expiration_date': l.expiration_date.isoformat() if l.expiration_date else None,
        'lot_code': l.lot_code or '',
        'note': l.note or '',
        'origin_sale_ticket': l.origin_sale_ticket or '',
    }


def _serialize_movement(m: InventoryMovement):
    return {
        'id': m.id,
        'date': m.movement_date.isoformat() if m.movement_date else '',
        'type': m.type,
        'sale_ticket': m.sale_ticket or '',
        'product_id': m.product_id,
        'product_name': (m.product.name if m.product else ''),
        'lot_id': m.lot_id,
        'qty_delta': m.qty_delta,
        'unit_cost': m.unit_cost,
        'total_cost': m.total_cost,
        'created_at': m.created_at.isoformat() if m.created_at else None,
    }


@bp.get('/api/products')
@login_required
@module_required('inventory')
def list_products():
    limit = int(request.args.get('limit') or 500)
    if limit <= 0 or limit > 5000:
        limit = 500
    active = (request.args.get('active') or '').strip()
    q = db.session.query(Product)
    if active in ('1', 'true', 'True'):
        q = q.filter(Product.active == True)  # noqa: E712
    q = q.order_by(Product.name.asc()).limit(limit)
    rows = q.all()
    return jsonify({'ok': True, 'items': [_serialize_product(r) for r in rows]})


@bp.get('/api/products/search')
@login_required
@module_required('inventory')
def search_products():
    qraw = (request.args.get('q') or '').strip()
    limit = int(request.args.get('limit') or 50)
    if limit <= 0 or limit > 200:
        limit = 50

    q = db.session.query(Product).filter(Product.active == True)  # noqa: E712
    if qraw:
        term = f"%{qraw}%"
        q = q.filter(
            or_(
                Product.name.ilike(term),
                Product.internal_code.ilike(term),
                Product.barcode.ilike(term),
            )
        )
    q = q.order_by(Product.name.asc()).limit(limit)
    rows = q.all()
    return jsonify({'ok': True, 'items': [_serialize_product(r) for r in rows]})


@bp.post('/api/products')
@login_required
@module_required('inventory')
def create_product():
    payload = request.get_json(silent=True) or {}
    name = str(payload.get('name') or '').strip()
    if not name:
        return jsonify({'ok': False, 'error': 'name_required'}), 400

    cat_name = str(payload.get('category_name') or '').strip()
    cat_id = payload.get('category_id')
    category = None
    if cat_id:
        try:
            category = db.session.get(Category, int(cat_id))
        except Exception:
            category = None
    if not category and cat_name:
        category = db.session.query(Category).filter(Category.name == cat_name, Category.parent_id == None).first()  # noqa: E711
        if not category:
            category = Category(name=cat_name)
            db.session.add(category)
            db.session.flush()

    row = Product(
        name=name,
        description=str(payload.get('description') or '').strip() or None,
        category_id=(category.id if category else None),
        sale_price=_num(payload.get('sale_price')),
        internal_code=str(payload.get('internal_code') or '').strip() or None,
        barcode=str(payload.get('barcode') or '').strip() or None,
        unit_name=str(payload.get('unit_name') or '').strip() or None,
        uses_lots=bool(payload.get('uses_lots', True)),
        method=str(payload.get('method') or 'FIFO').strip() or 'FIFO',
        min_stock=_num(payload.get('min_stock')),
        active=bool(payload.get('active', True)),
    )
    db.session.add(row)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_product(row)})


@bp.put('/api/products/<int:product_id>')
@login_required
@module_required('inventory')
def update_product(product_id: int):
    row = db.session.get(Product, int(product_id))
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    payload = request.get_json(silent=True) or {}
    name = str(payload.get('name') or row.name).strip()
    if not name:
        return jsonify({'ok': False, 'error': 'name_required'}), 400

    row.name = name
    row.description = str(payload.get('description') or '').strip() or None
    row.sale_price = _num(payload.get('sale_price') if payload.get('sale_price') is not None else row.sale_price)
    row.internal_code = str(payload.get('internal_code') or '').strip() or None
    row.barcode = str(payload.get('barcode') or '').strip() or None
    if payload.get('unit_name') is not None:
        row.unit_name = str(payload.get('unit_name') or '').strip() or None
    if payload.get('uses_lots') is not None:
        row.uses_lots = bool(payload.get('uses_lots'))
    if payload.get('method') is not None:
        row.method = str(payload.get('method') or '').strip() or 'FIFO'
    if payload.get('min_stock') is not None:
        row.min_stock = _num(payload.get('min_stock'))
    if payload.get('active') is not None:
        row.active = bool(payload.get('active'))

    db.session.commit()
    return jsonify({'ok': True, 'item': _serialize_product(row)})


@bp.post('/api/products/prices')
@login_required
@module_required('inventory')
def bulk_update_product_prices():
    payload = request.get_json(silent=True) or {}
    items = payload.get('items')
    if not isinstance(items, list) or not items:
        return jsonify({'ok': False, 'error': 'items_required'}), 400

    updated = []
    errors = []

    for it in items:
        d = it if isinstance(it, dict) else {}
        pid_raw = d.get('id')
        try:
            pid = int(pid_raw)
        except Exception:
            errors.append({'id': pid_raw, 'error': 'invalid_id'})
            continue

        row = db.session.get(Product, pid)
        if not row:
            errors.append({'id': pid, 'error': 'not_found'})
            continue

        if 'sale_price' not in d:
            errors.append({'id': pid, 'error': 'sale_price_required'})
            continue

        row.sale_price = _num(d.get('sale_price'))
        updated.append(row)

    if not updated and errors:
        return jsonify({'ok': False, 'error': 'no_updates', 'errors': errors}), 400

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400

    return jsonify({'ok': True, 'items': [_serialize_product(r) for r in updated], 'errors': errors})


@bp.get('/api/lots')
@login_required
@module_required('inventory')
def list_lots():
    limit = int(request.args.get('limit') or 2000)
    if limit <= 0 or limit > 10000:
        limit = 2000
    product_id = (request.args.get('product_id') or '').strip()
    q = db.session.query(InventoryLot).join(Product)
    if product_id:
        try:
            q = q.filter(InventoryLot.product_id == int(product_id))
        except Exception:
            return jsonify({'ok': True, 'items': []})
    q = q.order_by(InventoryLot.received_at.desc(), InventoryLot.id.desc()).limit(limit)
    rows = q.all()
    return jsonify({'ok': True, 'items': [_serialize_lot(r) for r in rows]})


@bp.post('/api/lots')
@login_required
@module_required('inventory')
def create_lot():
    payload = request.get_json(silent=True) or {}
    try:
        product_id = int(payload.get('product_id'))
    except Exception:
        return jsonify({'ok': False, 'error': 'product_id_required'}), 400

    prod = db.session.get(Product, product_id)
    if not prod:
        return jsonify({'ok': False, 'error': 'product_not_found'}), 404

    qty = _num(payload.get('qty'))
    if qty <= 0:
        return jsonify({'ok': False, 'error': 'qty_required'}), 400

    unit_cost = _num(payload.get('unit_cost'))
    supplier_name = str(payload.get('supplier_name') or '').strip() or None
    lot_code = str(payload.get('lot_code') or '').strip() or None
    note = str(payload.get('note') or '').strip() or None
    exp_raw = str(payload.get('expiration_date') or '').strip()
    exp_dt = _parse_date_iso(exp_raw, None) if exp_raw else None
    row = InventoryLot(
        product_id=product_id,
        qty_initial=qty,
        qty_available=qty,
        unit_cost=unit_cost,
        supplier_name=supplier_name,
        expiration_date=exp_dt,
        lot_code=lot_code,
        note=note,
    )
    db.session.add(row)
    db.session.flush()

    movement_date = _parse_date_iso(payload.get('date'), dt_date.today())
    try:
        received_at = datetime.combine(movement_date, datetime.min.time())
    except Exception:
        received_at = datetime.utcnow()

    mv = InventoryMovement(
        movement_date=movement_date,
        type='purchase',
        sale_ticket=None,
        product_id=product_id,
        lot_id=row.id,
        qty_delta=qty,
        unit_cost=unit_cost,
        total_cost=qty * unit_cost,
    )
    row.received_at = received_at
    db.session.add(mv)
    db.session.commit()
    return jsonify({'ok': True, 'item': _serialize_lot(row)})


@bp.delete('/api/lots/<int:lot_id>')
@login_required
@module_required('inventory')
def delete_lot(lot_id: int):
    lot = db.session.get(InventoryLot, int(lot_id))
    if not lot:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    # No permitir borrar si el lote tiene consumos registrados
    used = db.session.query(InventoryMovement.id).filter(InventoryMovement.lot_id == lot.id).filter(InventoryMovement.type == 'sale').first()
    if used:
        return jsonify({'ok': False, 'error': 'lot_has_sales'}), 400

    # borrar movimientos asociados (compras/ajustes/return) y el lote
    db.session.query(InventoryMovement).filter(InventoryMovement.lot_id == lot.id).delete(synchronize_session=False)
    db.session.delete(lot)
    db.session.commit()
    return jsonify({'ok': True})


@bp.get('/api/movements')
@login_required
@module_required('inventory')
def list_inventory_movements():
    raw_from = (request.args.get('from') or '').strip()
    raw_to = (request.args.get('to') or '').strip()
    product_id = (request.args.get('product_id') or '').strip()
    limit = int(request.args.get('limit') or 500)
    if limit <= 0 or limit > 5000:
        limit = 500

    d_from = _parse_date_iso(raw_from, None)
    d_to = _parse_date_iso(raw_to, None)
    q = db.session.query(InventoryMovement)
    if d_from:
        q = q.filter(InventoryMovement.movement_date >= d_from)
    if d_to:
        q = q.filter(InventoryMovement.movement_date <= d_to)
    if product_id:
        try:
            q = q.filter(InventoryMovement.product_id == int(product_id))
        except Exception:
            return jsonify({'ok': True, 'items': []})
    q = q.order_by(InventoryMovement.movement_date.desc(), InventoryMovement.id.desc()).limit(limit)
    rows = q.all()
    return jsonify({'ok': True, 'items': [_serialize_movement(r) for r in rows]})
