from datetime import date as dt_date, datetime

import json
import os
from uuid import uuid4

from flask import current_app, jsonify, render_template, request, url_for
from flask import g
from flask_login import login_required
from sqlalchemy import func, or_
from typing import Optional
from werkzeug.utils import secure_filename

from app import db
from app.models import Category, Expense, FileAsset, InventoryLot, InventoryMovement, Product, Sale, Supplier
from app.files.storage import upload_to_r2_and_create_asset
from app.permissions import module_required, module_required_any
from app.tenancy import ensure_request_context
from app.inventory import bp


def _company_id() -> str:
    try:
        return str(getattr(g, 'company_id', '') or '').strip()
    except Exception:
        return ''


@bp.route('/')
@bp.route('/index')
@login_required
@module_required('inventory')
def index():
    """Inventario avanzado."""
    return render_template('inventory/index.html', title='Inventario')


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
        'active': bool(getattr(cat, 'active', True)),
    }


def _image_url(p: Product):
    file_id = str(getattr(p, 'image_file_id', '') or '').strip()
    if file_id:
        try:
            return url_for('files.download_file_api', file_id=file_id)
        except Exception:
            return ''
    filename = str(getattr(p, 'image_filename', '') or '').strip()
    if not filename:
        return ''
    try:
        return url_for('static', filename=f'uploads/{filename}')
    except Exception:
        return ''


def _serialize_product(p: Product):
    cat_obj = _serialize_category(p.category) if p.category else None
    cat_name = ''
    try:
        cat_name = str((cat_obj or {}).get('name') or '').strip()
    except Exception:
        cat_name = ''
    return {
        'id': p.id,
        'name': p.name,
        'description': p.description or '',
        'category_id': p.category_id,
        'category': cat_name,
        'category_obj': cat_obj,
        'sale_price': p.sale_price,
        'internal_code': p.internal_code or '',
        'barcode': p.barcode or '',
        'image_url': _image_url(p),
        'unit_name': p.unit_name or '',
        'uses_lots': bool(p.uses_lots),
        'method': p.method or 'FIFO',
        'min_stock': p.min_stock,
        'reorder_point': getattr(p, 'reorder_point', 0.0) or 0.0,
        'primary_supplier_id': str(getattr(p, 'primary_supplier_id', '') or ''),
        'primary_supplier_name': getattr(p, 'primary_supplier_name', '') or '',
        'active': bool(p.active),
    }


def _normalize_name(name: str) -> str:
    return str(name or '').strip().lower()


def _product_method_locked(product_id: int) -> bool:
    has_lots = db.session.query(InventoryLot.id).filter(InventoryLot.product_id == int(product_id)).first() is not None
    if has_lots:
        return True
    has_movements = db.session.query(InventoryMovement.id).filter(InventoryMovement.product_id == int(product_id)).first() is not None
    return bool(has_movements)


def _generate_unique_internal_code() -> str:
    for _ in range(10):
        code = 'SKU-' + uuid4().hex[:10].upper()
        exists = db.session.query(Product.id).filter(Product.internal_code == code).first() is not None
        if not exists:
            return code
    return 'SKU-' + uuid4().hex.upper()


def _allowed_image(filename: str) -> bool:
    ext = os.path.splitext(filename or '')[1].lower()
    return ext in ('.png', '.jpg', '.jpeg')


@bp.get('/api/categories')
@login_required
@module_required('inventory')
def list_categories_api():
    limit = int(request.args.get('limit') or 5000)
    if limit <= 0 or limit > 10000:
        limit = 5000
    try:
        ensure_request_context()
    except Exception:
        pass
    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': True, 'items': []})

    q = db.session.query(Category).filter(Category.company_id == cid, Category.parent_id == None)  # noqa: E711
    active = (request.args.get('active') or '').strip()
    if active in ('1', 'true', 'True'):
        q = q.filter(Category.active == True)  # noqa: E712
    q = q.order_by(Category.name.asc()).limit(limit)
    rows = q.all()
    return jsonify({'ok': True, 'items': [_serialize_category(r) for r in rows]})


@bp.post('/api/categories')
@login_required
@module_required('inventory')
def create_category_api():
    payload = request.get_json(silent=True) or {}
    try:
        ensure_request_context()
    except Exception:
        pass
    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    name = str(payload.get('name') or '').strip()
    if not name:
        return jsonify({'ok': False, 'error': 'name_required'}), 400
    norm = _normalize_name(name)
    existing = (
        db.session.query(Category)
        .filter(Category.company_id == cid, func.lower(Category.name) == norm, Category.parent_id == None)
        .first()
    )  # noqa: E711
    if existing:
        return jsonify({'ok': False, 'error': 'already_exists', 'item': _serialize_category(existing)}), 400
    row = Category(company_id=cid, name=name, active=bool(payload.get('active', True)))
    db.session.add(row)
    db.session.commit()
    return jsonify({'ok': True, 'item': _serialize_category(row)})


@bp.delete('/api/categories/<int:category_id>')
@login_required
@module_required('inventory')
def delete_category_api(category_id: int):
    try:
        ensure_request_context()
    except Exception:
        pass
    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    try:
        cat_id = int(category_id)
    except Exception:
        return jsonify({'ok': False, 'error': 'invalid_id'}), 400

    row = db.session.query(Category).filter(Category.company_id == cid, Category.id == cat_id).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    child = db.session.query(Category.id).filter(Category.company_id == cid, Category.parent_id == row.id).first()
    if child:
        return jsonify({'ok': False, 'error': 'has_children'}), 400

    used = db.session.query(Product.id).filter(Product.company_id == cid, Product.category_id == row.id).first()
    if used:
        return jsonify({'ok': False, 'error': 'in_use'}), 400

    db.session.delete(row)
    db.session.commit()
    return jsonify({'ok': True})


@bp.get('/api/products/<int:product_id>/method_lock')
@login_required
@module_required('inventory')
def get_product_method_lock(product_id: int):
    row = db.session.get(Product, int(product_id))
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    locked = _product_method_locked(row.id)
    return jsonify({'ok': True, 'locked': bool(locked)})


def _serialize_lot(l: InventoryLot):
    return {
        'id': l.id,
        'product_id': l.product_id,
        'product_name': (l.product.name if l.product else ''),
        'qty_initial': l.qty_initial,
        'qty_available': l.qty_available,
        'unit_cost': l.unit_cost,
        'received_at': l.received_at.isoformat() if l.received_at else None,
        'supplier_id': str(getattr(l, 'supplier_id', '') or ''),
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
    try:
        ensure_request_context()
    except Exception:
        pass
    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': True, 'items': []})

    q = db.session.query(Product).filter(Product.company_id == cid)
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

    try:
        ensure_request_context()
    except Exception:
        pass
    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': True, 'items': []})

    q = db.session.query(Product).filter(Product.company_id == cid, Product.active == True)  # noqa: E712
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
    try:
        ensure_request_context()
    except Exception:
        pass
    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    name = str(payload.get('name') or '').strip()
    if not name:
        return jsonify({'ok': False, 'error': 'name_required'}), 400

    cat_id = payload.get('category_id')
    category = None
    if cat_id is not None and cat_id != '':
        try:
            category = db.session.query(Category).filter(Category.company_id == cid, Category.id == int(cat_id)).first()
        except Exception:
            category = None
    if not category:
        return jsonify({'ok': False, 'error': 'category_required'}), 400
    if hasattr(category, 'active') and not bool(category.active):
        return jsonify({'ok': False, 'error': 'category_inactive'}), 400

    internal_code = str(payload.get('internal_code') or '').strip() or None
    if not internal_code:
        internal_code = _generate_unique_internal_code()

    supplier_id = str(payload.get('primary_supplier_id') or '').strip() or None
    supplier_name = str(payload.get('primary_supplier_name') or '').strip() or None
    supplier_row = None
    if supplier_id:
        supplier_row = db.session.query(Supplier).filter(Supplier.company_id == cid, Supplier.id == supplier_id).first()
        if not supplier_row:
            return jsonify({'ok': False, 'error': 'supplier_not_found'}), 400
        supplier_name = str(supplier_row.name or '').strip() or supplier_name

    row = Product(
        company_id=cid,
        name=name,
        description=str(payload.get('description') or '').strip() or None,
        category_id=(category.id if category else None),
        sale_price=_num(payload.get('sale_price')),
        internal_code=internal_code,
        barcode=str(payload.get('barcode') or '').strip() or None,
        image_filename=str(payload.get('image_filename') or '').strip() or None,
        unit_name=str(payload.get('unit_name') or '').strip() or None,
        uses_lots=bool(payload.get('uses_lots', True)),
        method=str(payload.get('method') or 'FIFO').strip() or 'FIFO',
        min_stock=_num(payload.get('min_stock')),
        reorder_point=_num(payload.get('reorder_point')),
        primary_supplier_id=supplier_id,
        primary_supplier_name=supplier_name,
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

    cat_id = payload.get('category_id')
    category = None
    if cat_id is not None and cat_id != '':
        try:
            category = db.session.get(Category, int(cat_id))
        except Exception:
            category = None
    if category and hasattr(category, 'active') and not bool(category.active):
        return jsonify({'ok': False, 'error': 'category_inactive'}), 400

    row.name = name
    row.description = str(payload.get('description') or '').strip() or None
    if category:
        row.category_id = category.id
    row.sale_price = _num(payload.get('sale_price') if payload.get('sale_price') is not None else row.sale_price)
    if payload.get('internal_code') is not None:
        row.internal_code = str(payload.get('internal_code') or '').strip() or None
    row.barcode = str(payload.get('barcode') or '').strip() or None
    if payload.get('primary_supplier_id') is not None:
        next_sid = str(payload.get('primary_supplier_id') or '').strip() or None
        if next_sid:
            srow = db.session.get(Supplier, next_sid)
            if not srow:
                return jsonify({'ok': False, 'error': 'supplier_not_found'}), 400
            row.primary_supplier_id = next_sid
            row.primary_supplier_name = str(srow.name or '').strip() or row.primary_supplier_name
        else:
            row.primary_supplier_id = None

    if payload.get('primary_supplier_name') is not None:
        next_name = str(payload.get('primary_supplier_name') or '').strip() or None
        row.primary_supplier_name = next_name
        if next_name is None:
            row.primary_supplier_id = None
    if payload.get('reorder_point') is not None:
        row.reorder_point = _num(payload.get('reorder_point'))
    if payload.get('unit_name') is not None:
        row.unit_name = str(payload.get('unit_name') or '').strip() or None
    if payload.get('uses_lots') is not None:
        row.uses_lots = bool(payload.get('uses_lots'))
    if payload.get('method') is not None:
        next_method = str(payload.get('method') or '').strip() or 'FIFO'
        if next_method != (row.method or 'FIFO'):
            if _product_method_locked(row.id):
                return jsonify({'ok': False, 'error': 'method_locked'}), 400
            row.method = next_method
    if payload.get('min_stock') is not None:
        row.min_stock = _num(payload.get('min_stock'))
    if payload.get('active') is not None:
        row.active = bool(payload.get('active'))

    db.session.commit()
    return jsonify({'ok': True, 'item': _serialize_product(row)})


@bp.post('/api/products/<int:product_id>/image')
@login_required
@module_required('inventory')
def upload_product_image(product_id: int):
    try:
        ensure_request_context()
    except Exception:
        pass
    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    row = db.session.query(Product).filter(Product.company_id == cid, Product.id == int(product_id)).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    f = request.files.get('image')
    if not f or not getattr(f, 'filename', ''):
        return jsonify({'ok': False, 'error': 'file_required'}), 400
    if not _allowed_image(f.filename):
        return jsonify({'ok': False, 'error': 'invalid_file_type'}), 400

    try:
        asset = upload_to_r2_and_create_asset(
            company_id=cid,
            file_storage=f,
            entity_type='product',
            entity_id=str(row.id),
            key_prefix='products/images',
        )
        row.image_file_id = asset.id
        row.image_filename = None
        db.session.commit()
        return jsonify({'ok': True, 'item': _serialize_product(row)})
    except Exception:
        current_app.logger.exception('Failed to upload product image to R2')
        try:
            db.session.rollback()
        except Exception:
            pass
        return jsonify({'ok': False, 'error': 'upload_failed'}), 400


@bp.delete('/api/products/<int:product_id>')
@login_required
@module_required('inventory')
def delete_product(product_id: int):
    try:
        ensure_request_context()
    except Exception:
        pass
    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    row = db.session.query(Product).filter(Product.company_id == cid, Product.id == int(product_id)).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    has_lots = db.session.query(InventoryLot.id).filter(InventoryLot.company_id == cid, InventoryLot.product_id == row.id).first() is not None
    has_movements = db.session.query(InventoryMovement.id).filter(InventoryMovement.company_id == cid, InventoryMovement.product_id == row.id).first() is not None

    # Para mantener integridad y UX consistente, se desactiva (no se elimina) desde la UI.
    row.active = False
    db.session.commit()
    return jsonify({'ok': True, 'soft_deleted': True, 'had_history': bool(has_lots or has_movements)})


@bp.delete('/api/products/<int:product_id>/hard')
@login_required
@module_required('inventory')
def hard_delete_product(product_id: int):
    try:
        ensure_request_context()
    except Exception:
        pass
    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    row = db.session.query(Product).filter(Product.company_id == cid, Product.id == int(product_id)).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    try:
        img_asset_id = str(getattr(row, 'image_file_id', '') or '').strip()

        db.session.query(InventoryMovement).filter(
            InventoryMovement.company_id == cid,
            InventoryMovement.product_id == row.id,
        ).delete(synchronize_session=False)

        db.session.query(InventoryLot).filter(
            InventoryLot.company_id == cid,
            InventoryLot.product_id == row.id,
        ).delete(synchronize_session=False)

        if img_asset_id:
            db.session.query(FileAsset).filter(FileAsset.company_id == cid, FileAsset.id == img_asset_id).delete(synchronize_session=False)
        db.session.query(FileAsset).filter(
            FileAsset.company_id == cid,
            FileAsset.entity_type == 'product',
            FileAsset.entity_id == str(row.id),
        ).delete(synchronize_session=False)

        db.session.delete(row)
        db.session.commit()
        return jsonify({'ok': True})
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400


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
    limit = int(request.args.get('limit') or 5000)
    if limit <= 0 or limit > 20000:
        limit = 5000
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
    supplier_id = str(payload.get('supplier_id') or '').strip() or None
    supplier_name = str(payload.get('supplier_name') or '').strip() or None
    if supplier_id:
        srow = db.session.get(Supplier, supplier_id)
        if not srow:
            return jsonify({'ok': False, 'error': 'supplier_not_found'}), 400
        supplier_name = str(srow.name or '').strip() or supplier_name
    lot_code = str(payload.get('lot_code') or '').strip() or None
    note = str(payload.get('note') or '').strip() or None
    exp_raw = str(payload.get('expiration_date') or '').strip()
    exp_dt = _parse_date_iso(exp_raw, None) if exp_raw else None
    row = InventoryLot(
        product_id=product_id,
        qty_initial=qty,
        qty_available=qty,
        unit_cost=unit_cost,
        supplier_id=supplier_id,
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


@bp.put('/api/lots/<int:lot_id>')
@login_required
@module_required('inventory')
def update_lot(lot_id: int):
    lot = db.session.get(InventoryLot, int(lot_id))
    if not lot:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    payload = request.get_json(silent=True) or {}

    qty_initial_raw = payload.get('qty_initial')
    unit_cost_raw = payload.get('unit_cost')
    wants_qty_cost_change = (qty_initial_raw is not None) or (unit_cost_raw is not None)

    date_raw = payload.get('date')
    wants_date_change = date_raw is not None

    # Proteger consistencia: si el lote ya tuvo consumos, no permitimos cambiar cantidad/costo.
    # FIFO/PEPS depende de received_at: si ya hay consumos u otros movimientos aplicados al lote,
    # no se deben permitir cambios que alteren el orden o la magnitud del ingreso.
    if wants_qty_cost_change or wants_date_change:
        blocked = (
            db.session.query(InventoryMovement.id)
            .filter(InventoryMovement.lot_id == lot.id)
            .filter(InventoryMovement.type.notin_(['purchase', 'return']))
            .first()
        )
        if blocked:
            return jsonify({'ok': False, 'error': 'lot_has_sales'}), 400

        used_sale = (
            db.session.query(InventoryMovement.id)
            .filter(InventoryMovement.lot_id == lot.id)
            .filter(InventoryMovement.type == 'sale')
            .first()
        )
        if used_sale:
            return jsonify({'ok': False, 'error': 'lot_has_sales'}), 400

    qty_initial = None
    unit_cost = None
    if qty_initial_raw is not None:
        qty_initial = _num(qty_initial_raw)
        if qty_initial <= 0:
            return jsonify({'ok': False, 'error': 'qty_required'}), 400
    if unit_cost_raw is not None:
        unit_cost = _num(unit_cost_raw)
        if unit_cost < 0:
            return jsonify({'ok': False, 'error': 'unit_cost_invalid'}), 400

    supplier_id = str(payload.get('supplier_id') or '').strip() or None
    supplier_name = str(payload.get('supplier_name') or '').strip() or None
    if supplier_id:
        srow = db.session.get(Supplier, supplier_id)
        if not srow:
            return jsonify({'ok': False, 'error': 'supplier_not_found'}), 400
        supplier_name = str(srow.name or '').strip() or supplier_name

    lot_code = str(payload.get('lot_code') or '').strip() or None
    note = str(payload.get('note') or '').strip() or None

    exp_raw = str(payload.get('expiration_date') or '').strip()
    exp_dt = _parse_date_iso(exp_raw, None) if exp_raw else None

    movement_date = _parse_date_iso(payload.get('date'), None)
    if not movement_date:
        return jsonify({'ok': False, 'error': 'date_required'}), 400

    try:
        received_at = datetime.combine(movement_date, datetime.min.time())
    except Exception:
        received_at = datetime.utcnow()

    lot.supplier_id = supplier_id
    lot.supplier_name = supplier_name
    lot.lot_code = lot_code
    lot.note = note
    lot.expiration_date = exp_dt
    lot.received_at = received_at

    if qty_initial is not None:
        lot.qty_initial = qty_initial
        lot.qty_available = qty_initial
    if unit_cost is not None:
        lot.unit_cost = unit_cost

    # Mantener consistencia: el movimiento de ingreso del lote define la fecha del ingreso.
    try:
        mv_update = {InventoryMovement.movement_date: movement_date}
        if qty_initial is not None:
            mv_update[InventoryMovement.qty_delta] = qty_initial
        if unit_cost is not None:
            mv_update[InventoryMovement.unit_cost] = unit_cost
        if qty_initial is not None or unit_cost is not None:
            q = qty_initial if qty_initial is not None else float(lot.qty_initial or 0)
            c = unit_cost if unit_cost is not None else float(lot.unit_cost or 0)
            mv_update[InventoryMovement.total_cost] = q * c

        # Un lote puede venir de compra (purchase) o de devolución (return).
        # Actualizamos el movimiento de ingreso correspondiente.
        inbound = (
            db.session.query(InventoryMovement)
            .filter(InventoryMovement.lot_id == lot.id)
            .filter(InventoryMovement.type.in_(['purchase', 'return']))
        )
        inbound.update(mv_update, synchronize_session=False)
    except Exception:
        current_app.logger.exception('Failed to update inbound movement for inventory lot')

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400

    return jsonify({'ok': True, 'item': _serialize_lot(lot)})


@bp.post('/api/lots/<int:lot_id>/adjust')
@login_required
@module_required('inventory')
def adjust_lot(lot_id: int):
    from flask_login import current_user

    try:
        if getattr(current_user, 'can', None):
            allowed = True
            try:
                allowed = bool(current_user.can('inventario.ajuste_lote'))
            except Exception:
                allowed = True
            if not allowed:
                return jsonify({'ok': False, 'error': 'forbidden'}), 403
    except Exception:
        pass

    lot = db.session.get(InventoryLot, int(lot_id))
    if not lot:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    cid = _company_id()
    if cid and str(getattr(lot, 'company_id', '') or '') != cid:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    payload = request.get_json(silent=True) or {}
    date_adj = _parse_date_iso(payload.get('date') or payload.get('fecha'), dt_date.today())
    note_user = str(payload.get('note') or payload.get('nota') or '').strip()

    qty_initial_raw = payload.get('qty_initial')
    unit_cost_raw = payload.get('unit_cost')
    if qty_initial_raw is None and unit_cost_raw is None:
        return jsonify({'ok': False, 'error': 'qty_or_cost_required'}), 400

    old_qty_initial = float(getattr(lot, 'qty_initial', 0.0) or 0.0)
    old_unit_cost = float(getattr(lot, 'unit_cost', 0.0) or 0.0)

    new_qty_initial = old_qty_initial
    if qty_initial_raw is not None:
        new_qty_initial = _num(qty_initial_raw)
        if new_qty_initial <= 0:
            return jsonify({'ok': False, 'error': 'qty_required'}), 400

    new_unit_cost = old_unit_cost
    if unit_cost_raw is not None:
        new_unit_cost = _num(unit_cost_raw)
        if new_unit_cost < 0:
            return jsonify({'ok': False, 'error': 'unit_cost_invalid'}), 400

    sold_rows = (
        db.session.query(
            InventoryMovement.sale_ticket,
            InventoryMovement.movement_date,
            func.coalesce(func.sum(-InventoryMovement.qty_delta), 0.0),
        )
        .filter(InventoryMovement.company_id == cid)
        .filter(InventoryMovement.lot_id == lot.id)
        .filter(InventoryMovement.type == 'sale')
        .group_by(InventoryMovement.sale_ticket, InventoryMovement.movement_date)
        .all()
    )
    qty_sold_total = sum(float(qty or 0.0) for (_, __, qty) in (sold_rows or []))

    if float(new_qty_initial or 0.0) + 1e-9 < float(qty_sold_total or 0.0):
        return jsonify({'ok': False, 'error': 'qty_below_consumed', 'consumed_qty': round(qty_sold_total, 6)}), 400

    adjustment_id = uuid4().hex

    value_old = float(old_qty_initial or 0.0) * float(old_unit_cost or 0.0)
    value_new = float(new_qty_initial or 0.0) * float(new_unit_cost or 0.0)
    difference_valor = float(value_new or 0.0) - float(value_old or 0.0)
    amount_abs = abs(float(difference_valor or 0.0))

    product_name = str(getattr(getattr(lot, 'product', None), 'name', '') or '').strip() or 'Producto'
    lot_label = str(getattr(lot, 'lot_code', '') or '').strip() or str(lot.id)

    base_note_lines = [
        f"Ajuste de inventario del producto {product_name} – Lote {lot_label}.",
        f"AdjustmentId: {adjustment_id}",
        f"Diferencia_valor: {round(difference_valor, 4)} (nuevo {round(value_new, 4)} - anterior {round(value_old, 4)})",
        "Este registro NO corresponde a una operación comercial, sino a una corrección interna.",
    ]
    if note_user:
        base_note_lines.append(f"Nota: {note_user}")

    try:
        lot.qty_initial = float(new_qty_initial or 0.0)
        lot.unit_cost = float(new_unit_cost or 0.0)
        lot.qty_available = max(0.0, float(new_qty_initial or 0.0) - float(qty_sold_total or 0.0))

        qty_delta_adj = float(new_qty_initial or 0.0) - float(old_qty_initial or 0.0)
        db.session.add(InventoryMovement(
            company_id=cid,
            movement_date=date_adj,
            type='lot_adjust',
            sale_ticket=None,
            product_id=lot.product_id,
            lot_id=lot.id,
            qty_delta=qty_delta_adj,
            unit_cost=float(new_unit_cost or 0.0),
            total_cost=qty_delta_adj * float(new_unit_cost or 0.0),
        ))

        delta_unit_cost = float(new_unit_cost or 0.0) - float(old_unit_cost or 0.0)
        if abs(delta_unit_cost) > 1e-9 and sold_rows:
            for t, d, qty in (sold_rows or []):
                ticket = str(t or '').strip()
                if not ticket:
                    continue
                sold_qty = float(qty or 0.0)
                if abs(sold_qty) <= 1e-9:
                    continue
                db.session.add(InventoryMovement(
                    company_id=cid,
                    movement_date=d or date_adj,
                    type='sale_adjust',
                    sale_ticket=ticket,
                    product_id=lot.product_id,
                    lot_id=lot.id,
                    qty_delta=0.0,
                    unit_cost=delta_unit_cost,
                    total_cost=sold_qty * delta_unit_cost,
                ))

        created = {
            'adjustment_id': adjustment_id,
            'sales': [],
            'expenses': [],
            'difference_valor': round(difference_valor, 4),
        }

        if amount_abs > 1e-9:
            if difference_valor > 0:
                op_ticket = f"AJOP-{adjustment_id[:10]}"
                mv_ticket = f"AJMV-{adjustment_id[:10]}"
                op_notes = '\n'.join(base_note_lines + [
                    'Layer: operativa',
                    'Tipo: Venta por ajuste de costo de mercadería',
                ])
                mv_notes = '\n'.join(base_note_lines + [
                    'Layer: contable',
                    'Tipo: Ingreso por ajuste de inventario',
                    'Categoría: Inventario',
                ])

                db.session.add(Sale(
                    company_id=cid,
                    ticket=op_ticket,
                    sale_date=date_adj,
                    sale_type='AjusteInvCosto',
                    status='Completada',
                    payment_method='Ajuste interno',
                    notes=op_notes,
                    total=amount_abs,
                    discount_general_pct=0.0,
                    discount_general_amount=0.0,
                    on_account=False,
                    paid_amount=amount_abs,
                    due_amount=0.0,
                    customer_id=None,
                    customer_name='Sistema / Ajuste interno',
                ))
                db.session.add(Sale(
                    company_id=cid,
                    ticket=mv_ticket,
                    sale_date=date_adj,
                    sale_type='IngresoAjusteInv',
                    status='Completada',
                    payment_method='Ajuste interno',
                    notes=mv_notes,
                    total=amount_abs,
                    discount_general_pct=0.0,
                    discount_general_amount=0.0,
                    on_account=False,
                    paid_amount=amount_abs,
                    due_amount=0.0,
                    customer_id=None,
                    customer_name='Sistema / Ajuste interno',
                ))
                created['sales'] = [op_ticket, mv_ticket]
            elif difference_valor < 0:
                supplier_id = str(getattr(lot, 'supplier_id', '') or '').strip() or None
                supplier_name = str(getattr(lot, 'supplier_name', '') or '').strip() or None
                base_meta = {
                    'origin_ref': {
                        'kind': 'lot_adjustment',
                        'adjustment_id': adjustment_id,
                        'product_id': lot.product_id,
                        'lot_id': lot.id,
                    }
                }

                op_eid = uuid4().hex
                mv_eid = uuid4().hex

                op_meta = json.loads(json.dumps(base_meta, ensure_ascii=False))
                op_meta['origin_ref']['layer'] = 'operational'
                op_meta['origin_ref']['linked_expense_id'] = mv_eid

                mv_meta = json.loads(json.dumps(base_meta, ensure_ascii=False))
                mv_meta['origin_ref']['layer'] = 'movement'
                mv_meta['origin_ref']['linked_expense_id'] = op_eid

                op_notes = '\n'.join(base_note_lines + [
                    'Layer: operativa',
                    'Tipo: Gasto por ajuste de inventario',
                    'Categoría: Inventario',
                ])
                mv_notes = '\n'.join(base_note_lines + [
                    'Layer: contable',
                    'Tipo: Gasto por ajuste de inventario',
                    'Categoría: Inventario',
                ])

                db.session.add(Expense(
                    id=op_eid,
                    company_id=cid,
                    expense_date=date_adj,
                    payment_method='Ajuste interno',
                    amount=amount_abs,
                    category='Inventario',
                    supplier_id=supplier_id,
                    supplier_name=supplier_name,
                    note=op_notes,
                    expense_type='AjusteInventario',
                    origin='inventory',
                    meta_json=json.dumps(op_meta, ensure_ascii=False),
                ))
                db.session.add(Expense(
                    id=mv_eid,
                    company_id=cid,
                    expense_date=date_adj,
                    payment_method='Ajuste interno',
                    amount=amount_abs,
                    category='Inventario',
                    supplier_id=supplier_id,
                    supplier_name=supplier_name,
                    note=mv_notes,
                    expense_type='AjusteInventario',
                    origin='inventory',
                    meta_json=json.dumps(mv_meta, ensure_ascii=False),
                ))
                created['expenses'] = [op_eid, mv_eid]

        db.session.commit()
        return jsonify({
            'ok': True,
            'item': _serialize_lot(lot),
            'created': created,
        })
    except Exception:
        db.session.rollback()
        current_app.logger.exception('Failed to adjust inventory lot')
        return jsonify({'ok': False, 'error': 'db_error'}), 400


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

    # Borrado consistente:
    # - movimientos de inventario asociados (purchase/return/etc)
    # - egresos creados desde inventario que referencien este lote (meta_json.origin_ref.lot_id)
    try:
        cid = str(getattr(lot, 'company_id', '') or '').strip()

        # borrar egresos vinculados al lote
        # Nota: Expense.meta_json guarda origin_ref (dict) y puede tener lot_id como número o string.
        patterns = [
            f'%"lot_id":{lot.id}%',
            f'%"lot_id": {lot.id}%',
            f'%"lot_id":"{lot.id}"%',
            f'%"lot_id": "{lot.id}"%',
        ]
        try:
            conds = []
            for p in patterns:
                conds.append(Expense.meta_json.ilike(p))
            if cid and conds:
                (
                    db.session.query(Expense)
                    .filter(Expense.company_id == cid)
                    .filter(Expense.origin == 'inventory')
                    .filter(or_(*conds))
                    .delete(synchronize_session=False)
                )
        except Exception:
            current_app.logger.exception('Failed to delete inventory-origin expenses for lot')

        # borrar movimientos asociados (compras/ajustes/return) y el lote
        db.session.query(InventoryMovement).filter(InventoryMovement.lot_id == lot.id).delete(synchronize_session=False)
        db.session.delete(lot)
        db.session.commit()
    except Exception:
        db.session.rollback()
        current_app.logger.exception('Failed to delete inventory lot')
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True})


@bp.get('/api/movements')
@login_required
@module_required('inventory')
def list_inventory_movements():
    raw_from = (request.args.get('from') or '').strip()
    raw_to = (request.args.get('to') or '').strip()
    product_id = (request.args.get('product_id') or '').strip()
    limit = int(request.args.get('limit') or 2000)
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
