from datetime import date as dt_date, datetime
from typing import Any, Dict, List

from flask import jsonify, render_template, request
from flask_login import login_required

from app import db
from app.models import CashCount, Category, InventoryLot, InventoryMovement, Product, Sale, SaleItem
from app.permissions import module_required
from app.sales import bp


@bp.route("/")
@bp.route("/index")
@login_required
@module_required('sales')
def index():
    """Listado básico de ventas (dummy)."""
    sales = []
    return render_template("sales/list.html", title="Ventas", sales=sales)


def _dt_to_ms(dt):
    if not dt:
        return 0
    try:
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def _parse_date_iso(raw, fallback=None):
    try:
        return dt_date.fromisoformat(str(raw).strip())
    except Exception:
        return fallback


def _serialize_sale(row: Sale):
    return {
        'id': row.id,
        'ticket': row.ticket,
        'fecha': row.sale_date.isoformat() if row.sale_date else '',
        'type': row.sale_type,
        'status': row.status,
        'payment_method': row.payment_method,
        'notes': row.notes or '',
        'total': row.total,
        'discount_general_pct': row.discount_general_pct,
        'discount_general_amount': row.discount_general_amount,
        'customer_id': row.customer_id or '',
        'customer_name': row.customer_name or '',
        'on_account': bool(row.on_account),
        'paid_amount': row.paid_amount,
        'due_amount': row.due_amount,
        'exchange_return_total': row.exchange_return_total,
        'exchange_new_total': row.exchange_new_total,
        'created_at': _dt_to_ms(row.created_at),
        'updated_at': _dt_to_ms(row.updated_at),
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


def _serialize_product_for_sales(p: Product):
    cat = None
    try:
        if getattr(p, 'category', None):
            cat = {'id': p.category.id, 'name': p.category.name, 'parent_id': p.category.parent_id}
    except Exception:
        cat = None
    return {
        'id': p.id,
        'name': p.name,
        'sale_price': p.sale_price,
        'category_id': p.category_id,
        'category': cat,
        'category_name': (cat.get('name') if isinstance(cat, dict) else ''),
        'active': bool(p.active),
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
@module_required('sales')
def list_sales():
    raw_from = (request.args.get('from') or '').strip()
    raw_to = (request.args.get('to') or '').strip()
    include_replaced = str(request.args.get('include_replaced') or '').strip() in ('1', 'true', 'True')
    limit = int(request.args.get('limit') or 300)
    if limit <= 0 or limit > 2000:
        limit = 300

    d_from = _parse_date_iso(raw_from, None)
    d_to = _parse_date_iso(raw_to, None)

    q = db.session.query(Sale)
    if d_from:
        q = q.filter(Sale.sale_date >= d_from)
    if d_to:
        q = q.filter(Sale.sale_date <= d_to)
    if not include_replaced:
        q = q.filter(Sale.status != 'Reemplazada')
    q = q.order_by(Sale.sale_date.desc(), Sale.id.desc()).limit(limit)
    rows = q.all()
    return jsonify({'ok': True, 'items': [_serialize_sale(r) for r in rows]})


@bp.get('/api/sales/<ticket>')
@login_required
@module_required('sales')
def get_sale(ticket):
    t = str(ticket or '').strip()
    row = db.session.query(Sale).filter(Sale.ticket == t).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    return jsonify({'ok': True, 'item': _serialize_sale(row)})


@bp.get('/api/products')
@login_required
@module_required('sales')
def list_products_for_sales():
    limit = int(request.args.get('limit') or 5000)
    if limit <= 0 or limit > 10000:
        limit = 5000
    q = db.session.query(Product).filter(Product.active == True)  # noqa: E712
    q = q.order_by(Product.name.asc()).limit(limit)
    rows = q.all()
    return jsonify({'ok': True, 'items': [_serialize_product_for_sales(r) for r in rows]})


@bp.get('/api/lots')
@login_required
@module_required('sales')
def list_lots_for_sales():
    limit = int(request.args.get('limit') or 10000)
    if limit <= 0 or limit > 20000:
        limit = 10000
    product_id = (request.args.get('product_id') or '').strip()
    q = db.session.query(InventoryLot).filter(InventoryLot.qty_available > 0)
    if product_id:
        try:
            q = q.filter(InventoryLot.product_id == int(product_id))
        except Exception:
            return jsonify({'ok': True, 'items': []})
    q = q.order_by(InventoryLot.received_at.desc(), InventoryLot.id.desc()).limit(limit)
    rows = q.all()
    return jsonify({'ok': True, 'items': [_serialize_lot_for_sales(r) for r in rows]})


@bp.post('/api/sales/settle')
@login_required
@module_required('sales')
def settle_sale_due_amount():
    payload = request.get_json(silent=True) or {}
    sale_id = payload.get('sale_id')
    ticket = str(payload.get('ticket') or '').strip()
    payment_method = str(payload.get('payment_method') or 'Efectivo').strip() or 'Efectivo'

    row = None
    if sale_id is not None and str(sale_id).strip() != '':
        try:
            row = db.session.get(Sale, int(sale_id))
        except Exception:
            row = None
    if not row and ticket:
        row = db.session.query(Sale).filter(Sale.ticket == ticket).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    due = float(row.due_amount or 0.0)
    if due <= 0:
        return jsonify({'ok': False, 'error': 'no_due'}), 400

    settle_date = dt_date.today()
    payment_ticket = _next_ticket()
    ref = str(row.ticket or '').strip()
    note = f"Cobro cuenta corriente (Ticket {ref})"

    payment_sale = Sale(
        ticket=payment_ticket,
        sale_date=settle_date,
        sale_type='CobroCC',
        status='Completada',
        payment_method=payment_method,
        notes=note,
        total=abs(due),
        discount_general_pct=0.0,
        discount_general_amount=0.0,
        on_account=False,
        paid_amount=abs(due),
        due_amount=0.0,
        customer_id=row.customer_id,
        customer_name=row.customer_name,
        exchange_return_total=None,
        exchange_new_total=None,
    )
    try:
        from flask_login import current_user
        payment_sale.created_by_user_id = int(getattr(current_user, 'id', 0) or 0) or None
    except Exception:
        payment_sale.created_by_user_id = None

    row.paid_amount = float(row.paid_amount or 0.0) + abs(due)
    row.due_amount = 0.0
    row.on_account = False
    extra = f"CC saldada por {payment_ticket} ({payment_method})"
    prev = str(row.notes or '').strip()
    row.notes = (prev + ('\n' if prev else '') + extra) if extra else (prev or None)

    db.session.add(payment_sale)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400

    return jsonify({'ok': True, 'item': _serialize_sale(row), 'payment': _serialize_sale(payment_sale)})


@bp.post('/api/exchanges')
@login_required
@module_required('sales')
def create_exchange():
    payload = request.get_json(silent=True) or {}
    sale_date = _parse_date_iso(payload.get('fecha') or payload.get('date'), dt_date.today())
    payment_method = str(payload.get('payment_method') or 'Efectivo').strip() or 'Efectivo'

    original_ticket = str(payload.get('original_ticket') or '').strip()
    notes = str(payload.get('notes') or '').strip() or None

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

    # Reservar 2 tickets consecutivos (Devolución + Venta)
    last_id = db.session.query(db.func.max(Sale.id)).scalar() or 0
    return_ticket = '#' + str(int(last_id) + 1).zfill(4)
    sale_ticket = '#' + str(int(last_id) + 2).zfill(4)

    # En una transacción: revertir ticket original (si existe), crear devolución + nueva venta
    try:
        if original_ticket:
            # revertimos inventario del ticket anterior para eliminar su impacto
            _revert_inventory_for_ticket(original_ticket)

        return_row = Sale(
            ticket=return_ticket,
            sale_date=sale_date,
            sale_type='Devolucion',
            status='Completada',
            payment_method=payment_method,
            notes=notes,
            total=-abs(return_total),
            discount_general_pct=0.0,
            discount_general_amount=0.0,
            on_account=False,
            paid_amount=0.0,
            due_amount=0.0,
            customer_id=customer_id,
            customer_name=customer_name,
            exchange_return_total=return_total,
            exchange_new_total=new_total,
        )
        sale_row = Sale(
            ticket=sale_ticket,
            sale_date=sale_date,
            sale_type='Venta',
            status='Completada',
            payment_method=payment_method,
            notes=notes,
            total=abs(new_total),
            discount_general_pct=0.0,
            discount_general_amount=0.0,
            on_account=False,
            paid_amount=0.0,
            due_amount=0.0,
            customer_id=customer_id,
            customer_name=customer_name,
            exchange_return_total=return_total,
            exchange_new_total=new_total,
        )
        db.session.add(return_row)
        db.session.add(sale_row)
        db.session.flush()

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

        # aplicar inventario/movimientos usando la misma lógica
        _apply_inventory_for_sale(sale_ticket=return_ticket, sale_date=sale_date, items=return_items_inv)
        _apply_inventory_for_sale(sale_ticket=sale_ticket, sale_date=sale_date, items=new_items_inv)

        if original_ticket:
            _mark_sale_replaced(ticket=original_ticket, replaced_by=return_ticket)

        db.session.commit()
    except ValueError as e:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'stock_insufficient', 'message': str(e)}), 400
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400

    return jsonify({
        'ok': True,
        'original_ticket': original_ticket,
        'return_ticket': return_ticket,
        'new_ticket': sale_ticket,
        'items': {
            'return': _serialize_sale(return_row),
            'sale': _serialize_sale(sale_row),
        }
    })


def _next_ticket():
    last_id = db.session.query(db.func.max(Sale.id)).scalar() or 0
    n = int(last_id) + 1
    return '#' + str(n).zfill(4)


def _num(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def _int_or_none(v):
    try:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        return int(s)
    except Exception:
        return None


def _apply_inventory_for_sale(*, sale_ticket: str, sale_date: dt_date, items: List[Dict[str, Any]]):
    """Aplica impacto de inventario según los items (direction out/in).

    - out: consume FIFO en InventoryLot (qty_available)
    - in: crea un lote nuevo (devolución) y suma stock
    """
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

        if direction == 'in':
            # Devolución: entra stock. Creamos lote propio para trazabilidad.
            last_cost = db.session.query(InventoryLot.unit_cost).filter(InventoryLot.product_id == pid).order_by(InventoryLot.received_at.desc(), InventoryLot.id.desc()).first()
            unit_cost = float(last_cost[0]) if last_cost and last_cost[0] is not None else 0.0
            lot = InventoryLot(
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

    movs = (
        db.session.query(InventoryMovement)
        .filter(InventoryMovement.sale_ticket == t)
        .order_by(InventoryMovement.id.asc())
        .with_for_update()
        .all()
    )
    for m in movs:
        if m.lot_id:
            lot = db.session.get(InventoryLot, int(m.lot_id))
            if lot:
                lot.qty_available = float(lot.qty_available or 0) - float(m.qty_delta or 0)
                # Si era lote creado por devolución de este ticket y queda vacío, lo eliminamos.
                if (lot.origin_sale_ticket or '') == t and float(lot.qty_available or 0) <= 1e-9:
                    db.session.delete(lot)
        db.session.delete(m)


def _mark_sale_replaced(*, ticket: str, replaced_by: str):
    t = str(ticket or '').strip()
    if not t:
        return
    row = db.session.query(Sale).filter(Sale.ticket == t).first()
    if not row:
        return
    row.status = 'Reemplazada'
    note = str(row.notes or '').strip()
    extra = f"Reemplazada por {replaced_by}" if replaced_by else 'Reemplazada'
    row.notes = (note + ('\n' if note else '') + extra) if extra else (note or None)


@bp.post('/api/sales')
@login_required
@module_required('sales')
def create_sale():
    payload = request.get_json(silent=True) or {}
    sale_date = _parse_date_iso(payload.get('fecha') or payload.get('date'), dt_date.today())
    sale_type = str(payload.get('type') or 'Venta').strip() or 'Venta'
    status = str(payload.get('status') or ('Cambio' if sale_type == 'Cambio' else 'Completada')).strip() or 'Completada'
    payment_method = str(payload.get('payment_method') or 'Efectivo').strip() or 'Efectivo'

    row = Sale(
        ticket=_next_ticket(),
        sale_date=sale_date,
        sale_type=sale_type,
        status=status,
        payment_method=payment_method,
        notes=str(payload.get('notes') or '').strip() or None,
        total=_num(payload.get('total')),
        discount_general_pct=_num(payload.get('discount_general_pct')),
        discount_general_amount=_num(payload.get('discount_general_amount')),
        on_account=bool(payload.get('on_account')),
        paid_amount=_num(payload.get('paid_amount')),
        due_amount=_num(payload.get('due_amount')),
        customer_id=str(payload.get('customer_id') or '').strip() or None,
        customer_name=str(payload.get('customer_name') or '').strip() or None,
        exchange_return_total=(None if payload.get('exchange_return_total') is None else _num(payload.get('exchange_return_total'))),
        exchange_new_total=(None if payload.get('exchange_new_total') is None else _num(payload.get('exchange_new_total'))),
    )
    try:
        from flask_login import current_user
        row.created_by_user_id = int(getattr(current_user, 'id', 0) or 0) or None
    except Exception:
        row.created_by_user_id = None

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

    db.session.add(row)
    try:
        db.session.flush()
        try:
            _apply_inventory_for_sale(sale_ticket=row.ticket, sale_date=sale_date, items=items_list)
        except ValueError as e:
            db.session.rollback()
            return jsonify({'ok': False, 'error': 'stock_insufficient', 'message': str(e)}), 400
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_sale(row)})


@bp.put('/api/sales/<ticket>')
@login_required
@module_required('sales')
def update_sale(ticket):
    t = str(ticket or '').strip()
    row = db.session.query(Sale).filter(Sale.ticket == t).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

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
    row.exchange_return_total = (None if payload.get('exchange_return_total') is None else _num(payload.get('exchange_return_total')))
    row.exchange_new_total = (None if payload.get('exchange_new_total') is None else _num(payload.get('exchange_new_total')))

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
        except ValueError as e:
            db.session.rollback()
            return jsonify({'ok': False, 'error': 'stock_insufficient', 'message': str(e)}), 400
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_sale(row)})


@bp.delete('/api/sales/<ticket>')
@login_required
@module_required('sales')
def delete_sale(ticket):
    t = str(ticket or '').strip()
    row = db.session.query(Sale).filter(Sale.ticket == t).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    try:
        _revert_inventory_for_ticket(t)
        db.session.delete(row)
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True})


@bp.get('/api/cash-count')
@login_required
@module_required('sales')
def get_cash_count():
    raw = (request.args.get('date') or '').strip()
    try:
        d = dt_date.fromisoformat(raw) if raw else dt_date.today()
    except Exception:
        d = dt_date.today()

    row = db.session.query(CashCount).filter(CashCount.count_date == d).first()
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
def upsert_cash_count():
    payload = request.get_json(silent=True) or {}
    raw = str(payload.get('date') or '').strip()
    try:
        d = dt_date.fromisoformat(raw) if raw else dt_date.today()
    except Exception:
        d = dt_date.today()

    def num(v):
        try:
            return float(v)
        except Exception:
            return 0.0

    opening = num(payload.get('opening_amount'))
    cash_day = num(payload.get('cash_day_amount'))
    closing = num(payload.get('closing_amount'))
    diff = (opening + cash_day) - closing

    employee_id = str(payload.get('employee_id') or '').strip() or None
    employee_name = str(payload.get('employee_name') or '').strip() or None

    row = db.session.query(CashCount).filter(CashCount.count_date == d).first()
    if not row:
        row = CashCount(count_date=d)
        db.session.add(row)

    row.employee_id = employee_id
    row.employee_name = employee_name
    row.opening_amount = opening
    row.cash_day_amount = cash_day
    row.closing_amount = closing
    row.difference_amount = diff
    try:
        from flask_login import current_user
        row.created_by_user_id = int(getattr(current_user, 'id', 0) or 0) or None
    except Exception:
        row.created_by_user_id = None

    db.session.commit()

    return jsonify({'ok': True, 'item': {'date': row.count_date.isoformat(), 'difference_amount': row.difference_amount}})
