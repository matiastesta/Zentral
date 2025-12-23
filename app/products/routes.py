from flask import render_template
from flask_login import login_required

from sqlalchemy import func

from app import db
from app.models import InventoryLot, Product
from app.permissions import module_required
from app.products import bp


@bp.route("/")
@bp.route("/index")
@login_required
@module_required('inventory')
def index():
    """Listado básico de productos (dummy)."""
    rows = (
        db.session.query(Product)
        .order_by(Product.updated_at.desc(), Product.id.desc())
        .limit(5000)
        .all()
    )

    stock_rows = (
        db.session.query(InventoryLot.product_id, func.sum(InventoryLot.qty_available))
        .group_by(InventoryLot.product_id)
        .all()
    )
    stock_map = {int(pid): float(qty or 0.0) for (pid, qty) in (stock_rows or []) if pid is not None}

    products = []
    for p in rows:
        sku = str(getattr(p, 'internal_code', '') or '').strip() or str(getattr(p, 'barcode', '') or '').strip()
        price = float(getattr(p, 'sale_price', 0.0) or 0.0)
        stock = float(stock_map.get(int(p.id), 0.0))
        products.append({
            'id': p.id,
            'name': p.name or '',
            'sku': sku,
            'price': price,
            'stock': stock,
            'active': bool(getattr(p, 'active', True)),
        })

    return render_template("products/list.html", title="Productos", products=products)
