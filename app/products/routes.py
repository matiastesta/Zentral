from flask import render_template
from flask_login import login_required

from app.permissions import module_required
from app.products import bp


@bp.route("/")
@bp.route("/index")
@login_required
@module_required('inventory')
def index():
    """Listado básico de productos (dummy)."""
    products = []
    return render_template("products/list.html", title="Productos", products=products)
