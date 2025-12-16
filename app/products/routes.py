from flask import render_template
from app.products import bp


@bp.route("/")
@bp.route("/index")
def index():
    """Listado básico de productos (dummy)."""
    products = []
    return render_template("products/list.html", title="Productos", products=products)
