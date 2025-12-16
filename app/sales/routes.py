from flask import render_template
from app.sales import bp


@bp.route("/")
@bp.route("/index")
def index():
    """Listado básico de ventas (dummy)."""
    sales = []
    return render_template("sales/list.html", title="Ventas", sales=sales)
