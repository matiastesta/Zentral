from flask import render_template
from flask_login import login_required

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
