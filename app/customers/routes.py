from flask import render_template
from flask_login import login_required

from app.permissions import module_required
from app.customers import bp


@bp.route("/")
@bp.route("/index")
@login_required
@module_required('customers')
def index():
    """Listado básico de clientes (dummy)."""
    customers = []
    return render_template("customers/list.html", title="Clientes", customers=customers)
