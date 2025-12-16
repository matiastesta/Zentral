from flask import render_template
from app.customers import bp


@bp.route("/")
@bp.route("/index")
def index():
    """Listado básico de clientes (dummy)."""
    customers = []
    return render_template("customers/list.html", title="Clientes", customers=customers)
