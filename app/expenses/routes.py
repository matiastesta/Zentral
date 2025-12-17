from datetime import date

from flask import render_template
from flask_login import login_required

from app.permissions import module_required
from app.expenses import bp


@bp.route("/")
@bp.route("/index")
@login_required
@module_required('expenses')
def index():
    """Listado básico de gastos (dummy)."""
    expenses = []
    return render_template("expenses/list.html", title="Gastos", expenses=expenses)


@bp.route("/new")
@login_required
@module_required('expenses')
def new():
    return render_template("expenses/new.html", title="Nuevo gasto", today=date.today().isoformat())
