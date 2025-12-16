from datetime import date

from flask import render_template
from app.expenses import bp


@bp.route("/")
@bp.route("/index")
def index():
    """Listado básico de gastos (dummy)."""
    expenses = []
    return render_template("expenses/list.html", title="Gastos", expenses=expenses)


@bp.route("/new")
def new():
    return render_template("expenses/new.html", title="Nuevo gasto", today=date.today().isoformat())
