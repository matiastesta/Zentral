from flask import render_template
from flask_login import login_required

from app.permissions import module_required
from app.movements import bp


@bp.route('/')
@bp.route('/index')
@login_required
@module_required('movements')
def index():
    """Historial de movimientos (dummy)."""
    sales_history = []
    expenses_history = []
    return render_template('movements/index.html', title='Movimientos', sales_history=sales_history, expenses_history=expenses_history)
