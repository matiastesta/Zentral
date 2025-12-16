from flask import render_template
from app.movements import bp


@bp.route('/')
@bp.route('/index')
def index():
    """Historial de movimientos (dummy)."""
    sales_history = []
    expenses_history = []
    return render_template('movements/index.html', title='Movimientos', sales_history=sales_history, expenses_history=expenses_history)
