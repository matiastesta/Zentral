from flask import render_template
from app.inventory import bp


@bp.route('/')
@bp.route('/index')
def index():
    """Inventario avanzado (dummy)."""
    items = []
    return render_template('inventory/index.html', title='Inventario', items=items)
