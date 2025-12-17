from flask import render_template
from flask_login import login_required

from app.permissions import module_required
from app.inventory import bp


@bp.route('/')
@bp.route('/index')
@login_required
@module_required('inventory')
def index():
    """Inventario avanzado (dummy)."""
    items = []
    return render_template('inventory/index.html', title='Inventario', items=items)
