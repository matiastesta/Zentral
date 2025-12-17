from flask import render_template
from flask_login import login_required

from app.permissions import module_required
from app.suppliers import bp


@bp.route('/')
@bp.route('/index')
@login_required
@module_required('suppliers')
def index():
    """Gestor de proveedores (dummy)."""
    suppliers = []
    return render_template('suppliers/index.html', title='Proveedores', suppliers=suppliers)


@bp.route('/new')
@login_required
@module_required('suppliers')
def new():
    return render_template('suppliers/new.html', title='Nuevo proveedor')
