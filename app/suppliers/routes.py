from flask import render_template
from app.suppliers import bp


@bp.route('/')
@bp.route('/index')
def index():
    """Gestor de proveedores (dummy)."""
    suppliers = []
    return render_template('suppliers/index.html', title='Proveedores', suppliers=suppliers)


@bp.route('/new')
def new():
    return render_template('suppliers/new.html', title='Nuevo proveedor')
