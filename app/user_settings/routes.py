from flask import render_template
from app.user_settings import bp


@bp.route('/')
@bp.route('/index')
def index():
    """Configuración de usuario y permisos (dummy)."""
    users = []
    roles = ['admin', 'vendedor', 'contador']
    return render_template('user_settings/index.html', title='Configuración de Usuario', users=users, roles=roles)
