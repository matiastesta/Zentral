from datetime import date

from flask import render_template
from flask_login import login_required

from app.permissions import module_required
from app.employees import bp


@bp.route('/')
@bp.route('/index')
@login_required
@module_required('employees')
def index():
    return render_template('employees/index.html', title='Empleados')


@bp.route('/new')
@login_required
@module_required('employees')
def new():
    return render_template('employees/new.html', title='Nuevo empleado', today=date.today().isoformat())
