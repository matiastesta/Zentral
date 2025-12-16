from datetime import date

from flask import render_template

from app.employees import bp


@bp.route('/')
@bp.route('/index')
def index():
    return render_template('employees/index.html', title='Empleados')


@bp.route('/new')
def new():
    return render_template('employees/new.html', title='Nuevo empleado', today=date.today().isoformat())
