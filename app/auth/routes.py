from flask import render_template, redirect, url_for, flash, request
from flask_login import login_user, logout_user, current_user
from app.auth.forms import LoginForm, RegistrationForm
from app.auth import bp


@bp.route('/login', methods=['GET', 'POST'])
def login():
    """Vista de login temporal sin base de datos real.

    Por ahora solo muestra el formulario y, si se envía, redirige al índice
    sin validar credenciales ni crear sesión persistente.
    """
    if current_user.is_authenticated:
        return redirect(url_for('main.index'))
    form = LoginForm()
    # Mientras no haya autenticación real, cualquier envío del formulario
    # redirige al dashboard, sin validar credenciales.
    if request.method == 'POST':
        flash('Autenticación simulada (sin base de datos).', 'info')
        return redirect(url_for('main.index'))
    return render_template('auth/login.html', title='Iniciar Sesión', form=form)

@bp.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('main.index'))

@bp.route('/register', methods=['GET', 'POST'])
def register():
    """Vista de registro temporal sin escritura en base de datos.

    Acepta el formulario y muestra un mensaje, pero no crea usuarios reales.
    """
    if current_user.is_authenticated:
        return redirect(url_for('main.index'))
    form = RegistrationForm()
    if form.validate_on_submit():
        flash('Registro simulado (sin guardar usuario en base de datos).', 'info')
        return redirect(url_for('auth.login'))
    return render_template('auth/register.html', title='Registrarse', form=form)
