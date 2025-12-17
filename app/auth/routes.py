from flask import render_template, redirect, url_for, flash, request
from flask_login import login_user, logout_user, current_user
from app.auth.forms import LoginForm, RegistrationForm
from app.auth import bp
from app import db
from app.models import User


@bp.route('/login', methods=['GET', 'POST'])
def login():
    """Vista de login temporal sin base de datos real.

    Por ahora solo muestra el formulario y, si se envía, redirige al índice
    sin validar credenciales ni crear sesión persistente.
    """
    if current_user.is_authenticated:
        return redirect(url_for('main.index'))
    form = LoginForm()
    if form.validate_on_submit():
        username = (form.username.data or '').strip()
        password = form.password.data or ''
        user = db.session.query(User).filter_by(username=username).first()
        if not user or not user.check_password(password):
            flash('Usuario o contraseña inválidos.', 'error')
            return render_template('auth/login.html', title='Iniciar Sesión', form=form)
        if not user.active:
            flash('Usuario inactivo.', 'error')
            return render_template('auth/login.html', title='Iniciar Sesión', form=form)
        login_user(user, remember=getattr(form, 'remember_me', None) and form.remember_me.data)
        return redirect(url_for('main.index'))
    return render_template('auth/login.html', title='Iniciar Sesión', form=form)

@bp.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('auth.login'))

@bp.route('/register', methods=['GET', 'POST'])
def register():
    """Vista de registro temporal sin escritura en base de datos.

    Acepta el formulario y muestra un mensaje, pero no crea usuarios reales.
    """
    if current_user.is_authenticated:
        return redirect(url_for('main.index'))
    flash('Registro deshabilitado. Solicitá al administrador que cree tu usuario.', 'info')
    return redirect(url_for('auth.login'))
