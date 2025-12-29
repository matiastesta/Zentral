from flask import render_template, redirect, url_for, flash, request, session, current_app
from flask_login import login_user, logout_user, current_user
from sqlalchemy import func
from app.auth.forms import LoginForm, RegistrationForm
from app.auth import bp
from app import db
from app.db_context import apply_rls_context
from app.models import Company, User


@bp.route('/login', methods=['GET', 'POST'])
def login():
    """Vista de login temporal sin base de datos real.

    Por ahora solo muestra el formulario y, si se envía, redirige al índice
    sin validar credenciales ni crear sesión persistente.
    """
    if current_user.is_authenticated and request.method == 'GET':
        return redirect(url_for('main.index'))
    form = LoginForm()
    if current_user.is_authenticated and request.method == 'POST':
        try:
            logout_user()
        except Exception:
            pass
        try:
            session.pop('auth_is_zentral_admin', None)
            session.pop('auth_company_id', None)
            session.pop('impersonate_company_id', None)
        except Exception:
            pass
    if form.validate_on_submit():
        ident = (form.login.data or '').strip()
        ident_norm = ident.strip().lower()
        password = form.password.data or ''

        # During login we need broader lookup (email OR username). We pass the identifier
        # through app.login_email so Postgres RLS can whitelist the lookup.
        try:
            apply_rls_context(is_login=True, login_email=ident_norm)

            q = db.session.query(User)
            if '@' in ident_norm:
                user = q.filter(func.lower(User.email) == ident_norm).first()
            else:
                user = q.filter(func.lower(User.username) == ident_norm).first()
        except Exception:
            try:
                db.session.rollback()
            except Exception:
                pass
            try:
                current_app.logger.exception('Login failed')
            except Exception:
                pass
            flash('No se pudo iniciar sesión. Intentá nuevamente o contactá soporte.', 'error')
            return render_template('auth/login.html', title='Iniciar Sesión', form=form)
        if not user or not user.check_password(password):
            flash('Usuario o contraseña inválidos.', 'error')
            return render_template('auth/login.html', title='Iniciar Sesión', form=form)
        if not user.active:
            flash('Usuario inactivo.', 'error')
            return render_template('auth/login.html', title='Iniciar Sesión', form=form)

        if str(getattr(user, 'role', '') or '') != 'zentral_admin':
            cid = str(getattr(user, 'company_id', '') or '').strip()
            if not cid:
                flash('Usuario sin empresa asignada.', 'error')
                return render_template('auth/login.html', title='Iniciar Sesión', form=form)
            c = db.session.get(Company, cid)
            if not c:
                flash('Empresa inválida.', 'error')
                return render_template('auth/login.html', title='Iniciar Sesión', form=form)
            if str(getattr(c, 'status', '') or 'active') != 'active':
                flash('Empresa pausada. Contactá soporte.', 'error')
                return render_template('auth/login.html', title='Iniciar Sesión', form=form)

        try:
            session.permanent = True
        except Exception:
            pass

        is_zentral_admin = str(getattr(user, 'role', '') or '') == 'zentral_admin'
        remember_flag = bool(is_zentral_admin or (getattr(form, 'remember_me', None) and form.remember_me.data))
        login_user(user, remember=remember_flag)

        session.pop('impersonate_company_id', None)
        if is_zentral_admin:
            session['auth_is_zentral_admin'] = '1'
            session.pop('auth_company_id', None)
        else:
            session['auth_is_zentral_admin'] = '0'
            session['auth_company_id'] = str(getattr(user, 'company_id', '') or '').strip()

        return redirect(url_for('main.index'))
    return render_template('auth/login.html', title='Iniciar Sesión', form=form)

@bp.route('/logout')
def logout():
    logout_user()
    session.pop('auth_is_zentral_admin', None)
    session.pop('auth_company_id', None)
    session.pop('impersonate_company_id', None)
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
