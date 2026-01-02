import os
import secrets
import string

from flask import g, render_template, request, redirect, url_for, flash, current_app
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from app import db
from app.files.storage import upload_to_r2_and_create_asset
from app.models import User, BusinessSettings, CompanyRole
from app.permissions import module_required
from app.user_settings import bp


MODULE_KEYS = [
    'dashboard',
    'calendar',
    'sales',
    'expenses',
    'inventory',
    'movements',
    'customers',
    'suppliers',
    'employees',
    'reports',
    'user_settings',
    'settings',
]


def _normalize_role_name(raw: str) -> str:
    s = str(raw or '').strip().lower()
    s = '_'.join([p for p in s.split() if p])
    cleaned = []
    for ch in s:
        if (ch.isalnum()) or ch in {'_', '-'}:
            cleaned.append(ch)
    out = ''.join(cleaned)
    while '__' in out:
        out = out.replace('__', '_')
    while '--' in out:
        out = out.replace('--', '-')
    return out.strip('_-')


def _ensure_company_role_exists(company_id: str, raw_role: str) -> str:
    cid = str(company_id or '').strip()
    role = _normalize_role_name(raw_role)
    if not cid or not role:
        return ''
    try:
        existing = (
            db.session.query(CompanyRole)
            .filter(CompanyRole.company_id == cid, CompanyRole.name == role)
            .first()
        )
        if existing:
            return role
        rr = CompanyRole(company_id=cid, name=role)
        rr.set_permissions({})
        db.session.add(rr)
        db.session.flush()
        return role
    except Exception:
        return ''


@bp.route('/', methods=['GET', 'POST'])
@bp.route('/index', methods=['GET', 'POST'])
@login_required
@module_required('user_settings')
def index():
    """Configuración de usuario y permisos (dummy)."""
    if request.method == 'POST':
        action = (request.form.get('action') or '').strip()

        can_manage_users = bool(
            getattr(current_user, 'is_master', False)
            or (getattr(current_user, 'role', '') in {'admin', 'company_admin', 'zentral_admin'})
        )
        if action in {'create_user', 'update_user', 'delete_user', 'reset_password'} and not can_manage_users:
            flash('No tenés permisos para administrar usuarios.', 'error')
            return redirect(url_for('user_settings.index'))

        if action == 'reset_password':
            uid = request.form.get('user_id')
            u = db.session.get(User, int(uid)) if uid and str(uid).isdigit() else None
            if not u:
                flash('Usuario inválido.', 'error')
                return redirect(url_for('user_settings.index'))
            if getattr(current_user, 'role', '') == 'zentral_admin':
                if str(getattr(g, 'company_id', '') or ''):
                    if str(getattr(u, 'company_id', '') or '') != str(getattr(g, 'company_id', '') or ''):
                        flash('Usuario inválido.', 'error')
                        return redirect(url_for('user_settings.index'))
            else:
                if str(getattr(u, 'company_id', '') or '') != str(getattr(g, 'company_id', '') or ''):
                    flash('Usuario inválido.', 'error')
                    return redirect(url_for('user_settings.index'))
            if u.is_master:
                flash('El usuario master no es editable.', 'error')
                return redirect(url_for('user_settings.index'))

            requested = (request.form.get('new_password') or '').strip()
            if requested:
                new_pass = requested
            else:
                alphabet = string.ascii_letters + string.digits
                new_pass = ''.join(secrets.choice(alphabet) for _ in range(10))

            u.set_password(new_pass)
            db.session.commit()
            flash(f'Contraseña actualizada para {u.username}. Nueva contraseña: {new_pass}', 'success')
            return redirect(url_for('user_settings.index'))

        if action == 'save_business':
            bs = BusinessSettings.get_for_company(g.company_id)
            bs.name = (request.form.get('business_name') or '').strip() or bs.name
            bs.industry = (request.form.get('business_industry') or '').strip() or None
            bs.email = (request.form.get('business_email') or '').strip() or None
            bs.phone = (request.form.get('business_phone') or '').strip() or None
            bs.address = (request.form.get('business_address') or '').strip() or None

            f = request.files.get('business_logo')
            if f and getattr(f, 'filename', ''):
                filename = secure_filename(f.filename)
                _, ext = os.path.splitext(filename.lower())
                allowed = set((current_app.config.get('ALLOWED_EXTENSIONS') or set()))
                if allowed and ext.lstrip('.') not in allowed:
                    flash('Formato de logo no permitido.', 'error')
                    return redirect(url_for('user_settings.index'))

                try:
                    asset = upload_to_r2_and_create_asset(
                        company_id=str(getattr(g, 'company_id', '') or '').strip(),
                        file_storage=f,
                        entity_type='business_logo',
                        entity_id=str(getattr(g, 'company_id', '') or '').strip(),
                        key_prefix='business/logo',
                    )
                    bs.logo_file_id = asset.id
                    bs.logo_filename = None
                except Exception:
                    current_app.logger.exception('Failed to upload business logo to R2')
                    flash('No se pudo subir el logo. Intentá nuevamente.', 'error')
                    return redirect(url_for('user_settings.index'))

            db.session.add(bs)
            db.session.commit()
            flash('Datos del negocio guardados.', 'success')
            return redirect(url_for('user_settings.index'))

        if action == 'create_user':
            username = (request.form.get('username') or '').strip()
            display_name = (request.form.get('display_name') or '').strip()
            email = (request.form.get('email') or '').strip().lower()
            password = request.form.get('password') or ''
            role_raw = (request.form.get('role') or 'vendedor').strip()
            if role_raw == '__new__':
                role_raw = (request.form.get('role_new') or '').strip()
            role = _ensure_company_role_exists(str(g.company_id or ''), role_raw) or role_raw

            if not username or not password:
                flash('Completá usuario y contraseña.', 'error')
                return redirect(url_for('user_settings.index'))
            if db.session.query(User).filter(User.username == username, User.company_id == str(g.company_id or '')).first():
                flash('El usuario ya existe.', 'error')
                return redirect(url_for('user_settings.index'))
            if email and db.session.query(User).filter(User.email == email).first():
                flash('El email ya existe.', 'error')
                return redirect(url_for('user_settings.index'))

            u = User(username=username, display_name=(display_name or None), email=(email or None), role=role, is_master=False, company_id=str(g.company_id or ''))
            u.set_password(password)

            perms = {}
            for key in MODULE_KEYS:
                perms[key] = bool(request.form.get(f'perm_{key}') == 'on')
            u.set_permissions(perms)

            db.session.add(u)
            db.session.commit()
            flash('Usuario creado.', 'success')
            return redirect(url_for('user_settings.index'))

        if action == 'update_user':
            uid = request.form.get('user_id')
            u = db.session.get(User, int(uid)) if uid and str(uid).isdigit() else None
            if not u:
                flash('Usuario inválido.', 'error')
                return redirect(url_for('user_settings.index'))
            if getattr(current_user, 'role', '') == 'zentral_admin':
                if str(getattr(g, 'company_id', '') or ''):
                    if str(getattr(u, 'company_id', '') or '') != str(getattr(g, 'company_id', '') or ''):
                        flash('Usuario inválido.', 'error')
                        return redirect(url_for('user_settings.index'))
            else:
                if str(getattr(u, 'company_id', '') or '') != str(getattr(g, 'company_id', '') or ''):
                    flash('Usuario inválido.', 'error')
                    return redirect(url_for('user_settings.index'))
            if u.is_master:
                flash('El usuario master no es editable.', 'error')
                return redirect(url_for('user_settings.index'))

            username = (request.form.get('username') or '').strip()
            if username and username != u.username:
                if db.session.query(User).filter(User.username == username, User.company_id == str(getattr(u, 'company_id', '') or ''), User.id != u.id).first():
                    flash('Ya existe otro usuario con ese nombre.', 'error')
                    return redirect(url_for('user_settings.index'))
                u.username = username

            email = (request.form.get('email') or '').strip().lower()
            if email != (u.email or '').strip().lower():
                if email and db.session.query(User).filter(User.email == email, User.id != u.id).first():
                    flash('Ese email ya existe.', 'error')
                    return redirect(url_for('user_settings.index'))
            u.email = (email or None)

            role_raw = (request.form.get('role') or u.role).strip()
            if role_raw == '__new__':
                role_raw = (request.form.get('role_new') or '').strip()
            role = _ensure_company_role_exists(str(getattr(u, 'company_id', '') or ''), role_raw) or role_raw
            u.role = role

            active = bool(request.form.get('active') == 'on')
            u.active = active

            newPass = request.form.get('new_password') or ''
            if newPass.strip():
                u.set_password(newPass.strip())

            perms = {}
            for key in MODULE_KEYS:
                perms[key] = bool(request.form.get(f'perm_{key}') == 'on')
            u.set_permissions(perms)

            db.session.commit()
            flash('Usuario actualizado.', 'success')
            return redirect(url_for('user_settings.index'))

        if action == 'delete_user':
            uid = request.form.get('user_id')
            u = db.session.get(User, int(uid)) if uid and str(uid).isdigit() else None
            if not u:
                flash('Usuario inválido.', 'error')
                return redirect(url_for('user_settings.index'))
            if getattr(current_user, 'role', '') == 'zentral_admin':
                if str(getattr(g, 'company_id', '') or ''):
                    if str(getattr(u, 'company_id', '') or '') != str(getattr(g, 'company_id', '') or ''):
                        flash('Usuario inválido.', 'error')
                        return redirect(url_for('user_settings.index'))
            else:
                if str(getattr(u, 'company_id', '') or '') != str(getattr(g, 'company_id', '') or ''):
                    flash('Usuario inválido.', 'error')
                    return redirect(url_for('user_settings.index'))
            if u.is_master:
                flash('El usuario master no se puede eliminar.', 'error')
                return redirect(url_for('user_settings.index'))
            if current_user.id == u.id:
                flash('No podés eliminar tu propio usuario.', 'error')
                return redirect(url_for('user_settings.index'))
            db.session.delete(u)
            db.session.commit()
            flash('Usuario eliminado.', 'success')
            return redirect(url_for('user_settings.index'))

    q = db.session.query(User)
    if str(getattr(g, 'company_id', '') or '').strip():
        q = q.filter(User.company_id == str(getattr(g, 'company_id', '') or ''))
    users = q.order_by(User.is_master.desc(), User.username.asc()).all()

    roles = ['company_admin', 'admin', 'vendedor', 'contador']
    try:
        cid = str(getattr(g, 'company_id', '') or '').strip()
        if cid:
            rows = (
                db.session.query(CompanyRole)
                .filter(CompanyRole.company_id == cid)
                .order_by(CompanyRole.name.asc())
                .all()
            )
            for r in (rows or []):
                n = str(getattr(r, 'name', '') or '').strip()
                if n and n not in roles:
                    roles.append(n)
    except Exception:
        pass
    business = BusinessSettings.get_for_company(g.company_id)
    return render_template('user_settings/index.html', title='Configuración de Usuario', users=users, roles=roles, business=business, company=getattr(g, 'company', None))


@bp.route('/user/<int:user_id>', methods=['GET', 'POST'])
@login_required
def user_detail(user_id: int):
    u = db.session.get(User, int(user_id))
    if not u:
        flash('Usuario inválido.', 'error')
        return redirect(url_for('main.index'))

    if getattr(current_user, 'role', '') == 'zentral_admin':
        if str(getattr(g, 'company_id', '') or ''):
            if str(getattr(u, 'company_id', '') or '') != str(getattr(g, 'company_id', '') or ''):
                flash('Usuario inválido.', 'error')
                return redirect(url_for('main.index'))
    else:
        if str(getattr(u, 'company_id', '') or '') != str(getattr(g, 'company_id', '') or ''):
            flash('Usuario inválido.', 'error')
            return redirect(url_for('main.index'))

    can_manage_users = bool(
        getattr(current_user, 'is_master', False)
        or (getattr(current_user, 'role', '') in {'admin', 'company_admin', 'zentral_admin'})
    )
    is_self = bool(current_user.id == u.id)
    can_view = bool(is_self or can_manage_users)
    if not can_view:
        flash('No tenés permisos para ver este usuario.', 'error')
        return redirect(url_for('main.index'))

    if request.method == 'POST':
        action = (request.form.get('action') or '').strip()

        if action == 'delete_user':
            if u.is_master:
                flash('El usuario master no es editable.', 'error')
                return redirect(url_for('user_settings.user_detail', user_id=u.id))
            if not can_manage_users:
                flash('No tenés permisos para eliminar usuarios.', 'error')
                return redirect(url_for('user_settings.user_detail', user_id=u.id))
            if current_user.id == u.id:
                flash('No podés eliminar tu propio usuario.', 'error')
                return redirect(url_for('user_settings.user_detail', user_id=u.id))
            db.session.delete(u)
            db.session.commit()
            flash('Usuario eliminado.', 'success')
            return redirect(url_for('user_settings.index'))

        if action == 'save_user':
            if u.is_master:
                flash('El usuario master no es editable.', 'error')
                return redirect(url_for('user_settings.user_detail', user_id=u.id))
            if not can_manage_users:
                flash('No tenés permisos para editar este usuario.', 'error')
                return redirect(url_for('user_settings.user_detail', user_id=u.id))

            username = (request.form.get('username') or '').strip()
            if username and username != u.username:
                if db.session.query(User).filter(User.username == username, User.company_id == str(getattr(u, 'company_id', '') or ''), User.id != u.id).first():
                    flash('Ya existe otro usuario con ese nombre.', 'error')
                    return redirect(url_for('user_settings.user_detail', user_id=u.id))
                u.username = username

            email = (request.form.get('email') or '').strip().lower()
            if email != (u.email or '').strip().lower():
                if email and db.session.query(User).filter(User.email == email, User.id != u.id).first():
                    flash('Ese email ya existe.', 'error')
                    return redirect(url_for('user_settings.user_detail', user_id=u.id))
            u.email = (email or None)

            display_name = (request.form.get('display_name') or '').strip()
            u.display_name = (display_name or None)

            role_raw = (request.form.get('role') or u.role).strip()
            if role_raw == '__new__':
                role_raw = (request.form.get('role_new') or '').strip()
            role = _ensure_company_role_exists(str(getattr(u, 'company_id', '') or ''), role_raw) or role_raw
            u.role = role

            u.active = bool(request.form.get('active') == 'on')

            perms = {}
            for key in MODULE_KEYS:
                perms[key] = bool(request.form.get(f'perm_{key}') == 'on')
            u.set_permissions(perms)

            db.session.commit()
            flash('Usuario actualizado.', 'success')
            return redirect(url_for('user_settings.user_detail', user_id=u.id))

        if action == 'change_password':
            if u.is_master and not is_self:
                flash('El usuario master no es editable.', 'error')
                return redirect(url_for('user_settings.user_detail', user_id=u.id))
            if not (is_self or can_manage_users):
                flash('No tenés permisos para cambiar esta contraseña.', 'error')
                return redirect(url_for('user_settings.user_detail', user_id=u.id))
            current_pass = (request.form.get('current_password') or '').strip()
            new_pass = (request.form.get('new_password') or '').strip()
            new_pass_confirm = (request.form.get('new_password_confirm') or '').strip()

            if not new_pass:
                flash('Ingresá una nueva contraseña.', 'error')
                return redirect(url_for('user_settings.user_detail', user_id=u.id))
            if new_pass != new_pass_confirm:
                flash('La confirmación de contraseña no coincide.', 'error')
                return redirect(url_for('user_settings.user_detail', user_id=u.id))

            if is_self:
                if not current_pass:
                    flash('Ingresá tu contraseña actual.', 'error')
                    return redirect(url_for('user_settings.user_detail', user_id=u.id))
                if not u.check_password(current_pass):
                    flash('Contraseña actual incorrecta.', 'error')
                    return redirect(url_for('user_settings.user_detail', user_id=u.id))

            u.set_password(new_pass)
            db.session.commit()
            flash('Contraseña actualizada.', 'success')
            return redirect(url_for('user_settings.user_detail', user_id=u.id))

    roles = ['company_admin', 'admin', 'vendedor', 'contador']
    try:
        cid = str(getattr(u, 'company_id', '') or '').strip()
        if cid:
            rows = (
                db.session.query(CompanyRole)
                .filter(CompanyRole.company_id == cid)
                .order_by(CompanyRole.name.asc())
                .all()
            )
            for r in (rows or []):
                n = str(getattr(r, 'name', '') or '').strip()
                if n and n not in roles:
                    roles.append(n)
    except Exception:
        pass
    perms = u.get_permissions()
    can_edit_all = bool(can_manage_users and (not u.is_master))
    can_change_password = bool(is_self or (can_manage_users and (not u.is_master)))
    return render_template(
        'user_settings/user_detail.html',
        title=f'Usuario: {u.username}',
        user=u,
        roles=roles,
        perms=perms,
        can_manage_users=can_manage_users,
        can_edit_all=can_edit_all,
        can_change_password=can_change_password,
        is_self=is_self,
    )
