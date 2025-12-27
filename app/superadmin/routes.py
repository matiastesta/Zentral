import re
from datetime import datetime
import secrets
import string

from flask import abort, flash, redirect, render_template, request, session, url_for
from flask_login import current_user, login_required

from app import db
from app.models import (
    BusinessSettings,
    CalendarEvent,
    CalendarUserConfig,
    CashCount,
    Category,
    Company,
    Customer,
    Employee,
    Expense,
    ExpenseCategory,
    InventoryLot,
    InventoryMovement,
    Product,
    Sale,
    SaleItem,
    CompanyRole,
    Plan,
    Supplier,
    User,
)

from app.superadmin import bp


MODULE_KEYS = [
    'dashboard',
    'calendar',
    'sales',
    'expenses',
    'inventory',
    'customers',
    'suppliers',
    'employees',
    'movements',
    'reports',
    'settings',
    'user_settings',
]


def _ensure_default_roles(company_id: str) -> None:
    cid = str(company_id or '').strip()
    if not cid:
        return

    existing = {
        str(r.name or '').strip()
        for r in (db.session.query(CompanyRole).filter(CompanyRole.company_id == cid).all() or [])
    }

    def _mk(name: str, perms: dict) -> None:
        n = str(name or '').strip()
        if not n or n in existing:
            return
        rr = CompanyRole(company_id=cid, name=n)
        rr.set_permissions(perms)
        db.session.add(rr)
        existing.add(n)

    all_true = {k: True for k in MODULE_KEYS}
    _mk('company_admin', all_true)
    _mk('admin', all_true)
    _mk(
        'vendedor',
        {
            'dashboard': True,
            'calendar': True,
            'sales': True,
            'expenses': False,
            'inventory': True,
            'customers': True,
            'suppliers': False,
            'employees': False,
            'movements': True,
            'reports': False,
            'settings': False,
            'user_settings': False,
        },
    )
    _mk(
        'contador',
        {
            'dashboard': True,
            'calendar': False,
            'sales': False,
            'expenses': True,
            'inventory': False,
            'customers': False,
            'suppliers': True,
            'employees': False,
            'movements': False,
            'reports': True,
            'settings': False,
            'user_settings': False,
        },
    )


def _company_role_names(company_id: str) -> list[str]:
    cid = str(company_id or '').strip()
    base = ['company_admin', 'admin', 'vendedor', 'contador']
    if not cid:
        return base

    try:
        _ensure_default_roles(cid)
        db.session.flush()
    except Exception:
        pass

    names = list(base)
    try:
        rows = (
            db.session.query(CompanyRole)
            .filter(CompanyRole.company_id == cid)
            .order_by(CompanyRole.name.asc())
            .all()
        )
        for r in (rows or []):
            n = str(getattr(r, 'name', '') or '').strip()
            if n and n not in names:
                names.append(n)
    except Exception:
        pass
    return names


def _require_zentral_admin():
    if not current_user.is_authenticated:
        abort(401)
    if getattr(current_user, 'role', '') != 'zentral_admin':
        abort(403)


def _slugify(name: str) -> str:
    s = str(name or '').strip().lower()
    s = re.sub(r'[^a-z0-9]+', '-', s)
    s = re.sub(r'-{2,}', '-', s).strip('-')
    return s or 'empresa'


def _normalize_code(raw: str) -> str:
    s = str(raw or '').strip().lower()
    s = re.sub(r'[^a-z0-9_-]+', '-', s)
    s = re.sub(r'-{2,}', '-', s).strip('-')
    return s


def _ensure_plan_exists(raw_code: str) -> str:
    code = _normalize_code(raw_code)
    if not code:
        return ''
    try:
        row = db.session.get(Plan, code)
        if row:
            try:
                row.active = True
            except Exception:
                pass
            return code
        db.session.add(Plan(code=code, active=True))
        db.session.flush()
        return code
    except Exception:
        return ''


@bp.post('/plans/create')
@login_required
def plan_create():
    _require_zentral_admin()
    raw = (request.form.get('plan_code') or '').strip()
    code = _ensure_plan_exists(raw)
    if not code:
        flash('Plan inválido.', 'error')
        return redirect(url_for('superadmin.index'))
    try:
        db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
    flash('Plan creado.', 'success')
    return redirect(url_for('superadmin.index'))


@bp.post('/plans/<plan_code>/disable')
@login_required
def plan_disable(plan_code: str):
    _require_zentral_admin()
    code = _normalize_code(plan_code)
    if not code:
        flash('Plan inválido.', 'error')
        return redirect(url_for('superadmin.index'))
    row = db.session.get(Plan, code)
    if not row:
        flash('Plan inválido.', 'error')
        return redirect(url_for('superadmin.index'))
    try:
        row.active = False
        db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
        flash('No se pudo eliminar el plan.', 'error')
        return redirect(url_for('superadmin.index'))
    flash('Plan eliminado.', 'success')
    return redirect(url_for('superadmin.index'))


def _normalize_role_name(raw: str) -> str:
    s = str(raw or '').strip().lower()
    s = re.sub(r'\s+', '_', s)
    s = re.sub(r'[^a-z0-9_-]+', '', s)
    s = re.sub(r'_{2,}', '_', s).strip('_')
    s = re.sub(r'-{2,}', '-', s).strip('-')
    return s


def _ensure_company_role_exists(company_id: str, raw_role: str) -> str:
    cid = str(company_id or '').strip()
    role = _normalize_role_name(raw_role)
    if not cid or not role:
        return ''

    try:
        _ensure_default_roles(cid)
    except Exception:
        pass

    try:
        exists = (
            db.session.query(CompanyRole)
            .filter(CompanyRole.company_id == cid, CompanyRole.name == role)
            .first()
        )
        if exists:
            return role
        rr = CompanyRole(company_id=cid, name=role)
        rr.set_permissions({})
        db.session.add(rr)
        db.session.flush()
        return role
    except Exception:
        return ''


@bp.get('/')
@login_required
def index():
    _require_zentral_admin()
    companies = db.session.query(Company).order_by(Company.created_at.desc()).all()
    plans = []
    try:
        plans = [p.code for p in (db.session.query(Plan).filter(Plan.active == True).order_by(Plan.code.asc()).all() or [])]
    except Exception:
        plans = []
    plans_rows = []
    try:
        plans_rows = (db.session.query(Plan).order_by(Plan.code.asc()).all() or [])
    except Exception:
        plans_rows = []
    return render_template('superadmin/index.html', title='Zentral Admin', companies=companies, plans=plans, plans_rows=plans_rows)


@bp.get('/companies/<company_id>')
@login_required
def company_overview(company_id: str):
    _require_zentral_admin()
    c = db.session.get(Company, str(company_id))
    if not c:
        flash('Empresa inválida.', 'error')
        return redirect(url_for('superadmin.index'))

    try:
        _ensure_default_roles(str(c.id))
        db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass

    admin_user = None
    try:
        if getattr(c, 'admin_user_id', None):
            admin_user = db.session.get(User, int(c.admin_user_id))
    except Exception:
        admin_user = None

    users = db.session.query(User).filter(User.company_id == str(c.id)).order_by(User.username.asc()).all()
    roles = (
        db.session.query(CompanyRole)
        .filter(CompanyRole.company_id == str(c.id))
        .order_by(CompanyRole.name.asc())
        .all()
    )

    return render_template(
        'superadmin/company_overview.html',
        title=f'Editar · {c.name}',
        company=c,
        admin_user=admin_user,
        users=users,
        roles=roles,
    )


@bp.post('/companies/create')
@login_required
def create_company():
    _require_zentral_admin()
    name = (request.form.get('company_name') or '').strip()
    plan = (request.form.get('plan') or '').strip()
    plan_new = (request.form.get('plan_new') or '').strip()
    admin_username = (request.form.get('admin_username') or '').strip()
    admin_password = (request.form.get('admin_password') or '').strip()
    notes = (request.form.get('notes') or '').strip() or None

    if not name:
        flash('Nombre inválido.', 'error')
        return redirect(url_for('superadmin.index'))

    effective_plan = plan
    if plan == '__new__':
        effective_plan = plan_new
    effective_plan = _ensure_plan_exists(effective_plan)
    if not effective_plan:
        flash('Plan inválido.', 'error')
        return redirect(url_for('superadmin.index'))

    if not admin_username:
        flash('Usuario administrativo inválido.', 'error')
        return redirect(url_for('superadmin.index'))

    if not admin_password or len(admin_password) < 6:
        flash('Contraseña inválida (mínimo 6 caracteres).', 'error')
        return redirect(url_for('superadmin.index'))

    slug = _slugify(name)
    base_slug = slug
    i = 1
    while db.session.query(Company).filter(Company.slug == slug).first():
        i += 1
        slug = f'{base_slug}-{i}'

    c = Company(name=name, slug=slug, plan=effective_plan, status='active', notes=notes)
    db.session.add(c)
    db.session.flush()

    _ensure_default_roles(str(c.id))

    u = User(
        username=admin_username,
        email=None,
        role='company_admin',
        is_master=False,
        active=True,
        company_id=str(c.id),
        created_by_user_id=int(getattr(current_user, 'id', 0) or 0) or None,
    )
    u.set_password(admin_password)
    u.set_permissions_all(True)
    db.session.add(u)
    db.session.flush()

    try:
        c.admin_user_id = int(getattr(u, 'id', 0) or 0) or None
    except Exception:
        pass

    db.session.commit()

    flash('Empresa creada.', 'success')
    return redirect(url_for('superadmin.company_overview', company_id=c.id))


@bp.route('/companies/<company_id>/edit', methods=['GET', 'POST'])
@login_required
def company_edit(company_id: str):
    _require_zentral_admin()
    c = db.session.get(Company, str(company_id))
    if not c:
        flash('Empresa inválida.', 'error')
        return redirect(url_for('superadmin.index'))

    admin_user = None
    try:
        if getattr(c, 'admin_user_id', None):
            admin_user = db.session.get(User, int(c.admin_user_id))
    except Exception:
        admin_user = None

    if request.method == 'POST':
        name = (request.form.get('company_name') or '').strip()
        plan = (request.form.get('plan') or '').strip()
        plan_new = (request.form.get('plan_new') or '').strip()
        notes = (request.form.get('notes') or '').strip() or None
        admin_username = (request.form.get('admin_username') or '').strip()
        admin_password = (request.form.get('admin_password') or '').strip()

        if not name:
            flash('Nombre inválido.', 'error')
            return redirect(url_for('superadmin.company_edit', company_id=c.id))
        effective_plan = plan
        if plan == '__new__':
            effective_plan = plan_new
        effective_plan = _ensure_plan_exists(effective_plan)
        if not effective_plan:
            flash('Plan inválido.', 'error')
            return redirect(url_for('superadmin.company_edit', company_id=c.id))

        c.name = name
        c.plan = effective_plan
        try:
            c.notes = notes
        except Exception:
            pass

        try:
            _ensure_default_roles(str(c.id))
        except Exception:
            pass

        if admin_username or admin_password:
            if not admin_user:
                if not admin_username:
                    flash('Usuario administrativo inválido.', 'error')
                    return redirect(url_for('superadmin.company_edit', company_id=c.id))
                if not admin_password or len(admin_password) < 6:
                    flash('Contraseña inválida (mínimo 6 caracteres).', 'error')
                    return redirect(url_for('superadmin.company_edit', company_id=c.id))

                exists = (
                    db.session.query(User)
                    .filter(User.company_id == str(c.id), User.username == admin_username)
                    .first()
                )
                if exists:
                    flash('Ya existe otro usuario con ese nombre en esta empresa.', 'error')
                    return redirect(url_for('superadmin.company_edit', company_id=c.id))

                admin_user = User(
                    username=admin_username,
                    email=None,
                    role='company_admin',
                    is_master=False,
                    active=True,
                    company_id=str(c.id),
                    created_by_user_id=int(getattr(current_user, 'id', 0) or 0) or None,
                )
                admin_user.set_password(admin_password)
                admin_user.set_permissions_all(True)
                db.session.add(admin_user)
                db.session.flush()
                c.admin_user_id = int(getattr(admin_user, 'id', 0) or 0) or None
            else:
                if admin_username and admin_username != (admin_user.username or ''):
                    exists = (
                        db.session.query(User)
                        .filter(User.company_id == str(c.id), User.username == admin_username, User.id != admin_user.id)
                        .first()
                    )
                    if exists:
                        flash('Ya existe otro usuario con ese nombre en esta empresa.', 'error')
                        return redirect(url_for('superadmin.company_edit', company_id=c.id))
                    admin_user.username = admin_username

                if admin_password:
                    if len(admin_password) < 6:
                        flash('Contraseña inválida (mínimo 6 caracteres).', 'error')
                        return redirect(url_for('superadmin.company_edit', company_id=c.id))
                    admin_user.set_password(admin_password)

                admin_user.active = True
                admin_user.role = 'company_admin'
                admin_user.company_id = str(c.id)
                try:
                    admin_user.set_permissions_all(True)
                except Exception:
                    pass

                try:
                    c.admin_user_id = int(getattr(admin_user, 'id', 0) or 0) or None
                except Exception:
                    pass

        db.session.commit()
        flash('Empresa actualizada.', 'success')
        return redirect(url_for('superadmin.company_overview', company_id=c.id))

    plans = []
    try:
        plans = [p.code for p in (db.session.query(Plan).filter(Plan.active == True).order_by(Plan.code.asc()).all() or [])]
    except Exception:
        plans = []
    cur_plan = str(getattr(c, 'plan', '') or '').strip()
    if cur_plan and cur_plan not in plans:
        plans = [cur_plan] + plans

    plans_rows = []
    try:
        plans_rows = (db.session.query(Plan).order_by(Plan.code.asc()).all() or [])
    except Exception:
        plans_rows = []

    return render_template(
        'superadmin/company_edit.html',
        title=f'Editar empresa · {c.name}',
        company=c,
        admin_user=admin_user,
        plans=plans,
        plans_rows=plans_rows,
    )


@bp.post('/companies/<company_id>/pause')
@login_required
def pause_company(company_id: str):
    _require_zentral_admin()
    reason = (request.form.get('reason') or '').strip() or None
    c = db.session.get(Company, str(company_id))
    if not c:
        flash('Empresa inválida.', 'error')
        return redirect(url_for('superadmin.index'))
    c.status = 'paused'
    c.paused_at = datetime.utcnow()
    c.pause_reason = reason
    db.session.commit()
    flash(f'Empresa {c.name} pausada.', 'success')
    return redirect(url_for('superadmin.index'))


@bp.route('/companies/<company_id>/delete', methods=['POST'])
@login_required
def delete_company(company_id: str):
    _require_zentral_admin()
    c = db.session.get(Company, str(company_id))
    if not c:
        flash('Empresa inválida.', 'error')
        return redirect(url_for('superadmin.index'))

    if session.get('impersonate_company_id') == str(c.id):
        session.pop('impersonate_company_id', None)

    cid = str(c.id)

    # Delete child tables first
    db.session.query(SaleItem).filter(SaleItem.company_id == cid).delete(synchronize_session=False)
    db.session.query(Sale).filter(Sale.company_id == cid).delete(synchronize_session=False)
    db.session.query(InventoryMovement).filter(InventoryMovement.company_id == cid).delete(synchronize_session=False)
    db.session.query(InventoryLot).filter(InventoryLot.company_id == cid).delete(synchronize_session=False)
    db.session.query(Expense).filter(Expense.company_id == cid).delete(synchronize_session=False)
    db.session.query(ExpenseCategory).filter(ExpenseCategory.company_id == cid).delete(synchronize_session=False)
    db.session.query(CalendarEvent).filter(CalendarEvent.company_id == cid).delete(synchronize_session=False)
    db.session.query(CalendarUserConfig).filter(CalendarUserConfig.company_id == cid).delete(synchronize_session=False)
    db.session.query(CashCount).filter(CashCount.company_id == cid).delete(synchronize_session=False)
    db.session.query(Product).filter(Product.company_id == cid).delete(synchronize_session=False)
    db.session.query(Category).filter(Category.company_id == cid).delete(synchronize_session=False)
    db.session.query(Customer).filter(Customer.company_id == cid).delete(synchronize_session=False)
    db.session.query(Supplier).filter(Supplier.company_id == cid).delete(synchronize_session=False)
    db.session.query(Employee).filter(Employee.company_id == cid).delete(synchronize_session=False)
    db.session.query(BusinessSettings).filter(BusinessSettings.company_id == cid).delete(synchronize_session=False)
    db.session.query(CompanyRole).filter(CompanyRole.company_id == cid).delete(synchronize_session=False)
    db.session.query(User).filter(User.company_id == cid).delete(synchronize_session=False)

    db.session.delete(c)
    db.session.commit()

    flash('Empresa eliminada.', 'success')
    return redirect(url_for('superadmin.index'))


@bp.post('/companies/<company_id>/reactivate')
@login_required
def reactivate_company(company_id: str):
    _require_zentral_admin()
    c = db.session.get(Company, str(company_id))
    if not c:
        flash('Empresa inválida.', 'error')
        return redirect(url_for('superadmin.index'))
    c.status = 'active'
    c.paused_at = None
    c.pause_reason = None
    db.session.commit()
    flash('Empresa reactivada.', 'success')
    return redirect(url_for('superadmin.index'))


@bp.post('/companies/<company_id>/impersonate')
@login_required
def impersonate_company(company_id: str):
    _require_zentral_admin()
    c = db.session.get(Company, str(company_id))
    if not c:
        flash('Empresa inválida.', 'error')
        return redirect(url_for('superadmin.index'))
    session['impersonate_company_id'] = str(c.id)
    flash(f'Modo soporte activado para {c.name}.', 'success')
    return redirect(url_for('main.index'))


@bp.post('/impersonate/clear')
@login_required
def clear_impersonation():
    _require_zentral_admin()
    session.pop('impersonate_company_id', None)
    flash('Modo soporte desactivado.', 'success')
    return redirect(url_for('superadmin.index'))


@bp.get('/companies/<company_id>/users')
@login_required
def company_users(company_id: str):
    _require_zentral_admin()
    c = db.session.get(Company, str(company_id))
    if not c:
        flash('Empresa inválida.', 'error')
        return redirect(url_for('superadmin.index'))
    users = db.session.query(User).filter(User.company_id == str(c.id)).order_by(User.username.asc()).all()
    return render_template('superadmin/company_users.html', title=f'Usuarios · {c.name}', company=c, users=users)


@bp.route('/companies/<company_id>/users/<int:user_id>/edit', methods=['GET', 'POST'])
@login_required
def company_user_edit(company_id: str, user_id: int):
    _require_zentral_admin()
    c = db.session.get(Company, str(company_id))
    if not c:
        flash('Empresa inválida.', 'error')
        return redirect(url_for('superadmin.index'))

    u = db.session.get(User, int(user_id))
    if not u or str(getattr(u, 'company_id', '') or '') != str(c.id):
        flash('Usuario inválido.', 'error')
        return redirect(url_for('superadmin.company_users', company_id=c.id))

    if request.method == 'POST':
        if getattr(u, 'is_master', False):
            flash('El usuario master no es editable.', 'error')
            return redirect(url_for('superadmin.company_user_edit', company_id=c.id, user_id=u.id))

        username = (request.form.get('username') or '').strip()
        if username and username != u.username:
            exists = (
                db.session.query(User)
                .filter(User.company_id == str(c.id), User.username == username, User.id != u.id)
                .first()
            )
            if exists:
                flash('Ya existe otro usuario con ese nombre en esta empresa.', 'error')
                return redirect(url_for('superadmin.company_user_edit', company_id=c.id, user_id=u.id))
            u.username = username

        email = (request.form.get('email') or '').strip().lower()
        if email != (u.email or '').strip().lower():
            if email and db.session.query(User).filter(User.email == email, User.id != u.id).first():
                flash('Ese email ya existe.', 'error')
                return redirect(url_for('superadmin.company_user_edit', company_id=c.id, user_id=u.id))
        u.email = (email or None)

        u.display_name = ((request.form.get('display_name') or '').strip() or None)

        role_raw = (request.form.get('role') or '').strip() or (u.role or 'vendedor')
        if role_raw == '__new__':
            role_raw = (request.form.get('role_new') or '').strip()
        role = _ensure_company_role_exists(str(c.id), role_raw)
        if not role:
            flash('Rol inválido.', 'error')
            return redirect(url_for('superadmin.company_user_edit', company_id=c.id, user_id=u.id))
        u.role = role

        u.active = bool(request.form.get('active') == 'on')

        perms = {}
        for key in MODULE_KEYS:
            perms[key] = bool(request.form.get(f'perm_{key}') == 'on')
        u.set_permissions(perms)

        new_password = (request.form.get('new_password') or '').strip()
        new_password_confirm = (request.form.get('new_password_confirm') or '').strip()
        if new_password or new_password_confirm:
            if new_password != new_password_confirm:
                flash('Las contraseñas no coinciden.', 'error')
                return redirect(url_for('superadmin.company_user_edit', company_id=c.id, user_id=u.id))
            if len(new_password) < 6:
                flash('La nueva contraseña es muy corta.', 'error')
                return redirect(url_for('superadmin.company_user_edit', company_id=c.id, user_id=u.id))
            u.set_password(new_password)

        db.session.commit()
        flash('Usuario actualizado.', 'success')
        return redirect(url_for('superadmin.company_users', company_id=c.id))

    try:
        _ensure_default_roles(str(c.id))
        db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass

    roles = _company_role_names(str(c.id))
    perms = u.get_permissions() if getattr(u, 'get_permissions', None) else {}
    return render_template(
        'superadmin/company_user_edit.html',
        title=f'Editar usuario · {c.name}',
        company=c,
        user=u,
        roles=roles,
        perms=perms,
    )


@bp.post('/companies/<company_id>/users/<int:user_id>/reset-password')
@login_required
def company_users_reset_password(company_id: str, user_id: int):
    _require_zentral_admin()
    c = db.session.get(Company, str(company_id))
    if not c:
        flash('Empresa inválida.', 'error')
        return redirect(url_for('superadmin.index'))

    u = db.session.get(User, int(user_id))
    if not u:
        flash('Usuario inválido.', 'error')
        return redirect(url_for('superadmin.company_users', company_id=c.id))
    if str(getattr(u, 'company_id', '') or '') != str(c.id):
        flash('Usuario inválido.', 'error')
        return redirect(url_for('superadmin.company_users', company_id=c.id))
    if getattr(u, 'is_master', False):
        flash('El usuario master no es editable.', 'error')
        return redirect(url_for('superadmin.company_users', company_id=c.id))

    alphabet = string.ascii_letters + string.digits
    new_pass = ''.join(secrets.choice(alphabet) for _ in range(12))
    u.set_password(new_pass)
    db.session.commit()
    flash(f'Contraseña reseteada para {u.username}. Nueva contraseña: {new_pass}', 'success')
    return redirect(url_for('superadmin.company_users', company_id=c.id))


@bp.get('/companies/<company_id>/roles')
@login_required
def company_roles(company_id: str):
    _require_zentral_admin()
    c = db.session.get(Company, str(company_id))
    if not c:
        flash('Empresa inválida.', 'error')
        return redirect(url_for('superadmin.index'))

    try:
        _ensure_default_roles(str(c.id))
        db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass

    roles = (
        db.session.query(CompanyRole)
        .filter(CompanyRole.company_id == str(c.id))
        .order_by(CompanyRole.name.asc())
        .all()
    )
    return render_template('superadmin/company_roles.html', title=f'Roles · {c.name}', company=c, roles=roles, module_keys=MODULE_KEYS)


@bp.route('/companies/<company_id>/roles/new', methods=['GET', 'POST'])
@login_required
def company_role_new(company_id: str):
    _require_zentral_admin()
    c = db.session.get(Company, str(company_id))
    if not c:
        flash('Empresa inválida.', 'error')
        return redirect(url_for('superadmin.index'))

    if request.method == 'POST':
        name = (request.form.get('name') or '').strip()
        if not name:
            flash('Nombre de rol inválido.', 'error')
            return redirect(url_for('superadmin.company_role_new', company_id=c.id))

        existing = (
            db.session.query(CompanyRole)
            .filter(CompanyRole.company_id == str(c.id), CompanyRole.name == name)
            .first()
        )
        if existing:
            flash('Ya existe un rol con ese nombre.', 'error')
            return redirect(url_for('superadmin.company_role_new', company_id=c.id))

        perms = {}
        for k in MODULE_KEYS:
            perms[k] = bool(request.form.get(f'perm_{k}') == 'on')

        r = CompanyRole(company_id=str(c.id), name=name)
        r.set_permissions(perms)
        db.session.add(r)
        db.session.commit()
        flash('Rol creado.', 'success')
        return redirect(url_for('superadmin.company_roles', company_id=c.id))

    return render_template(
        'superadmin/company_role_edit.html',
        title=f'Nuevo rol · {c.name}',
        company=c,
        role=None,
        module_keys=MODULE_KEYS,
        perms={},
    )


@bp.route('/companies/<company_id>/roles/<role_id>/edit', methods=['GET', 'POST'])
@login_required
def company_role_edit(company_id: str, role_id: str):
    _require_zentral_admin()
    c = db.session.get(Company, str(company_id))
    if not c:
        flash('Empresa inválida.', 'error')
        return redirect(url_for('superadmin.index'))

    r = db.session.get(CompanyRole, str(role_id))
    if not r or str(getattr(r, 'company_id', '') or '') != str(c.id):
        flash('Rol inválido.', 'error')
        return redirect(url_for('superadmin.company_roles', company_id=c.id))

    if request.method == 'POST':
        name = (request.form.get('name') or '').strip() or (r.name or '')
        if not name:
            flash('Nombre de rol inválido.', 'error')
            return redirect(url_for('superadmin.company_role_edit', company_id=c.id, role_id=r.id))

        exists = (
            db.session.query(CompanyRole)
            .filter(CompanyRole.company_id == str(c.id), CompanyRole.name == name, CompanyRole.id != r.id)
            .first()
        )
        if exists:
            flash('Ya existe otro rol con ese nombre.', 'error')
            return redirect(url_for('superadmin.company_role_edit', company_id=c.id, role_id=r.id))

        perms = {}
        for k in MODULE_KEYS:
            perms[k] = bool(request.form.get(f'perm_{k}') == 'on')

        r.name = name
        r.set_permissions(perms)
        db.session.commit()
        flash('Rol actualizado.', 'success')
        return redirect(url_for('superadmin.company_roles', company_id=c.id))

    perms = r.get_permissions() if getattr(r, 'get_permissions', None) else {}
    return render_template(
        'superadmin/company_role_edit.html',
        title=f'Editar rol · {c.name}',
        company=c,
        role=r,
        module_keys=MODULE_KEYS,
        perms=perms,
    )


@bp.post('/companies/<company_id>/roles/<role_id>/delete')
@login_required
def company_role_delete(company_id: str, role_id: str):
    _require_zentral_admin()
    c = db.session.get(Company, str(company_id))
    if not c:
        flash('Empresa inválida.', 'error')
        return redirect(url_for('superadmin.index'))

    r = db.session.get(CompanyRole, str(role_id))
    if not r or str(getattr(r, 'company_id', '') or '') != str(c.id):
        flash('Rol inválido.', 'error')
        return redirect(url_for('superadmin.company_roles', company_id=c.id))

    try:
        role_name = str(getattr(r, 'name', '') or '').strip()
        db.session.query(User).filter(User.company_id == str(c.id), User.role == role_name).update({User.role: 'vendedor'})
    except Exception:
        pass

    db.session.delete(r)
    db.session.commit()
    flash('Rol eliminado.', 'success')
    return redirect(url_for('superadmin.company_roles', company_id=c.id))
