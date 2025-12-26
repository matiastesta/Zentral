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


@bp.get('/')
@login_required
def index():
    _require_zentral_admin()
    companies = db.session.query(Company).order_by(Company.created_at.desc()).all()
    return render_template('superadmin/index.html', title='Zentral Admin', companies=companies)


@bp.post('/companies/create')
@login_required
def create_company():
    _require_zentral_admin()
    name = (request.form.get('name') or '').strip()
    plan = (request.form.get('plan') or 'standard').strip() or 'standard'
    slug = (request.form.get('slug') or '').strip().lower()
    if not name:
        flash('Nombre inválido.', 'error')
        return redirect(url_for('superadmin.index'))
    if not slug:
        slug = _slugify(name)

    existing = db.session.query(Company).filter(Company.slug == slug).first()
    if existing:
        flash('Ya existe una empresa con ese slug.', 'error')
        return redirect(url_for('superadmin.index'))

    c = Company(name=name, slug=slug, plan=plan, status='active')
    db.session.add(c)
    db.session.commit()

    flash('Empresa creada.', 'success')
    return redirect(url_for('superadmin.index'))


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

        role = (request.form.get('role') or '').strip() or (u.role or 'vendedor')
        u.role = role

        try:
            u.level = int(request.form.get('level') or u.level or 2)
        except Exception:
            pass

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

    roles = ['company_admin', 'admin', 'vendedor', 'contador']
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
