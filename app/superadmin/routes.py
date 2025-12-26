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


@bp.post('/companies/<company_id>/reset-data')
@login_required
def reset_company_data(company_id: str):
    _require_zentral_admin()
    c = db.session.get(Company, str(company_id))
    if not c:
        flash('Empresa inválida.', 'error')
        return redirect(url_for('superadmin.index'))

    cid = str(c.id)

    # Delete operational tables but keep company, users and business settings.
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

    db.session.commit()

    flash(f'Datos operativos reiniciados para {c.name}.', 'success')
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


@bp.route('/companies/reset-data-all', methods=['POST'])
@login_required
def reset_all_companies_data():
    _require_zentral_admin()

    session.pop('impersonate_company_id', None)

    # Delete operational tables for all companies but keep company, users and business settings.
    db.session.query(SaleItem).delete(synchronize_session=False)
    db.session.query(Sale).delete(synchronize_session=False)
    db.session.query(InventoryMovement).delete(synchronize_session=False)
    db.session.query(InventoryLot).delete(synchronize_session=False)
    db.session.query(Expense).delete(synchronize_session=False)
    db.session.query(ExpenseCategory).delete(synchronize_session=False)
    db.session.query(CalendarEvent).delete(synchronize_session=False)
    db.session.query(CalendarUserConfig).delete(synchronize_session=False)
    db.session.query(CashCount).delete(synchronize_session=False)
    db.session.query(Product).delete(synchronize_session=False)
    db.session.query(Category).delete(synchronize_session=False)
    db.session.query(Customer).delete(synchronize_session=False)
    db.session.query(Supplier).delete(synchronize_session=False)
    db.session.query(Employee).delete(synchronize_session=False)

    db.session.commit()
    flash('Datos operativos reiniciados para TODAS las empresas.', 'success')
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


@bp.post('/companies/<company_id>/users/create')
@login_required
def company_users_create(company_id: str):
    _require_zentral_admin()
    c = db.session.get(Company, str(company_id))
    if not c:
        flash('Empresa inválida.', 'error')
        return redirect(url_for('superadmin.index'))

    email = (request.form.get('email') or '').strip().lower()
    display_name = (request.form.get('display_name') or '').strip()
    username = (request.form.get('username') or '').strip()
    password = (request.form.get('password') or '').strip()
    role = (request.form.get('role') or 'company_admin').strip() or 'company_admin'
    try:
        level = int(request.form.get('level') or 2)
    except Exception:
        level = 2

    if not password or not username:
        flash('Completá usuario y contraseña.', 'error')
        return redirect(url_for('superadmin.company_users', company_id=c.id))

    if db.session.query(User).filter(User.username == username, User.company_id == str(c.id)).first():
        flash('El usuario ya existe en esta empresa.', 'error')
        return redirect(url_for('superadmin.company_users', company_id=c.id))

    if email and db.session.query(User).filter(User.email == email).first():
        flash('Ese email ya existe.', 'error')
        return redirect(url_for('superadmin.company_users', company_id=c.id))

    u = User(email=(email or None), display_name=(display_name or None), username=username, role=role, company_id=str(c.id), is_master=False, active=True, level=level)
    u.set_password(password)
    u.set_permissions_all(True)
    db.session.add(u)
    db.session.commit()

    flash('Usuario creado.', 'success')
    return redirect(url_for('superadmin.company_users', company_id=c.id))


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
