"""Modelos dummy sin base de datos.

Este módulo define stubs mínimos para que el resto de la aplicación pueda
importar `User`, `Product`, etc. sin depender de SQLAlchemy ni de una base
de datos real. Más adelante se puede reemplazar por otra capa de datos.
"""

import json
import uuid
from datetime import datetime

from flask_login import UserMixin
from werkzeug.security import check_password_hash, generate_password_hash

from app import db


def _default_company_id():
    try:
        from flask import g

        return str(getattr(g, 'company_id', '') or '').strip() or None
    except Exception:
        return None


class SystemMeta(db.Model):
    __tablename__ = 'system_meta'

    key = db.Column(db.String(64), primary_key=True)
    value = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class Plan(db.Model):
    __tablename__ = 'plan'

    code = db.Column(db.String(64), primary_key=True)
    active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class Company(db.Model):
    __tablename__ = 'company'

    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = db.Column(db.String(255), nullable=False)
    slug = db.Column(db.String(64), nullable=False, unique=True, index=True)
    plan = db.Column(db.String(64), nullable=False, default='standard')
    notes = db.Column(db.Text, nullable=True)
    admin_user_id = db.Column(db.Integer, nullable=True)
    status = db.Column(db.String(16), nullable=False, default='active')
    paused_at = db.Column(db.DateTime, nullable=True)
    pause_reason = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class CompanyRole(db.Model):
    __tablename__ = 'company_role'

    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    name = db.Column(db.String(64), nullable=False)
    permissions_json = db.Column(db.Text, nullable=False, default='{}')
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('company_id', 'name', name='uq_company_role_company_name'),
    )

    def get_permissions(self) -> dict:
        try:
            parsed = json.loads(self.permissions_json or '{}')
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def set_permissions(self, perms: dict) -> None:
        payload = perms if isinstance(perms, dict) else {}
        self.permissions_json = json.dumps(payload, ensure_ascii=False)


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(36), nullable=True, index=True)
    username = db.Column(db.String(80), nullable=False)
    display_name = db.Column(db.String(120), nullable=True)
    email = db.Column(db.String(255), nullable=True, unique=True, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    password_plain = db.Column(db.Text, nullable=True)
    role = db.Column(db.String(32), nullable=False, default='vendedor')
    level = db.Column(db.Integer, nullable=False, default=0)
    created_by_user_id = db.Column(db.Integer, nullable=True)
    permissions_json = db.Column(db.Text, nullable=False, default='{}')
    is_master = db.Column(db.Boolean, nullable=False, default=False)
    active = db.Column(db.Boolean, nullable=False, default=True)

    __table_args__ = (
        db.UniqueConstraint('company_id', 'username', name='uq_user_company_username'),
    )

    def set_password(self, password: str) -> None:
        raw = password or ''
        self.password_hash = generate_password_hash(raw)
        try:
            self.password_plain = raw
        except Exception:
            pass

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash or '', password or '')

    def get_permissions(self) -> dict:
        try:
            parsed = json.loads(self.permissions_json or '{}')
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def set_permissions(self, perms: dict) -> None:
        payload = perms if isinstance(perms, dict) else {}
        self.permissions_json = json.dumps(payload, ensure_ascii=False)

    def set_permissions_all(self, enabled: bool) -> None:
        val = bool(enabled)
        self.set_permissions({
            'dashboard': val,
            'calendar': val,
            'sales': val,
            'expenses': val,
            'inventory': val,
            'customers': val,
            'suppliers': val,
            'employees': val,
            'movements': val,
            'reports': val,
            'settings': val,
            'user_settings': val,
        })

    def can(self, module_name: str) -> bool:
        if not self.active:
            return False
        if self.is_master:
            return True
        if self.role in {'zentral_admin'}:
            return True

        try:
            cid = str(getattr(self, 'company_id', '') or '').strip()
            if cid:
                role_row = (
                    db.session.query(CompanyRole)
                    .filter(CompanyRole.company_id == cid, CompanyRole.name == str(self.role or '').strip())
                    .first()
                )
                if role_row:
                    perms = role_row.get_permissions()
                    if bool(perms.get(str(module_name), False)):
                        return True
        except Exception:
            pass
        perms = self.get_permissions()
        return bool(perms.get(str(module_name), False))


class BusinessSettings(db.Model):
    __tablename__ = 'business_settings'

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, unique=True, index=True, default=_default_company_id)
    name = db.Column(db.String(255), nullable=False, default='Nombre del negocio')
    industry = db.Column(db.String(255), nullable=True)
    email = db.Column(db.String(255), nullable=True)
    phone = db.Column(db.String(64), nullable=True)
    address = db.Column(db.String(255), nullable=True)
    logo_filename = db.Column(db.String(255), nullable=True)
    label_customers = db.Column(db.String(64), nullable=True)
    label_products = db.Column(db.String(64), nullable=True)
    primary_color = db.Column(db.String(16), nullable=True)

    background_image_filename = db.Column(db.String(255), nullable=True)
    background_brightness = db.Column(db.Float, nullable=True)
    background_contrast = db.Column(db.Float, nullable=True)

    insight_margin_delta_pp = db.Column(db.Float, nullable=True)
    insight_profitability_delta_pp = db.Column(db.Float, nullable=True)
    insight_expenses_ratio_pct = db.Column(db.Float, nullable=True)

    @staticmethod
    def get_for_company(company_id: str):
        cid = str(company_id or '').strip()
        if not cid:
            return None
        bs = db.session.query(BusinessSettings).filter(BusinessSettings.company_id == cid).first()
        if bs:
            return bs
        bs = BusinessSettings(company_id=cid, name='Nombre del negocio')
        db.session.add(bs)
        db.session.commit()
        return bs


class CalendarEvent(db.Model):
    __tablename__ = 'calendar_event'

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text, nullable=True)
    event_date = db.Column(db.Date, nullable=False, index=True)
    priority = db.Column(db.String(16), nullable=False, default='media')
    color = db.Column(db.String(16), nullable=False, default='yellow')
    source_module = db.Column(db.String(32), nullable=False, default='manual')
    event_type = db.Column(db.String(32), nullable=False, default='nota')
    is_system = db.Column(db.Boolean, nullable=False, default=False)
    assigned_user_id = db.Column(db.Integer, nullable=True)
    created_by_user_id = db.Column(db.Integer, nullable=True)
    status = db.Column(db.String(16), nullable=False, default='open')
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class Category(db.Model):
    __tablename__ = 'category'

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    name = db.Column(db.String(255), nullable=False, index=True)
    parent_id = db.Column(db.Integer, db.ForeignKey('category.id'), nullable=True, index=True)

    active = db.Column(db.Boolean, nullable=False, default=True)

    parent = db.relationship('Category', remote_side=[id], backref='children')


class Product(db.Model):
    __tablename__ = 'product'

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    name = db.Column(db.String(255), nullable=False, index=True)
    description = db.Column(db.Text, nullable=True)

    category_id = db.Column(db.Integer, db.ForeignKey('category.id'), nullable=True, index=True)
    category = db.relationship('Category', backref='products')

    sale_price = db.Column(db.Float, nullable=False, default=0.0)
    internal_code = db.Column(db.String(64), nullable=True, unique=True, index=True)
    barcode = db.Column(db.String(64), nullable=True, unique=True, index=True)

    image_filename = db.Column(db.String(255), nullable=True)

    unit_name = db.Column(db.String(32), nullable=True)
    uses_lots = db.Column(db.Boolean, nullable=False, default=True)
    method = db.Column(db.String(16), nullable=False, default='FIFO')
    min_stock = db.Column(db.Float, nullable=False, default=0.0)
    reorder_point = db.Column(db.Float, nullable=False, default=0.0)

    primary_supplier_id = db.Column(db.String(64), nullable=True, index=True)
    primary_supplier_name = db.Column(db.String(255), nullable=True)

    active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class InventoryLot(db.Model):
    __tablename__ = 'inventory_lot'

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    product_id = db.Column(db.Integer, db.ForeignKey('product.id'), nullable=False, index=True)
    product = db.relationship('Product', backref='lots')

    qty_initial = db.Column(db.Float, nullable=False, default=0.0)
    qty_available = db.Column(db.Float, nullable=False, default=0.0)
    unit_cost = db.Column(db.Float, nullable=False, default=0.0)

    received_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    supplier_id = db.Column(db.String(64), nullable=True, index=True)
    supplier_name = db.Column(db.String(255), nullable=True)
    expiration_date = db.Column(db.Date, nullable=True, index=True)
    lot_code = db.Column(db.String(64), nullable=True, index=True)
    note = db.Column(db.Text, nullable=True)
    origin_sale_ticket = db.Column(db.String(32), nullable=True, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class InventoryMovement(db.Model):
    __tablename__ = 'inventory_movement'

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    movement_date = db.Column(db.Date, nullable=False, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    type = db.Column(db.String(16), nullable=False, default='sale')
    sale_ticket = db.Column(db.String(32), nullable=True, index=True)

    product_id = db.Column(db.Integer, db.ForeignKey('product.id'), nullable=False, index=True)
    product = db.relationship('Product', backref='movements')
    lot_id = db.Column(db.Integer, db.ForeignKey('inventory_lot.id'), nullable=True, index=True)
    lot = db.relationship('InventoryLot', backref='movements')

    qty_delta = db.Column(db.Float, nullable=False, default=0.0)
    unit_cost = db.Column(db.Float, nullable=False, default=0.0)
    total_cost = db.Column(db.Float, nullable=False, default=0.0)


class Sale(db.Model):
    __tablename__ = 'sale'

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    ticket = db.Column(db.String(32), nullable=False, index=True)
    sale_date = db.Column(db.Date, nullable=False, index=True)
    sale_type = db.Column(db.String(16), nullable=False, default='Venta')
    status = db.Column(db.String(16), nullable=False, default='Completada')
    payment_method = db.Column(db.String(32), nullable=False, default='Efectivo')
    notes = db.Column(db.Text, nullable=True)

    is_gift = db.Column(db.Boolean, nullable=False, default=False)
    gift_code = db.Column(db.String(64), nullable=True)

    total = db.Column(db.Float, nullable=False, default=0.0)
    discount_general_pct = db.Column(db.Float, nullable=False, default=0.0)
    discount_general_amount = db.Column(db.Float, nullable=False, default=0.0)

    on_account = db.Column(db.Boolean, nullable=False, default=False)
    paid_amount = db.Column(db.Float, nullable=False, default=0.0)
    due_amount = db.Column(db.Float, nullable=False, default=0.0)

    customer_id = db.Column(db.String(64), nullable=True)
    customer_name = db.Column(db.String(255), nullable=True)

    exchange_return_total = db.Column(db.Float, nullable=True)
    exchange_new_total = db.Column(db.Float, nullable=True)

    created_by_user_id = db.Column(db.Integer, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('company_id', 'ticket', name='uq_sale_company_ticket'),
    )

    items = db.relationship('SaleItem', backref='sale', cascade='all, delete-orphan', lazy=True)


class SaleItem(db.Model):
    __tablename__ = 'sale_item'

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    sale_id = db.Column(db.Integer, db.ForeignKey('sale.id'), nullable=False, index=True)
    direction = db.Column(db.String(8), nullable=False, default='out')

    product_id = db.Column(db.String(64), nullable=True)
    product_name = db.Column(db.String(255), nullable=False, default='Producto')
    qty = db.Column(db.Float, nullable=False, default=0.0)
    unit_price = db.Column(db.Float, nullable=False, default=0.0)
    discount_pct = db.Column(db.Float, nullable=False, default=0.0)
    subtotal = db.Column(db.Float, nullable=False, default=0.0)


class CalendarUserConfig(db.Model):
    __tablename__ = 'calendar_user_config'

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    user_id = db.Column(db.Integer, nullable=False, index=True)
    config_json = db.Column(db.Text, nullable=False, default='{}')
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('company_id', 'user_id', name='uq_calendar_user_config_company_user'),
    )

    def get_config(self) -> dict:
        try:
            parsed = json.loads(self.config_json or '{}')
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def set_config(self, cfg: dict) -> None:
        payload = cfg if isinstance(cfg, dict) else {}
        self.config_json = json.dumps(payload, ensure_ascii=False)


class CashCount(db.Model):
    __tablename__ = 'cash_count'

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    count_date = db.Column(db.Date, nullable=False, index=True)
    employee_id = db.Column(db.String(64), nullable=True)
    employee_name = db.Column(db.String(255), nullable=True)
    opening_amount = db.Column(db.Float, nullable=False, default=0.0)
    cash_day_amount = db.Column(db.Float, nullable=False, default=0.0)
    closing_amount = db.Column(db.Float, nullable=False, default=0.0)
    difference_amount = db.Column(db.Float, nullable=False, default=0.0)
    created_by_user_id = db.Column(db.Integer, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('company_id', 'count_date', name='uq_cash_count_company_date'),
    )


class Customer(db.Model):
    __tablename__ = 'customer'

    id = db.Column(db.String(64), primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    first_name = db.Column(db.String(255), nullable=True)
    last_name = db.Column(db.String(255), nullable=True)
    name = db.Column(db.String(255), nullable=True, index=True)
    email = db.Column(db.String(255), nullable=True)
    phone = db.Column(db.String(64), nullable=True)
    birthday = db.Column(db.Date, nullable=True)
    address = db.Column(db.String(255), nullable=True)
    notes = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(32), nullable=False, default='activo')
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class Employee(db.Model):
    __tablename__ = 'employee'

    id = db.Column(db.String(64), primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    first_name = db.Column(db.String(255), nullable=True)
    last_name = db.Column(db.String(255), nullable=True)
    name = db.Column(db.String(255), nullable=True, index=True)
    hire_date = db.Column(db.Date, nullable=True, index=True)
    inactive_date = db.Column(db.Date, nullable=True)
    default_payment_method = db.Column(db.String(32), nullable=True)
    contract_type = db.Column(db.String(64), nullable=True)
    status = db.Column(db.String(16), nullable=False, default='Active')
    role = db.Column(db.String(255), nullable=True)
    birth_date = db.Column(db.Date, nullable=True)
    document_id = db.Column(db.String(64), nullable=True)
    phone = db.Column(db.String(64), nullable=True)
    email = db.Column(db.String(255), nullable=True)
    address = db.Column(db.String(255), nullable=True)
    reference_salary = db.Column(db.Float, nullable=True)
    notes = db.Column(db.Text, nullable=True)
    active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class Expense(db.Model):
    __tablename__ = 'expense'

    id = db.Column(db.String(64), primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    expense_date = db.Column(db.Date, nullable=False, index=True)
    payment_method = db.Column(db.String(32), nullable=False, default='Efectivo')
    amount = db.Column(db.Float, nullable=False, default=0.0)
    description = db.Column(db.Text, nullable=True)
    category = db.Column(db.String(255), nullable=True)
    supplier_id = db.Column(db.String(64), nullable=True)
    supplier_name = db.Column(db.String(255), nullable=True)
    note = db.Column(db.Text, nullable=True)
    expense_type = db.Column(db.String(32), nullable=True)
    frequency = db.Column(db.String(32), nullable=True)
    employee_id = db.Column(db.String(64), nullable=True)
    employee_name = db.Column(db.String(255), nullable=True)
    period_from = db.Column(db.Date, nullable=True)
    period_to = db.Column(db.Date, nullable=True)
    meta_json = db.Column(db.Text, nullable=True)
    origin = db.Column(db.String(32), nullable=False, default='manual')
    created_by_user_id = db.Column(db.Integer, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class Supplier(db.Model):
    __tablename__ = 'supplier'

    id = db.Column(db.String(64), primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    name = db.Column(db.String(255), nullable=False, index=True)
    supplier_type = db.Column(db.String(32), nullable=True)
    status = db.Column(db.String(32), nullable=False, default='Active')
    categories_json = db.Column(db.Text, nullable=True)
    invoice_type = db.Column(db.String(32), nullable=True)
    default_payment_method = db.Column(db.String(64), nullable=True)
    payment_terms = db.Column(db.String(64), nullable=True)
    contact_person = db.Column(db.String(255), nullable=True)
    preferred_contact_channel = db.Column(db.String(32), nullable=True)
    phone = db.Column(db.String(64), nullable=True)
    email = db.Column(db.String(255), nullable=True)
    address = db.Column(db.String(255), nullable=True)
    notes = db.Column(db.Text, nullable=True)
    meta_json = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class ExpenseCategory(db.Model):
    __tablename__ = 'expense_category'

    id = db.Column(db.String(64), primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    name = db.Column(db.String(255), nullable=False, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
