"""Modelos dummy sin base de datos.

Este módulo define stubs mínimos para que el resto de la aplicación pueda
importar `User`, `Product`, etc. sin depender de SQLAlchemy ni de una base
de datos real. Más adelante se puede reemplazar por otra capa de datos.
"""

import json
import uuid
from datetime import datetime

from flask_login import UserMixin
from flask import current_app
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError
from werkzeug.security import check_password_hash, generate_password_hash

from app import db


def _default_company_id():
    try:
        from flask import g

        return str(getattr(g, 'company_id', '') or '').strip() or None
    except Exception:
        return None


def _ensure_business_settings_columns() -> None:
    try:
        engine = db.engine
        if str(engine.url.drivername).startswith('sqlite'):
            return

        insp = inspect(engine)
        if 'business_settings' not in set(insp.get_table_names() or []):
            return

        cols = {str(c.get('name') or '') for c in (insp.get_columns('business_settings') or [])}
        if 'habilitar_sistema_cuotas' in cols:
            return

        with engine.begin() as conn:
            conn.execute(
                text(
                    'ALTER TABLE business_settings ADD COLUMN IF NOT EXISTS habilitar_sistema_cuotas BOOLEAN NOT NULL DEFAULT FALSE'
                )
            )
    except Exception:
        try:
            current_app.logger.exception('Failed to ensure business_settings columns')
        except Exception:
            pass


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
    pause_scheduled_for = db.Column(db.Date, nullable=True)
    subscription_ends_at = db.Column(db.Date, nullable=True)
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
            'cash_withdrawals': val,
            'can_cash_withdrawal': val,
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
        perms = self.get_permissions()
        key = str(module_name or '').strip()
        if key in {'can_cash_withdrawal', 'cash_withdrawals'}:
            return bool(perms.get('can_cash_withdrawal', False) or perms.get('cash_withdrawals', False))
        return bool(perms.get(key, False))


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
    logo_file_id = db.Column(db.String(64), nullable=True, index=True)
    label_customers = db.Column(db.String(64), nullable=True)
    label_products = db.Column(db.String(64), nullable=True)
    primary_color = db.Column(db.String(16), nullable=True)

    background_image_filename = db.Column(db.String(255), nullable=True)
    background_file_id = db.Column(db.String(64), nullable=True, index=True)
    background_brightness = db.Column(db.Float, nullable=True)
    background_contrast = db.Column(db.Float, nullable=True)

    habilitar_sistema_cuotas = db.Column(db.Boolean, nullable=False, default=False)

    insight_margin_delta_pp = db.Column(db.Float, nullable=True)
    insight_profitability_delta_pp = db.Column(db.Float, nullable=True)
    insight_expenses_ratio_pct = db.Column(db.Float, nullable=True)

    @staticmethod
    def get_for_company(company_id: str):
        cid = str(company_id or '').strip()
        if not cid:
            return None
        _ensure_business_settings_columns()
        bs = db.session.query(BusinessSettings).filter(BusinessSettings.company_id == cid).first()
        if bs:
            return bs
        bs = BusinessSettings(company_id=cid, name='Nombre del negocio')
        db.session.add(bs)
        try:
            db.session.flush()
            return bs
        except IntegrityError:
            try:
                db.session.rollback()
            except Exception:
                pass
            return db.session.query(BusinessSettings).filter(BusinessSettings.company_id == cid).first()


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

    __table_args__ = (
        db.UniqueConstraint('company_id', 'internal_code', name='uq_product_company_internal_code'),
        db.UniqueConstraint('company_id', 'barcode', name='uq_product_company_barcode'),
    )

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)
    name = db.Column(db.String(255), nullable=False, index=True)
    description = db.Column(db.Text, nullable=True)

    category_id = db.Column(db.Integer, db.ForeignKey('category.id'), nullable=True, index=True)
    category = db.relationship('Category', backref='products')

    sale_price = db.Column(db.Float, nullable=False, default=0.0)
    internal_code = db.Column(db.String(64), nullable=True, index=True)
    barcode = db.Column(db.String(64), nullable=True, index=True)

    image_filename = db.Column(db.String(255), nullable=True)
    image_file_id = db.Column(db.String(64), nullable=True, index=True)

    unit_name = db.Column(db.String(32), nullable=True)
    uses_lots = db.Column(db.Boolean, nullable=False, default=True)
    stock_ilimitado = db.Column(db.Boolean, nullable=False, default=False)
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
    ticket_number = db.Column(db.Integer, nullable=True, index=True)
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

    is_installments = db.Column(db.Boolean, nullable=False, default=False)

    customer_id = db.Column(db.String(64), nullable=True)
    customer_name = db.Column(db.String(255), nullable=True)

    employee_id = db.Column(db.String(64), nullable=True)
    employee_name = db.Column(db.String(255), nullable=True)

    exchange_return_total = db.Column(db.Float, nullable=True)
    exchange_new_total = db.Column(db.Float, nullable=True)

    created_by_user_id = db.Column(db.Integer, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('company_id', 'ticket', name='uq_sale_company_ticket'),
        db.UniqueConstraint('company_id', 'ticket_number', name='uq_sale_company_ticket_number'),
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


class InstallmentPlan(db.Model):
    __tablename__ = 'installment_plan'

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)

    sale_id = db.Column(db.Integer, db.ForeignKey('sale.id'), nullable=False, index=True)
    sale_ticket = db.Column(db.String(32), nullable=True, index=True)

    customer_id = db.Column(db.String(64), nullable=False, index=True)
    customer_name = db.Column(db.String(255), nullable=True)

    start_date = db.Column(db.Date, nullable=False, index=True)
    interval_days = db.Column(db.Integer, nullable=False, default=30)
    installments_count = db.Column(db.Integer, nullable=False, default=1)

    is_indefinite = db.Column(db.Boolean, nullable=False, default=False)
    amount_per_period = db.Column(db.Float, nullable=False, default=0.0)
    mode = db.Column(db.String(16), nullable=False, default='fixed')

    total_amount = db.Column(db.Float, nullable=False, default=0.0)
    installment_amount = db.Column(db.Float, nullable=False, default=0.0)
    first_payment_method = db.Column(db.String(32), nullable=True)

    status = db.Column(db.String(16), nullable=False, default='activo')
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    sale = db.relationship('Sale', backref='installment_plan', lazy=True)


class Installment(db.Model):
    __tablename__ = 'installment'

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)

    plan_id = db.Column(db.Integer, db.ForeignKey('installment_plan.id'), nullable=False, index=True)
    installment_number = db.Column(db.Integer, nullable=False)

    due_date = db.Column(db.Date, nullable=False, index=True)
    amount = db.Column(db.Float, nullable=False, default=0.0)
    status = db.Column(db.String(16), nullable=False, default='pendiente')

    paid_at = db.Column(db.DateTime, nullable=True)
    paid_payment_method = db.Column(db.String(32), nullable=True)
    paid_sale_id = db.Column(db.Integer, db.ForeignKey('sale.id'), nullable=True, index=True)

    plan = db.relationship('InstallmentPlan', backref='installments', lazy=True)


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
    efectivo_calculado_snapshot = db.Column(db.Float, nullable=True)
    cash_expected_at_save = db.Column(db.Float, nullable=True)
    last_cash_event_at_save = db.Column(db.DateTime, nullable=True)
    status = db.Column(db.String(16), nullable=False, default='draft')
    done_at = db.Column(db.DateTime, nullable=True)
    closing_amount = db.Column(db.Float, nullable=False, default=0.0)
    difference_amount = db.Column(db.Float, nullable=False, default=0.0)
    created_by_user_id = db.Column(db.Integer, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('company_id', 'count_date', name='uq_cash_count_company_date'),
    )


class CashWithdrawal(db.Model):
    __tablename__ = 'cash_withdrawals'

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)

    fecha_imputacion = db.Column(db.Date, nullable=False, index=True)
    fecha_registro = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    monto = db.Column(db.Float, nullable=False, default=0.0)
    nota = db.Column(db.Text, nullable=True)

    usuario_registro_id = db.Column(db.Integer, nullable=True)
    usuario_responsable_id = db.Column(db.Integer, nullable=True)

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        db.Index('ix_cash_withdrawals_company_imputacion', 'company_id', 'fecha_imputacion'),
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


class FileAsset(db.Model):
    __tablename__ = 'file_asset'

    id = db.Column(db.String(64), primary_key=True)
    company_id = db.Column(db.String(36), nullable=False, index=True, default=_default_company_id)

    entity_type = db.Column(db.String(32), nullable=True, index=True)
    entity_id = db.Column(db.String(64), nullable=True, index=True)

    storage_provider = db.Column(db.String(16), nullable=False, default='r2')
    bucket = db.Column(db.String(128), nullable=True)
    object_key = db.Column(db.String(512), nullable=True, index=True)

    original_name = db.Column(db.String(255), nullable=True)
    content_type = db.Column(db.String(128), nullable=True)
    size_bytes = db.Column(db.Integer, nullable=False, default=0)

    checksum_sha256 = db.Column(db.String(64), nullable=True)
    etag = db.Column(db.String(128), nullable=True)

    status = db.Column(db.String(16), nullable=False, default='active')
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('company_id', 'storage_provider', 'bucket', 'object_key', name='uq_file_asset_company_object'),
    )
