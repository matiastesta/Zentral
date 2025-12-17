"""Modelos dummy sin base de datos.

Este módulo define stubs mínimos para que el resto de la aplicación pueda
importar `User`, `Product`, etc. sin depender de SQLAlchemy ni de una base
de datos real. Más adelante se puede reemplazar por otra capa de datos.
"""

import json
from datetime import datetime

from flask_login import UserMixin
from werkzeug.security import check_password_hash, generate_password_hash

from app import db


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(255), nullable=True)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(32), nullable=False, default='vendedor')
    permissions_json = db.Column(db.Text, nullable=False, default='{}')
    is_master = db.Column(db.Boolean, nullable=False, default=False)
    active = db.Column(db.Boolean, nullable=False, default=True)

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password or '')

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
        if self.role == 'admin':
            return True
        perms = self.get_permissions()
        return bool(perms.get(str(module_name), False))


class BusinessSettings(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False, default='Nombre del negocio')
    industry = db.Column(db.String(255), nullable=True)
    email = db.Column(db.String(255), nullable=True)
    phone = db.Column(db.String(64), nullable=True)
    address = db.Column(db.String(255), nullable=True)
    logo_filename = db.Column(db.String(255), nullable=True)

    @staticmethod
    def get_singleton():
        bs = db.session.get(BusinessSettings, 1)
        if bs:
            return bs
        bs = BusinessSettings(id=1, name='Nombre del negocio')
        db.session.add(bs)
        db.session.commit()
        return bs


class CalendarEvent(db.Model):
    id = db.Column(db.Integer, primary_key=True)
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


class CalendarUserConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False, unique=True, index=True)
    config_json = db.Column(db.Text, nullable=False, default='{}')
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def get_config(self) -> dict:
        try:
            parsed = json.loads(self.config_json or '{}')
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def set_config(self, cfg: dict) -> None:
        payload = cfg if isinstance(cfg, dict) else {}
        self.config_json = json.dumps(payload, ensure_ascii=False)
