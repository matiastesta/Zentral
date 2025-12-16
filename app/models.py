"""Modelos dummy sin base de datos.

Este módulo define stubs mínimos para que el resto de la aplicación pueda
importar `User`, `Product`, etc. sin depender de SQLAlchemy ni de una base
de datos real. Más adelante se puede reemplazar por otra capa de datos.
"""

from dataclasses import dataclass
from typing import Optional
from flask_login import UserMixin


@dataclass
class User(UserMixin):
    id: int
    username: str
    email: str

    def set_password(self, password: str) -> None:
        pass

    def check_password(self, password: str) -> bool:
        return True


@dataclass
class Business:
    id: int
    name: str


@dataclass
class Product:
    id: int
    name: str


@dataclass
class Customer:
    id: int
    first_name: str


@dataclass
class Sale:
    id: int
    total: float


@dataclass
class SaleItem:
    id: int
    quantity: int


@dataclass
class ExpenseCategory:
    id: int
    name: str


@dataclass
class Expense:
    id: int
    amount: float
