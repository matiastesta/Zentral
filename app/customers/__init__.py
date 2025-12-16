from flask import Blueprint

bp = Blueprint('customers', __name__)

from app.customers import routes
