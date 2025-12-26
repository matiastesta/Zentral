from flask import Blueprint

bp = Blueprint('superadmin', __name__)

from app.superadmin import routes
