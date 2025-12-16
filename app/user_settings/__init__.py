from flask import Blueprint

bp = Blueprint('user_settings', __name__)

from app.user_settings import routes
