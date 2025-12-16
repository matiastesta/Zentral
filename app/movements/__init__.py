from flask import Blueprint

bp = Blueprint('movements', __name__)

from app.movements import routes
