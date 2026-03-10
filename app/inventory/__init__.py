from flask import Blueprint

bp = Blueprint('inventory', __name__)

from app.inventory import routes
from app.inventory import routes_tanda_advanced
from app.inventory import routes_tanda_update
