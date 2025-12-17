from flask import render_template
from flask_login import login_required

from app.permissions import module_required
from app.reports import bp


@bp.route("/")
@bp.route("/index")
@login_required
@module_required('reports')
def index():
    """Vista general de reportes (dummy)."""
    return render_template("reports/index.html", title="Reportes")
