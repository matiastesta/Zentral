from flask import render_template
from app.reports import bp


@bp.route("/")
@bp.route("/index")
def index():
    """Vista general de reportes (dummy)."""
    return render_template("reports/index.html", title="Reportes")
