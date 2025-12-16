from flask import render_template
from app.settings import bp


@bp.route("/")
@bp.route("/business")
def business_settings():
    """Configuración básica del negocio (dummy)."""
    return render_template("settings/business.html", title="Configuración del negocio")
