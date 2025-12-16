from flask import render_template
from app.main import bp


@bp.route('/')
@bp.route('/index')
def index():
    """Dashboard temporal sin acceso a base de datos.

    Muestra métricas en cero y listas vacías hasta que se implemente
    una nueva capa de persistencia.
    """
    today_revenue = 0
    weekly_revenue = 0
    monthly_revenue = 0
    recent_sales = []
    low_stock_products = []

    return render_template(
        'main/index.html',
        title='Dashboard',
        today_revenue=today_revenue,
        weekly_revenue=weekly_revenue,
        monthly_revenue=monthly_revenue,
        recent_sales=recent_sales,
        low_stock_products=low_stock_products,
    )
