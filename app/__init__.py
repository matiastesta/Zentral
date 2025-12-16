import os
from datetime import datetime
from flask import Flask
from flask_login import LoginManager
from flask_babel import Babel
from flask_bootstrap import Bootstrap
from config import Config

login_manager = LoginManager()
login_manager.login_view = 'auth.login'
login_manager.login_message_category = 'info'
babel = Babel()
bootstrap = Bootstrap()


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    # Inicializar extensiones que no dependen de base de datos
    login_manager.init_app(app)
    babel.init_app(app)
    bootstrap.init_app(app)

    # Registrar blueprints principales
    from app.auth import bp as auth_bp
    from app.main import bp as main_bp
    from app.products import bp as products_bp
    from app.sales import bp as sales_bp
    from app.customers import bp as customers_bp
    from app.expenses import bp as expenses_bp
    from app.reports import bp as reports_bp
    from app.settings import bp as settings_bp
    from app.inventory import bp as inventory_bp
    from app.movements import bp as movements_bp
    from app.suppliers import bp as suppliers_bp
    from app.employees import bp as employees_bp
    from app.user_settings import bp as user_settings_bp

    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(main_bp)
    app.register_blueprint(products_bp, url_prefix='/products')
    app.register_blueprint(sales_bp, url_prefix='/sales')
    app.register_blueprint(customers_bp, url_prefix='/customers')
    app.register_blueprint(expenses_bp, url_prefix='/expenses')
    app.register_blueprint(reports_bp, url_prefix='/reports')
    app.register_blueprint(settings_bp, url_prefix='/settings')
    app.register_blueprint(inventory_bp, url_prefix='/inventory')
    app.register_blueprint(movements_bp, url_prefix='/movements')
    app.register_blueprint(suppliers_bp, url_prefix='/suppliers')
    app.register_blueprint(employees_bp, url_prefix='/employees')
    app.register_blueprint(user_settings_bp, url_prefix='/user-settings')

    @app.context_processor
    def inject_now():
        """Inyecta la variable 'now' en todas las plantillas Jinja."""
        return {"now": datetime.utcnow()}

    return app


@login_manager.user_loader
def load_user(user_id):
    """Cargador de usuario temporal sin base de datos.

    Siempre devuelve None, indicando que no hay usuarios persistidos.
    Esto es suficiente para que Flask-Login funcione sin romper.
    """
    return None
