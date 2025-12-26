import os
from datetime import datetime
from flask import Flask, g
from flask_login import LoginManager
from flask_babel import Babel
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from config import Config

login_manager = LoginManager()
login_manager.login_view = 'auth.login'
login_manager.login_message_category = 'info'
babel = Babel()
db = SQLAlchemy()
migrate = Migrate()


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    try:
        debug_env = str(os.environ.get('APP_DEBUG') or '').strip().lower() in ('1', 'true', 'yes', 'on')
        reload_env = str(os.environ.get('APP_RELOAD') or '').strip().lower() in ('1', 'true', 'yes', 'on')
        flask_debug_env = str(os.environ.get('FLASK_DEBUG') or '').strip().lower() in ('1', 'true', 'yes', 'on')
        flask_env = str(os.environ.get('FLASK_ENV') or '').strip().lower()
        is_dev = bool(app.config.get('DEBUG')) or str(app.config.get('ENV') or '').strip().lower() == 'development' or flask_env == 'development'
        if debug_env or reload_env or flask_debug_env or is_dev:
            app.config['TEMPLATES_AUTO_RELOAD'] = True
            app.jinja_env.auto_reload = True
            app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
    except Exception:
        app.logger.exception('Failed to apply dev reload configuration')

    # Inicializar extensiones que no dependen de base de datos
    login_manager.init_app(app)
    babel.init_app(app)
    db.init_app(app)
    migrate.init_app(app, db)

    try:
        with app.app_context():
            if str(db.engine.url.drivername).startswith('sqlite'):
                from app.rls import bootstrap_schema

                bootstrap_schema(reset=False)
    except Exception:
        app.logger.exception('Failed to bootstrap SQLite schema')

    try:
        from app.db_context import configure_sqlite_tenant_guards

        configure_sqlite_tenant_guards()
    except Exception:
        app.logger.exception('Failed to configure SQLite tenant guards')

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
    from app.calendar import bp as calendar_bp
    from app.superadmin import bp as superadmin_bp

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
    app.register_blueprint(calendar_bp, url_prefix='/calendar')
    app.register_blueprint(superadmin_bp, url_prefix='/superadmin')

    @app.before_request
    def _apply_tenant_context():
        try:
            from app.db_context import apply_rls_context

            apply_rls_context(is_login=False)
        except Exception:
            try:
                db.session.rollback()
            except Exception:
                pass
            app.logger.exception('Failed to apply tenant context')

    @app.context_processor
    def inject_now():
        """Inyecta la variable 'now' en todas las plantillas Jinja."""
        return {"now": datetime.utcnow()}

    @app.context_processor
    def inject_business():
        try:
            from app.models import BusinessSettings
            cid = str(getattr(g, 'company_id', '') or '').strip()
            return {"business": BusinessSettings.get_for_company(cid) if cid else None}
        except Exception:
            app.logger.exception('Failed to inject business settings')
            return {"business": None}

    @app.context_processor
    def inject_support_mode():
        try:
            from app.tenancy import is_impersonating

            return {"is_support_mode": bool(is_impersonating())}
        except Exception:
            return {"is_support_mode": False}

    @app.cli.group('zentral')
    def zentral_cli():
        pass

    @zentral_cli.command('bootstrap')
    def zentral_bootstrap():
        from app.rls import bootstrap_schema

        bootstrap_schema(reset=False)

    @zentral_cli.command('reset-db')
    def zentral_reset_db():
        reset_flag = str(os.environ.get('ZENTRAL_RESET_DB') or '').strip().lower() in {'1', 'true', 'yes', 'on'}
        reset_confirm = str(os.environ.get('ZENTRAL_RESET_DB_CONFIRM') or '').strip().upper() == 'YES'
        if not (reset_flag and reset_confirm):
            raise RuntimeError('Reset blocked: set ZENTRAL_RESET_DB=1 and ZENTRAL_RESET_DB_CONFIRM=YES')
        from app.rls import bootstrap_schema

        bootstrap_schema(reset=True)

    return app


@login_manager.user_loader
def load_user(user_id):
    """Cargador de usuario temporal sin base de datos.

    Siempre devuelve None, indicando que no hay usuarios persistidos.
    Esto es suficiente para que Flask-Login funcione sin romper.
    """
    try:
        try:
            from app.db_context import apply_rls_context

            apply_rls_context(is_login=False)
        except Exception:
            try:
                db.session.rollback()
            except Exception:
                pass
            pass
        from app.models import User
        return db.session.get(User, int(user_id))
    except Exception:
        return None
