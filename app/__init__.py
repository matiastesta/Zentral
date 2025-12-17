import os
from datetime import datetime
from flask import Flask
from flask_login import LoginManager
from flask_babel import Babel
from flask_bootstrap import Bootstrap
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import inspect, text
from config import Config

login_manager = LoginManager()
login_manager.login_view = 'auth.login'
login_manager.login_message_category = 'info'
babel = Babel()
bootstrap = Bootstrap()
db = SQLAlchemy()


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    # Inicializar extensiones que no dependen de base de datos
    login_manager.init_app(app)
    babel.init_app(app)
    bootstrap.init_app(app)
    db.init_app(app)

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

    @app.context_processor
    def inject_now():
        """Inyecta la variable 'now' en todas las plantillas Jinja."""
        return {"now": datetime.utcnow()}

    @app.context_processor
    def inject_business():
        try:
            from app.models import BusinessSettings
            return {"business": BusinessSettings.get_singleton()}
        except Exception:
            return {"business": None}

    with app.app_context():
        db.create_all()
        try:
            from app.models import User, BusinessSettings
            # Lightweight schema upgrade for SQLite (create_all doesn't add columns)
            try:
                if str(db.engine.url.drivername) == 'sqlite':
                    insp = inspect(db.engine)

                    def ensure_columns(table, cols):
                        if not insp.has_table(table):
                            return
                        existing = {c['name'] for c in insp.get_columns(table)}
                        for name, coltype in cols:
                            if name in existing:
                                continue
                            db.session.execute(text(f'ALTER TABLE {table} ADD COLUMN {name} {coltype}'))

                    ensure_columns('business_settings', [
                        ('industry', 'VARCHAR(255)'),
                        ('email', 'VARCHAR(255)'),
                        ('phone', 'VARCHAR(64)'),
                        ('address', 'VARCHAR(255)'),
                        ('logo_filename', 'VARCHAR(255)'),
                    ])
                    ensure_columns('user', [
                        ('role', 'VARCHAR(32)'),
                        ('permissions_json', 'TEXT'),
                        ('is_master', 'BOOLEAN'),
                        ('active', 'BOOLEAN'),
                        ('email', 'VARCHAR(255)'),
                        ('password_hash', 'VARCHAR(255)'),
                    ])
                    db.session.commit()
            except Exception:
                db.session.rollback()

            # Ensure singleton business row exists (after schema upgrade)
            try:
                BusinessSettings.get_singleton()
            except Exception:
                pass

            if db.session.query(User).count() == 0:
                admin = User(username='admin', email='admin@local', role='admin', is_master=False)
                admin.set_password(os.environ.get('INITIAL_ADMIN_PASSWORD') or 'admin')
                admin.set_permissions_all(True)
                db.session.add(admin)

                master = User(username='zentra', email='support@zentra.local', role='admin', is_master=True)
                master.set_password(os.environ.get('ZENTRA_MASTER_PASSWORD') or 'zentra')
                master.set_permissions_all(True)
                db.session.add(master)

                db.session.commit()
        except Exception:
            db.session.rollback()

    return app


@login_manager.user_loader
def load_user(user_id):
    """Cargador de usuario temporal sin base de datos.

    Siempre devuelve None, indicando que no hay usuarios persistidos.
    Esto es suficiente para que Flask-Login funcione sin romper.
    """
    try:
        from app.models import User
        return db.session.get(User, int(user_id))
    except Exception:
        return None
