import os
from datetime import datetime
from flask import Flask
from flask_login import LoginManager
from flask_babel import Babel
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from sqlalchemy import inspect, text
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
            app.logger.exception('Failed to inject business settings')
            return {"business": None}

    with app.app_context():
        try:
            if str(db.engine.url.drivername) == 'sqlite':
                db.create_all()

                from app.models import User, BusinessSettings

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
                        ('label_customers', 'VARCHAR(64)'),
                        ('label_products', 'VARCHAR(64)'),
                        ('primary_color', 'VARCHAR(16)'),
                        ('insight_margin_delta_pp', 'FLOAT'),
                        ('insight_profitability_delta_pp', 'FLOAT'),
                        ('insight_expenses_ratio_pct', 'FLOAT'),
                    ])
                ensure_columns('user', [
                        ('role', 'VARCHAR(32)'),
                        ('permissions_json', 'TEXT'),
                        ('is_master', 'BOOLEAN'),
                        ('active', 'BOOLEAN'),
                        ('email', 'VARCHAR(255)'),
                        ('password_hash', 'VARCHAR(255)'),
                    ])

                ensure_columns('product', [
                        ('description', 'TEXT'),
                        ('category_id', 'INTEGER'),
                        ('sale_price', 'FLOAT'),
                        ('internal_code', 'VARCHAR(64)'),
                        ('barcode', 'VARCHAR(64)'),
                        ('image_filename', 'VARCHAR(255)'),
                        ('unit_name', 'VARCHAR(32)'),
                        ('uses_lots', 'BOOLEAN'),
                        ('method', 'VARCHAR(16)'),
                        ('min_stock', 'FLOAT'),
                        ('reorder_point', 'FLOAT'),
                        ('primary_supplier_id', 'VARCHAR(64)'),
                        ('primary_supplier_name', 'VARCHAR(255)'),
                        ('active', 'BOOLEAN'),
                        ('created_at', 'DATETIME'),
                        ('updated_at', 'DATETIME'),
                    ])

                ensure_columns('category', [
                    ('active', 'BOOLEAN'),
                ])

                ensure_columns('inventory_lot', [
                    ('qty_initial', 'FLOAT'),
                    ('qty_available', 'FLOAT'),
                    ('unit_cost', 'FLOAT'),
                    ('received_at', 'DATETIME'),
                    ('supplier_id', 'VARCHAR(64)'),
                    ('supplier_name', 'VARCHAR(255)'),
                    ('expiration_date', 'DATE'),
                    ('lot_code', 'VARCHAR(64)'),
                    ('note', 'TEXT'),
                    ('origin_sale_ticket', 'VARCHAR(32)'),
                    ('created_at', 'DATETIME'),
                    ('updated_at', 'DATETIME'),
                ])

                ensure_columns('inventory_movement', [
                    ('movement_date', 'DATE'),
                    ('created_at', 'DATETIME'),
                    ('type', 'VARCHAR(16)'),
                    ('sale_ticket', 'VARCHAR(32)'),
                    ('product_id', 'INTEGER'),
                    ('lot_id', 'INTEGER'),
                    ('qty_delta', 'FLOAT'),
                    ('unit_cost', 'FLOAT'),
                    ('total_cost', 'FLOAT'),
                ])

                ensure_columns('customer', [
                    ('first_name', 'VARCHAR(255)'),
                    ('last_name', 'VARCHAR(255)'),
                    ('name', 'VARCHAR(255)'),
                    ('email', 'VARCHAR(255)'),
                    ('phone', 'VARCHAR(64)'),
                    ('birthday', 'DATE'),
                    ('address', 'VARCHAR(255)'),
                    ('notes', 'TEXT'),
                    ('status', 'VARCHAR(32)'),
                    ('created_at', 'DATETIME'),
                    ('updated_at', 'DATETIME'),
                ])

                ensure_columns('sale', [
                    ('is_gift', 'BOOLEAN'),
                    ('gift_code', 'VARCHAR(64)'),
                ])

                ensure_columns('employee', [
                    ('first_name', 'VARCHAR(255)'),
                    ('last_name', 'VARCHAR(255)'),
                    ('name', 'VARCHAR(255)'),
                    ('hire_date', 'DATE'),
                    ('inactive_date', 'DATE'),
                    ('default_payment_method', 'VARCHAR(32)'),
                    ('contract_type', 'VARCHAR(64)'),
                    ('status', 'VARCHAR(16)'),
                    ('role', 'VARCHAR(255)'),
                    ('birth_date', 'DATE'),
                    ('document_id', 'VARCHAR(64)'),
                    ('phone', 'VARCHAR(64)'),
                    ('email', 'VARCHAR(255)'),
                    ('address', 'VARCHAR(255)'),
                    ('reference_salary', 'FLOAT'),
                    ('notes', 'TEXT'),
                    ('active', 'BOOLEAN'),
                    ('created_at', 'DATETIME'),
                    ('updated_at', 'DATETIME'),
                ])

                ensure_columns('expense', [
                    ('expense_date', 'DATE'),
                    ('payment_method', 'VARCHAR(32)'),
                    ('amount', 'FLOAT'),
                    ('description', 'TEXT'),
                    ('category', 'VARCHAR(255)'),
                    ('supplier_id', 'VARCHAR(64)'),
                    ('supplier_name', 'VARCHAR(255)'),
                    ('note', 'TEXT'),
                    ('expense_type', 'VARCHAR(32)'),
                    ('frequency', 'VARCHAR(32)'),
                    ('employee_id', 'VARCHAR(64)'),
                    ('employee_name', 'VARCHAR(255)'),
                    ('period_from', 'DATE'),
                    ('period_to', 'DATE'),
                    ('meta_json', 'TEXT'),
                    ('origin', 'VARCHAR(32)'),
                    ('created_by_user_id', 'INTEGER'),
                    ('created_at', 'DATETIME'),
                    ('updated_at', 'DATETIME'),
                ])

                ensure_columns('supplier', [
                    ('name', 'VARCHAR(255)'),
                    ('supplier_type', 'VARCHAR(32)'),
                    ('status', 'VARCHAR(32)'),
                    ('categories_json', 'TEXT'),
                    ('invoice_type', 'VARCHAR(32)'),
                    ('default_payment_method', 'VARCHAR(64)'),
                    ('payment_terms', 'VARCHAR(64)'),
                    ('contact_person', 'VARCHAR(255)'),
                    ('preferred_contact_channel', 'VARCHAR(32)'),
                    ('phone', 'VARCHAR(64)'),
                    ('email', 'VARCHAR(255)'),
                    ('address', 'VARCHAR(255)'),
                    ('notes', 'TEXT'),
                    ('meta_json', 'TEXT'),
                    ('created_at', 'DATETIME'),
                    ('updated_at', 'DATETIME'),
                ])

                ensure_columns('expense_category', [
                    ('name', 'VARCHAR(255)'),
                    ('created_at', 'DATETIME'),
                    ('updated_at', 'DATETIME'),
                ])
                db.session.commit()

                try:
                    BusinessSettings.get_singleton()
                except Exception:
                    app.logger.exception('Failed to ensure BusinessSettings singleton exists')

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
