import os
import sys
import time
import uuid
from datetime import datetime
import re
from flask import Flask, g, jsonify, render_template, request, redirect, session
from flask_login import LoginManager
from flask_babel import Babel
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from config import Config, config

login_manager = LoginManager()
login_manager.login_view = 'auth.login'
login_manager.login_message_category = 'info'
babel = Babel()
db = SQLAlchemy()
migrate = Migrate()


def create_app(config_class=Config):
    if config_class is Config:
        try:
            env_name = str(os.environ.get('APP_ENV') or os.environ.get('FLASK_ENV') or 'default').strip().lower()
            config_class = config.get(env_name) or config.get('default') or Config
        except Exception:
            config_class = Config
    app = Flask(__name__)
    app.config.from_object(config_class)

    try:
        class _TenantPrefixMiddleware:
            def __init__(self, wsgi_app):
                self.wsgi_app = wsgi_app

            def __call__(self, environ, start_response):
                path = str(environ.get('PATH_INFO') or '')
                # Tenant prefix format: /c/<slug>/...
                m = re.match(r'^/c/([^/]+)(/.*)?$', path)
                if m:
                    slug = str(m.group(1) or '').strip().lower()
                    rest = m.group(2) or '/'
                    if not rest.startswith('/'):
                        rest = '/' + rest
                    prefix = '/c/' + slug
                    environ['SCRIPT_NAME'] = prefix
                    environ['PATH_INFO'] = rest
                    environ['ZENTRAL_TENANT_SLUG'] = slug
                return self.wsgi_app(environ, start_response)

        app.wsgi_app = _TenantPrefixMiddleware(app.wsgi_app)
    except Exception:
        app.logger.exception('Failed to apply tenant prefix middleware')

    try:
        is_railway = bool(os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('RAILWAY_PROJECT_ID') or os.environ.get('RAILWAY_SERVICE_ID'))
        if is_railway:
            from werkzeug.middleware.proxy_fix import ProxyFix

            app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
    except Exception:
        app.logger.exception('Failed to apply ProxyFix')

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
                if 'db' in [str(a or '').strip().lower() for a in (sys.argv or [])]:
                    pass
                else:
                    from app.rls import bootstrap_schema

                    bootstrap_schema(reset=False)
    except Exception:
        app.logger.exception('Failed to bootstrap SQLite schema')

    try:
        with app.app_context():
            if not str(db.engine.url.drivername).startswith('sqlite'):
                auto_bootstrap_raw = str(os.environ.get('AUTO_BOOTSTRAP_DB') or '').strip().lower()
                if auto_bootstrap_raw:
                    auto_bootstrap = auto_bootstrap_raw in ('1', 'true', 'yes', 'on')
                else:
                    auto_bootstrap = bool(os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('RAILWAY_PROJECT_ID') or os.environ.get('RAILWAY_SERVICE_ID'))
                if auto_bootstrap:
                    from app.rls import bootstrap_schema

                    # Never reset Postgres schema automatically at startup.
                    # Schema resets are destructive (DROP SCHEMA public CASCADE) and must be
                    # performed only via the explicit CLI command guarded by confirmation.
                    bootstrap_schema(reset=False)
    except Exception:
        app.logger.exception('Failed to bootstrap Postgres schema')

    try:
        from app.db_context import configure_sqlite_tenant_guards

        configure_sqlite_tenant_guards()
    except Exception:
        app.logger.exception('Failed to configure SQLite tenant guards')

    try:
        from app.db_context import configure_session_tenant_context_hooks

        configure_session_tenant_context_hooks()
    except Exception:
        app.logger.exception('Failed to configure session tenant context hooks')

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
    from app.files import bp as files_bp

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
    app.register_blueprint(files_bp)

    def _wants_json() -> bool:
        try:
            path = str(request.path or '')
            if path.startswith('/api/'):
                return True
            if '/api/' in path:
                return True
            accept = request.headers.get('Accept') or ''
            if 'application/json' in accept:
                return True
        except Exception:
            return False
        return False

    @app.errorhandler(401)
    def _err_401(err):
        if _wants_json():
            return jsonify({'ok': False, 'error': 'unauthorized'}), 401
        return render_template('errors/http_error.html', title='Acceso requerido', code=401), 401

    @app.errorhandler(403)
    def _err_403(err):
        if _wants_json():
            return jsonify({'ok': False, 'error': 'forbidden'}), 403
        return render_template('errors/http_error.html', title='Acceso denegado', code=403), 403

    @app.errorhandler(404)
    def _err_404(err):
        if _wants_json():
            return jsonify({'ok': False, 'error': 'not_found'}), 404
        return render_template('errors/http_error.html', title='No encontrado', code=404), 404

    @app.errorhandler(400)
    def _err_400(err):
        if _wants_json():
            try:
                detail = str(getattr(err, 'description', '') or '')
            except Exception:
                detail = ''
            payload = {'ok': False, 'error': 'bad_request'}
            if detail:
                payload['detail'] = detail
            return jsonify(payload), 400
        return render_template('errors/http_error.html', title='Solicitud inválida', code=400), 400

    @app.before_request
    def _request_logging_context():
        try:
            rid = (request.headers.get('X-Request-Id') or request.headers.get('X-Request-ID') or '').strip()
            g.request_id = rid or uuid.uuid4().hex
        except Exception:
            g.request_id = uuid.uuid4().hex
        try:
            g._request_start_ts = time.perf_counter()
        except Exception:
            g._request_start_ts = None

    @app.before_request
    def _enforce_tenant_prefix():
        try:
            if str(session.get('auth_is_zentral_admin') or '') == '1':
                return None

            # Use session-only checks to avoid triggering Flask-Login user loading
            # before tenant context has been applied (can cause RLS to hide the user row).
            cid = str(session.get('auth_company_id') or '').strip()
            if not cid:
                return None

            script_root = ''
            try:
                script_root = str(getattr(request, 'script_root', '') or '').strip()
            except Exception:
                script_root = ''
            slug = str(session.get('auth_company_slug') or '').strip().lower()
            if not slug:
                return None

            # If user is already under /c/<other>, force them back to their tenant prefix.
            if script_root.startswith('/c/'):
                try:
                    m = re.match(r'^/c/([^/]+)$', script_root)
                    current_slug = str(m.group(1) or '').strip().lower() if m else ''
                except Exception:
                    current_slug = ''
                if current_slug == slug:
                    return None
                try:
                    path_info = str(request.environ.get('PATH_INFO') or '/')
                except Exception:
                    path_info = '/'
                qs = ''
                try:
                    qs = str(request.query_string.decode('utf-8') if request.query_string else '')
                except Exception:
                    qs = ''
                dest = '/c/' + slug + (path_info if path_info.startswith('/') else ('/' + path_info))
                if qs:
                    dest = dest + ('&' if '?' in dest else '?') + qs
                return redirect(dest)

            path = str(getattr(request, 'path', '') or '')
            if path.startswith('/c/'):
                return None
            # Keep superadmin on root
            if path.startswith('/superadmin'):
                return None
            # Avoid redirect loops on static
            if path.startswith('/static'):
                return None

            return redirect('/c/' + slug + path)
        except Exception:
            return None

    @app.after_request
    def _log_request(response):
        try:
            start = getattr(g, '_request_start_ts', None)
            dur_ms = None
            if start is not None:
                try:
                    dur_ms = int((time.perf_counter() - float(start)) * 1000)
                except Exception:
                    dur_ms = None

            company_id = str(getattr(g, 'company_id', '') or '').strip()
            imp_company_id = ''
            auth_company_id = ''
            is_admin_flag = ''
            try:
                from flask import session

                imp_company_id = str(session.get('impersonate_company_id') or '').strip()
                auth_company_id = str(session.get('auth_company_id') or '').strip()
                is_admin_flag = str(session.get('auth_is_zentral_admin') or '').strip()
            except Exception:
                imp_company_id = ''
                auth_company_id = ''
                is_admin_flag = ''
            user_id = None
            role = ''
            try:
                from flask_login import current_user

                if getattr(current_user, 'is_authenticated', False):
                    user_id = getattr(current_user, 'id', None)
                    role = str(getattr(current_user, 'role', '') or '')
            except Exception:
                user_id = None
                role = ''

            app.logger.info(
                'REQ id=%s method=%s path=%s status=%s dur_ms=%s company_id=%s auth_company_id=%s imp_company_id=%s admin=%s user_id=%s role=%s',
                str(getattr(g, 'request_id', '') or ''),
                str(getattr(request, 'method', '') or ''),
                str(getattr(request, 'path', '') or ''),
                int(getattr(response, 'status_code', 0) or 0),
                dur_ms if dur_ms is not None else '',
                company_id,
                auth_company_id,
                imp_company_id,
                is_admin_flag,
                user_id if user_id is not None else '',
                role,
            )
        except Exception:
            pass
        return response

    @app.after_request
    def _tenant_cookie_path_isolation(response):
        return response
        try:
            prefix = str(request.script_root or '').strip()
        except Exception:
            prefix = ''
        if not prefix or not prefix.startswith('/c/'):
            try:
                slug = str(session.get('auth_company_slug') or '').strip().lower()
            except Exception:
                slug = ''
            if slug:
                prefix = '/c/' + slug
        if not prefix or not prefix.startswith('/c/'):
            return response
        try:
            cookies = response.headers.getlist('Set-Cookie')
        except Exception:
            cookies = []
        if not cookies:
            return response
        patched = []
        for c in cookies:
            s = str(c or '')
            if not s:
                continue
            if 'Path=' in s:
                s = re.sub(r'(?i)\bPath=[^;]*', 'Path=' + prefix, s)
            else:
                s = s + '; Path=' + prefix
            patched.append(s)
        try:
            del response.headers['Set-Cookie']
        except Exception:
            pass
        for s in patched:
            try:
                response.headers.add('Set-Cookie', s)
            except Exception:
                pass

        # Clear root-path cookies to avoid a global cookie overriding tenant-scoped cookies.
        try:
            session_cookie = str(current_app.config.get('SESSION_COOKIE_NAME') or 'session').strip() or 'session'
        except Exception:
            session_cookie = 'session'
        try:
            remember_cookie = str(current_app.config.get('REMEMBER_COOKIE_NAME') or 'remember_token').strip() or 'remember_token'
        except Exception:
            remember_cookie = 'remember_token'
        try:
            response.headers.add('Set-Cookie', f"{session_cookie}=; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Max-Age=0; Path=/")
        except Exception:
            pass
        try:
            response.headers.add('Set-Cookie', f"{remember_cookie}=; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Max-Age=0; Path=/")
        except Exception:
            pass
        return response

    @app.teardown_request
    def _log_exception(err):
        if err is None:
            return
        try:
            db.session.rollback()
        except Exception:
            pass
        try:
            company_id = str(getattr(g, 'company_id', '') or '').strip()
            imp_company_id = ''
            auth_company_id = ''
            is_admin_flag = ''
            try:
                from flask import session

                imp_company_id = str(session.get('impersonate_company_id') or '').strip()
                auth_company_id = str(session.get('auth_company_id') or '').strip()
                is_admin_flag = str(session.get('auth_is_zentral_admin') or '').strip()
            except Exception:
                imp_company_id = ''
                auth_company_id = ''
                is_admin_flag = ''
            user_id = None
            role = ''
            try:
                from flask_login import current_user

                if getattr(current_user, 'is_authenticated', False):
                    user_id = getattr(current_user, 'id', None)
                    role = str(getattr(current_user, 'role', '') or '')
            except Exception:
                user_id = None
                role = ''
            app.logger.exception(
                'REQ_ERROR id=%s method=%s path=%s company_id=%s auth_company_id=%s imp_company_id=%s admin=%s user_id=%s role=%s',
                str(getattr(g, 'request_id', '') or ''),
                str(getattr(request, 'method', '') or ''),
                str(getattr(request, 'path', '') or ''),
                company_id,
                auth_company_id,
                imp_company_id,
                is_admin_flag,
                user_id if user_id is not None else '',
                role,
            )
        except Exception:
            pass

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

    @app.before_request
    def _guard_broken_login_state():
        # If Flask-Login thinks there's a user but RLS/tenant settings hide that row,
        # accessing current_user attributes can raise ObjectDeletedError.
        # In that case, force logout and redirect to the correct tenant login.
        try:
            from flask_login import current_user, logout_user

            if not getattr(current_user, 'is_authenticated', False):
                return None
            _ = str(getattr(current_user, 'role', '') or '')
            return None
        except Exception:
            try:
                from flask_login import logout_user

                logout_user()
            except Exception:
                pass
            try:
                session.pop('auth_company_id', None)
                session.pop('auth_company_slug', None)
                session.pop('auth_is_zentral_admin', None)
                session.pop('impersonate_company_id', None)
            except Exception:
                pass
            try:
                sr = str(getattr(request, 'script_root', '') or '').strip()
            except Exception:
                sr = ''
            if sr.startswith('/c/'):
                return redirect(sr + '/auth/login')
            return redirect('/auth/login')

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
            from flask import g
            from app.tenancy import ensure_request_context, is_impersonating

            support_mode = bool(is_impersonating())
            if support_mode:
                try:
                    ensure_request_context()
                except Exception:
                    pass
            support_company = getattr(g, 'company', None) if support_mode else None
            return {
                "is_support_mode": support_mode,
                "support_company": support_company,
            }
        except Exception:
            return {"is_support_mode": False, "support_company": None}

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
