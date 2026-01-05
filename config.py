import os
from datetime import timedelta
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-key-change-in-production'

    # Cookies / sesión
    # Nota: si usás subdominios por empresa (empresa.tudominio.com) y superadmin en el dominio base,
    # necesitás SESSION_COOKIE_DOMAIN='.' + dominio_base para que el navegador comparta la cookie.
    SESSION_COOKIE_DOMAIN = os.environ.get('SESSION_COOKIE_DOMAIN') or None
    SESSION_COOKIE_SAMESITE = os.environ.get('SESSION_COOKIE_SAMESITE') or 'Lax'
    _is_railway = bool(os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('RAILWAY_PROJECT_ID') or os.environ.get('RAILWAY_SERVICE_ID'))
    _session_secure_raw = str(os.environ.get('SESSION_COOKIE_SECURE') or '').strip().lower()
    SESSION_COOKIE_SECURE = (_session_secure_raw in {'1', 'true', 'yes', 'on'}) if _session_secure_raw else _is_railway

    REMEMBER_COOKIE_DOMAIN = os.environ.get('REMEMBER_COOKIE_DOMAIN') or SESSION_COOKIE_DOMAIN
    REMEMBER_COOKIE_SAMESITE = os.environ.get('REMEMBER_COOKIE_SAMESITE') or SESSION_COOKIE_SAMESITE
    _remember_secure_raw = str(os.environ.get('REMEMBER_COOKIE_SECURE') or '').strip().lower()
    REMEMBER_COOKIE_SECURE = (_remember_secure_raw in {'1', 'true', 'yes', 'on'}) if _remember_secure_raw else SESSION_COOKIE_SECURE
    try:
        _remember_days = int(str(os.environ.get('REMEMBER_COOKIE_DAYS') or '30').strip())
    except Exception:
        _remember_days = 30
    REMEMBER_COOKIE_DURATION = timedelta(days=max(1, _remember_days))

    _db_url = os.environ.get('DATABASE_URL')
    if _db_url:
        raw = str(_db_url).strip()
        if raw in ('sqlite://', 'sqlite:///:memory:'):
            _db_url = None
        else:
            if raw.startswith('postgresql+psycopg2://'):
                raw = 'postgresql+psycopg://' + raw[len('postgresql+psycopg2://'):]
            elif raw.startswith('postgres://'):
                raw = 'postgresql+psycopg://' + raw[len('postgres://'):]
            elif raw.startswith('postgresql://') and 'postgresql+' not in raw:
                raw = 'postgresql+psycopg://' + raw[len('postgresql://'):]
            _db_url = raw

    SQLALCHEMY_DATABASE_URI = _db_url or ('sqlite:///' + os.path.join(basedir, 'app.db'))
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Email configuration
    MAIL_SERVER = os.environ.get('MAIL_SERVER')
    MAIL_PORT = int(os.environ.get('MAIL_PORT') or 25)
    MAIL_USE_TLS = os.environ.get('MAIL_USE_TLS') is not None
    MAIL_USERNAME = os.environ.get('MAIL_USERNAME')
    MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD')
    ADMINS = ['your-email@example.com']
    
    # Pagination
    ITEMS_PER_PAGE = 10
    
    # Supported languages
    LANGUAGES = ['es', 'en']
    BABEL_DEFAULT_LOCALE = 'es'
    
    # Upload configuration
    UPLOAD_FOLDER = os.path.join(basedir, 'app/static/uploads')
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max upload size
    ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

    # Cloudflare R2 (S3 compatible) - private objects with presigned URLs
    R2_ENDPOINT_URL = os.environ.get('R2_ENDPOINT_URL') or ''
    R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID') or ''
    R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY') or ''
    R2_BUCKET = os.environ.get('R2_BUCKET') or ''
    R2_REGION = os.environ.get('R2_REGION') or 'auto'
    try:
        R2_PRESIGNED_EXPIRES_SECONDS = int(str(os.environ.get('R2_PRESIGNED_EXPIRES_SECONDS') or '120').strip())
    except Exception:
        R2_PRESIGNED_EXPIRES_SECONDS = 120

class DevelopmentConfig(Config):
    DEBUG = (not Config._is_railway)
    SQLALCHEMY_ECHO = (not Config._is_railway)

class TestingConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite://'
    WTF_CSRF_ENABLED = False

class ProductionConfig(Config):
    pass

config = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}
