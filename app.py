import os

from app import create_app

app = create_app()

if __name__ == '__main__':
    debug_env = str(os.environ.get('APP_DEBUG') or '').strip().lower() in ('1', 'true', 'yes', 'on')
    flask_debug_env = str(os.environ.get('FLASK_DEBUG') or '').strip().lower() in ('1', 'true', 'yes', 'on')
    flask_env = str(os.environ.get('FLASK_ENV') or '').strip().lower()
    debug = bool(debug_env or flask_debug_env or flask_env == 'development')

    reload_raw = os.environ.get('APP_RELOAD')
    if reload_raw is None:
        use_reloader = bool(debug)
    else:
        use_reloader = str(reload_raw or '').strip().lower() in ('1', 'true', 'yes', 'on')
    port_raw = os.environ.get('APP_PORT') or os.environ.get('PORT') or '5000'
    try:
        port = int(str(port_raw).strip())
    except Exception:
        port = 5000
    app.run(debug=debug, use_reloader=use_reloader, port=port)
