import os

from app import create_app

app = create_app()

if __name__ == '__main__':
    debug = str(os.environ.get('APP_DEBUG') or '').strip().lower() in ('1', 'true', 'yes', 'on')
    use_reloader = str(os.environ.get('APP_RELOAD') or '').strip().lower() in ('1', 'true', 'yes', 'on')
    port_raw = os.environ.get('APP_PORT') or os.environ.get('PORT') or '5000'
    try:
        port = int(str(port_raw).strip())
    except Exception:
        port = 5000
    app.run(debug=debug, use_reloader=use_reloader, port=port)
