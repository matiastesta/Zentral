import os
from app import create_app

if str(os.environ.get("APP_DEBUG") or "").strip() == "":
    os.environ["APP_DEBUG"] = "1"
if str(os.environ.get("APP_RELOAD") or "").strip() == "":
    os.environ["APP_RELOAD"] = "1"
if str(os.environ.get("FLASK_DEBUG") or "").strip() == "":
    os.environ["FLASK_DEBUG"] = "1"

app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    debug = str(os.environ.get("FLASK_DEBUG") or "").strip().lower() in {"1", "true", "yes", "on"}
    app.run(
        host="0.0.0.0",
        port=port,
        debug=debug,
        use_reloader=debug
    )
