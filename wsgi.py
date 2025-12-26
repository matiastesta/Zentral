import traceback

try:
    from app import create_app

    app = create_app()
except Exception:
    print("❌ ERROR BOOTSTRAPPING APP")
    traceback.print_exc()
    raise
