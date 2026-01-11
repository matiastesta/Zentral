web: sh -c "python -m flask --app app.py db upgrade && gunicorn wsgi:app --bind 0.0.0.0:${PORT}"
