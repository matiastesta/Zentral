FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080
EXPOSE 8080

CMD ["sh", "-c", "PORT_RAW=\"${PORT:-8080}\"; PORT_NUM=$(echo \"$PORT_RAW\" | tr -cd '0-9'); if [ -z \"$PORT_NUM\" ]; then PORT_NUM=8080; fi; python -m flask --app app.py db upgrade && gunicorn wsgi:app --bind 0.0.0.0:${PORT_NUM}"]
