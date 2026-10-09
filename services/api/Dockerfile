FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py costs.py dashboard.html costs.html start_api.sh ./
RUN chmod +x /app/start_api.sh

ENTRYPOINT ["/app/start_api.sh"]
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "3003", "--workers", "2", "--loop", "uvloop", "--http", "httptools", "--no-access-log"]
