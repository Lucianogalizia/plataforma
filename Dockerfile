FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Instalamos deps desde backend/requirements.txt
COPY backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copiamos SOLO el backend (evita meter frontend/scheduler si no hace falta)
COPY backend /app/backend

# Esto es CLAVE para que "from api import ..." funcione bien
ENV PYTHONPATH=/app/backend

WORKDIR /app/backend

# Cloud Run te setea PORT (normalmente 8080). Vos NO lo hardcodees.
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port $PORT --workers 1 --log-level debug"]
