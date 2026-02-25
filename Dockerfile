FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Copiamos requirements del backend (NO el de la raíz)
COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

# Copiamos solo el backend
COPY backend /app/backend

# Importante: correr desde /app/backend para que funcionen imports tipo "from api.xxx import ..."
WORKDIR /app/backend

RUN mkdir -p /app/backend/assets

EXPOSE 8000

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1 --log-level debug"]
