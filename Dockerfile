# ==========================================================
# backend/Dockerfile
#
# Imagen de producción para Cloud Run
#
# Estructura:
#   - Python 3.11 slim (igual que el Streamlit actual)
#   - Instala dependencias en capa separada (caché de Docker)
#   - Copia el código
#   - Corre uvicorn en puerto 8080 (requerido por Cloud Run)
# ==========================================================

FROM python:3.11-slim

# Evitar prompts interactivos en apt
ENV DEBIAN_FRONTEND=noninteractive

# Variables de entorno de Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# --- Instalar dependencias del sistema ---
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# --- Instalar dependencias Python ---
# Copiar solo requirements primero para aprovechar caché de Docker.
# Si el código cambia pero requirements no, esta capa no se rebuildeará.
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# --- Copiar código ---
COPY . .

# --- Puerto de Cloud Run ---
# Cloud Run espera que la app escuche en el puerto definido por
# la variable de entorno PORT (default 8080).
ENV PORT=8080
EXPOSE 8080

# --- Comando de inicio ---
# workers=1 porque el estado del batch (_batch_status) vive en memoria.
# Si necesitás múltiples workers, mové ese estado a Redis o GCS.
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT} --workers 1 --log-level info"]
