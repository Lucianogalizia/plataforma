# ==========================================================
# backend/main.py
#
# Servidor FastAPI principal de la plataforma DINA
#
# Incluye:
#   - Configuración CORS para el frontend Next.js
#   - Registro de todos los routers (din, mapa, validaciones, diagnosticos)
#   - Health check y endpoint de info
#   - Manejo global de excepciones
#   - Startup event (verificación de conexiones)
# ==========================================================

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.din          import router as din_router
from api.mapa         import router as mapa_router
from api.validaciones import router as validaciones_router
from api.diagnosticos import router as diagnosticos_router


# ==========================================================
# Configuración de entorno
# ==========================================================

# Orígenes permitidos para CORS.
# En producción: la URL de tu frontend en Cloud Run.
# En desarrollo: localhost:3000 (Next.js dev server).
CORS_ORIGINS_ENV = os.environ.get(
    "CORS_ORIGINS",
    "http://localhost:3000,http://localhost:8501"
)
CORS_ORIGINS = [o.strip() for o in CORS_ORIGINS_ENV.split(",") if o.strip()]

# Versión de la API (para /api/info)
API_VERSION = "1.0.0"

# Timestamp de inicio (para uptime en /api/health)
_START_TIME = time.time()


# ==========================================================
# Lifespan (startup / shutdown)
# ==========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Eventos de startup y shutdown.

    Startup:
        - Verifica conexión a GCS
        - Verifica disponibilidad de índices DIN y NIV
        - Imprime resumen de configuración

    Shutdown:
        - Log de cierre (Cloud Run maneja el SIGTERM)
    """
    # --- STARTUP ---
    print("=" * 60)
    print("  Plataforma DINA — Backend FastAPI")
    print(f"  Versión:     {API_VERSION}")
    print(f"  Iniciando:   {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    # Verificar variables de entorno críticas
    bucket = os.environ.get("DINAS_BUCKET", "")
    prefix = os.environ.get("DINAS_GCS_PREFIX", "")
    print(f"  GCS Bucket:  {bucket or '⚠️  NO CONFIGURADO'}")
    print(f"  GCS Prefix:  {prefix or '(vacío)'}")

    # Verificar GCS
    try:
        from core.gcs import get_gcs_client
        client = get_gcs_client()
        if client:
            print("  GCS Client:  ✅ Conectado")
        else:
            print("  GCS Client:  ⚠️  No disponible (modo local)")
    except Exception as e:
        print(f"  GCS Client:  ❌ Error: {e}")

    # Verificar índices
    try:
        from core.gcs import load_din_index, load_niv_index
        df_din = load_din_index()
        df_niv = load_niv_index()
        print(f"  DIN Index:   ✅ {len(df_din)} registros")
        print(f"  NIV Index:   ✅ {len(df_niv)} registros")
    except Exception as e:
        print(f"  Índices:     ❌ Error al cargar: {e}")

    # Verificar OpenAI key
    try:
        from ia.diagnostico import get_openai_key
        key = get_openai_key()
        print(f"  OpenAI Key:  {'✅ Configurada' if key else '⚠️  No encontrada'}")
    except Exception as e:
        print(f"  OpenAI Key:  ❌ Error: {e}")

    # CORS
    print(f"  CORS Origins: {CORS_ORIGINS}")
    print("=" * 60)

    yield

    # --- SHUTDOWN ---
    uptime = round(time.time() - _START_TIME, 1)
    print(f"\n  Plataforma DINA cerrando. Uptime: {uptime}s")


# ==========================================================
# App principal
# ==========================================================

app = FastAPI(
    title       = "Plataforma DINA — API",
    description = (
        "Backend FastAPI para la plataforma de análisis dinamométrico DINA. "
        "Gestiona cartas dinamométricas, sumergencias, diagnósticos IA "
        "y validaciones de pozos petroleros."
    ),
    version     = API_VERSION,
    docs_url    = "/api/docs",
    redoc_url   = "/api/redoc",
    openapi_url = "/api/openapi.json",
    lifespan    = lifespan,
)


# ==========================================================
# Middleware CORS
# ==========================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins     = CORS_ORIGINS,
    allow_credentials = True,
    allow_methods     = ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers     = ["*"],
)


# ==========================================================
# Middleware de timing (para logs de latencia)
# ==========================================================

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """
    Agrega el header X-Process-Time a cada respuesta.
    Útil para monitorear latencia en Cloud Run logs.
    """
    t0       = time.time()
    response = await call_next(request)
    elapsed  = round((time.time() - t0) * 1000, 2)
    response.headers["X-Process-Time"] = f"{elapsed}ms"
    return response


# ==========================================================
# Manejo global de excepciones
# ==========================================================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Captura excepciones no manejadas y devuelve JSON estructurado
    en vez de un 500 genérico de FastAPI.
    """
    return JSONResponse(
        status_code=500,
        content={
            "error":   "Error interno del servidor",
            "detalle": str(exc),
            "path":    str(request.url),
        },
    )


# ==========================================================
# Routers
# ==========================================================

app.include_router(
    din_router,
    prefix = "/api/din",
    tags   = ["DIN — Cartas dinamométricas"],
)

app.include_router(
    mapa_router,
    prefix = "/api/mapa",
    tags   = ["Mapa — Sumergencia georreferenciada"],
)

app.include_router(
    validaciones_router,
    prefix = "/api/validaciones",
    tags   = ["Validaciones — Sistema de validación de sumergencias"],
)

app.include_router(
    diagnosticos_router,
    prefix = "/api/diagnosticos",
    tags   = ["Diagnósticos — IA con OpenAI"],
)


# ==========================================================
# Endpoints raíz
# ==========================================================

@app.get("/", include_in_schema=False)
async def root():
    """Redirect informativo a la documentación."""
    return {
        "mensaje": "Plataforma DINA — Backend API",
        "docs":    "/api/docs",
        "version": API_VERSION,
    }


@app.get("/api/health", tags=["Sistema"])
async def health_check():
    """
    Health check para Cloud Run.
    Cloud Run llama a este endpoint para verificar que el servicio está vivo.

    Returns:
        { "status": "ok", "uptime_seg": float, "timestamp": str }
    """
    return {
        "status":     "ok",
        "uptime_seg": round(time.time() - _START_TIME, 1),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/info", tags=["Sistema"])
async def get_info():
    """
    Información del sistema: versión, configuración GCS,
    conteo de índices, y disponibilidad de OpenAI.

    Returns:
        {
            "version":     str,
            "gcs_bucket":  str,
            "gcs_prefix":  str,
            "gcs_ok":      bool,
            "openai_ok":   bool,
            "din_count":   int,
            "niv_count":   int,
            "uptime_seg":  float,
        }
    """
    from core.gcs import (
        get_gcs_client,
        load_din_index,
        load_niv_index,
        GCS_BUCKET,
        GCS_PREFIX,
    )
    from ia.diagnostico import get_openai_key

    # GCS
    try:
        client = get_gcs_client()
        gcs_ok = client is not None
    except Exception:
        gcs_ok = False

    # Índices
    try:
        din_count = len(load_din_index())
        niv_count = len(load_niv_index())
    except Exception:
        din_count = -1
        niv_count = -1

    # OpenAI
    try:
        openai_ok = bool(get_openai_key())
    except Exception:
        openai_ok = False

    return {
        "version":    API_VERSION,
        "gcs_bucket": GCS_BUCKET or "no configurado",
        "gcs_prefix": GCS_PREFIX or "(vacío)",
        "gcs_ok":     gcs_ok,
        "openai_ok":  openai_ok,
        "din_count":  din_count,
        "niv_count":  niv_count,
        "uptime_seg": round(time.time() - _START_TIME, 1),
    }


@app.get("/api/rutas", tags=["Sistema"])
async def get_rutas():
    """
    Lista todas las rutas registradas en la API.
    Útil para desarrollo y debugging.

    Returns:
        { "rutas": [{ "path": str, "methods": [str] }] }
    """
    rutas = []
    for route in app.routes:
        if hasattr(route, "path") and hasattr(route, "methods"):
            rutas.append({
                "path":    route.path,
                "methods": sorted(list(route.methods or [])),
            })
    return {"rutas": sorted(rutas, key=lambda r: r["path"])}
