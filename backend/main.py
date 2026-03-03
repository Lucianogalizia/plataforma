# ==========================================================
# backend/main.py
#
# Servidor FastAPI principal de la plataforma DINA
#
# Incluye:
#   - Configuración CORS para el frontend Next.js
#   - Registro de todos los routers (din, niv, mapa, validaciones, diagnosticos)
#   - Health check y endpoint de info
#   - Manejo global de excepciones
#   - Startup event (verificación de conexiones)
# ==========================================================

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import math
import json as _json

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse


def _clean_nans(o):
    """Recursivamente reemplaza NaN/Inf por None para serialización JSON segura."""
    if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
        return None
    if isinstance(o, dict):
        return {k: _clean_nans(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_clean_nans(v) for v in o]
    return o


class NanSafeJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        return _json.dumps(
            _clean_nans(content),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")

from api.din          import router as din_router
from api.niv          import router as niv_router
from api.mapa         import router as mapa_router
from api.validaciones import router as validaciones_router
from api.diagnosticos import router as diagnosticos_router
from api.acciones     import router as acciones_router
from api.merma        import router as merma_router


# ==========================================================
# Configuración de entorno
# ==========================================================

CORS_ORIGINS_ENV = os.environ.get(
    "CORS_ORIGINS",
    "http://localhost:3000,http://localhost:8501"
)
CORS_ORIGINS = [o.strip() for o in CORS_ORIGINS_ENV.split(",") if o.strip()]

API_VERSION = "1.0.0"
_START_TIME = time.time()


# ==========================================================
# Lifespan (startup / shutdown)
# ==========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---
    print("=" * 60)
    print("  Plataforma DINA — Backend FastAPI")
    print(f"  Versión:     {API_VERSION}")
    print(f"  Iniciando:   {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    bucket = os.environ.get("DINAS_BUCKET", "")
    prefix = os.environ.get("DINAS_GCS_PREFIX", "")
    print(f"  GCS Bucket:  {bucket or '⚠️  NO CONFIGURADO'}")
    print(f"  GCS Prefix:  {prefix or '(vacío)'}")

    try:
        from core.gcs import get_gcs_client
        client = get_gcs_client()
        print(f"  GCS Client:  {'✅ Conectado' if client else '⚠️  No disponible (modo local)'}")
    except Exception as e:
        print(f"  GCS Client:  ❌ Error: {e}")

    try:
        from ia.diagnostico import get_openai_key
        key = get_openai_key()
        print(f"  OpenAI Key:  {'✅ Configurada' if key else '⚠️  No encontrada'}")
    except Exception as e:
        print(f"  OpenAI Key:  ❌ Error: {e}")

    # Precalentar caché
    try:
        from api.din import _load_indexes_with_keys
        from api.mapa import _build_snap_con_coords
        print("  Caché:       ⏳ Precalentando índices y snapshot...")
        din_ok, niv_ok, _ = _load_indexes_with_keys()
        _build_snap_con_coords(din_ok, niv_ok)
        print("  Caché:       ✅ Listo")
    except Exception as e:
        print(f"  Caché:       ⚠️  Error precalentando: {e}")

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
    version          = API_VERSION,
    docs_url         = "/api/docs",
    redoc_url        = "/api/redoc",
    openapi_url      = "/api/openapi.json",
    lifespan         = lifespan,
    default_response_class = NanSafeJSONResponse,
)


# ==========================================================
# Middleware CORS
# ==========================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["*"],
    allow_credentials = False,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)


# ==========================================================
# Middleware de timing
# ==========================================================

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
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
    niv_router,
    prefix = "/api/niv",
    tags   = ["NIV — Niveles de fluido"],
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

app.include_router(
    acciones_router,
    prefix = "/api/acciones",
    tags   = ["Acciones — Optimización de pozos"],
)

app.include_router(
    merma_router,
    prefix = "/api/merma",
    tags   = ["MERMA — Dashboard de análisis de merma"],
)


# ==========================================================
# Endpoints raíz
# ==========================================================

@app.get("/", include_in_schema=False)
async def root():
    return {
        "mensaje": "Plataforma DINA — Backend API",
        "docs":    "/api/docs",
        "version": API_VERSION,
    }


@app.get("/api/health", tags=["Sistema"])
async def health_check():
    return {
        "status":     "ok",
        "uptime_seg": round(time.time() - _START_TIME, 1),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/info", tags=["Sistema"])
async def get_info():
    from core.gcs import (
        get_gcs_client,
        load_din_index,
        load_niv_index,
        GCS_BUCKET,
        GCS_PREFIX,
    )
    from ia.diagnostico import get_openai_key

    try:
        client = get_gcs_client()
        gcs_ok = client is not None
    except Exception:
        gcs_ok = False

    try:
        din_count = len(load_din_index())
        niv_count = len(load_niv_index())
    except Exception:
        din_count = -1
        niv_count = -1

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
    rutas = []
    for route in app.routes:
        if hasattr(route, "path") and hasattr(route, "methods"):
            rutas.append({
                "path":    route.path,
                "methods": sorted(list(route.methods or [])),
            })
    return {"rutas": sorted(rutas, key=lambda r: r["path"])}
