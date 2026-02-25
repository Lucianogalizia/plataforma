# ==========================================================
# backend/main.py
#
# Servidor FastAPI principal de la plataforma DINA
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


# ==========================
# Routers
# ==========================

from api.din          import router as din_router
from api.niv          import router as niv_router
from api.mapa         import router as mapa_router
from api.validaciones import router as validaciones_router
from api.diagnosticos import router as diagnosticos_router
from api.health       import router as health_router   # ✅ NUEVO


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
# Lifespan
# ==========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
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
        from core.gcs import load_din_index, load_niv_index
        df_din = load_din_index()
        df_niv = load_niv_index()
        print(f"  DIN Index:   ✅ {len(df_din)} registros")
        print(f"  NIV Index:   ✅ {len(df_niv)} registros")
    except Exception as e:
        print(f"  Índices:     ❌ Error al cargar: {e}")

    print(f"  CORS Origins: {CORS_ORIGINS}")
    print("=" * 60)

    yield

    uptime = round(time.time() - _START_TIME, 1)
    print(f"\n  Plataforma DINA cerrando. Uptime: {uptime}s")


# ==========================================================
# App principal
# ==========================================================

app = FastAPI(
    title="Plataforma DINA — API",
    description="Backend FastAPI para la plataforma DINA.",
    version=API_VERSION,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
    default_response_class=NanSafeJSONResponse,
)


# ==========================================================
# Middleware CORS
# ==========================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==========================================================
# Middleware timing
# ==========================================================

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    t0 = time.time()
    response = await call_next(request)
    elapsed = round((time.time() - t0) * 1000, 2)
    response.headers["X-Process-Time"] = f"{elapsed}ms"
    return response


# ==========================================================
# Exception handler
# ==========================================================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "error": "Error interno del servidor",
            "detalle": str(exc),
            "path": str(request.url),
        },
    )


# ==========================================================
# Registro de routers
# ==========================================================

app.include_router(health_router)  # ✅ NUEVO

app.include_router(
    din_router,
    prefix="/api/din",
    tags=["DIN — Cartas dinamométricas"],
)

app.include_router(
    niv_router,
    prefix="/api/niv",
    tags=["NIV — Niveles de fluido"],
)

app.include_router(
    mapa_router,
    prefix="/api/mapa",
    tags=["Mapa — Sumergencia georreferenciada"],
)

app.include_router(
    validaciones_router,
    prefix="/api/validaciones",
    tags=["Validaciones"],
)

app.include_router(
    diagnosticos_router,
    prefix="/api/diagnosticos",
    tags=["Diagnósticos — IA"],
)


# ==========================================================
# Endpoints base
# ==========================================================

@app.get("/", include_in_schema=False)
async def root():
    return {
        "mensaje": "Plataforma DINA — Backend API",
        "docs": "/api/docs",
        "version": API_VERSION,
    }


@app.get("/api/health", tags=["Sistema"])
async def health_check():
    return {
        "status": "ok",
        "uptime_seg": round(time.time() - _START_TIME, 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
