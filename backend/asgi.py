from __future__ import annotations

import os
import time
import math
import json as _json
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse


def _clean_nans(o):
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


API_VERSION = "1.0.0"
_START_TIME = time.time()

CORS_ORIGINS_ENV = os.environ.get(
    "CORS_ORIGINS",
    "http://localhost:3000,http://localhost:8501"
)
CORS_ORIGINS = [o.strip() for o in CORS_ORIGINS_ENV.split(",") if o.strip()]


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup liviano (no cargas pesadas)
    print("=" * 60)
    print("  Plataforma DINA — Backend FastAPI (ASGI entrypoint)")
    print(f"  Versión:     {API_VERSION}")
    print(f"  Iniciando:   {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    bucket = os.environ.get("DINAS_BUCKET", "")
    prefix = os.environ.get("DINAS_GCS_PREFIX", "")
    print(f"  GCS Bucket:  {bucket or '⚠️  NO CONFIGURADO'}")
    print(f"  GCS Prefix:  {prefix or '(vacío)'}")
    print(f"  CORS Origins: {CORS_ORIGINS}")
    print("=" * 60)

    # ✅ IMPORTS DIFERIDOS (acá) para que Cloud Run no muera antes de escuchar el puerto
    from api.din          import router as din_router
    from api.niv          import router as niv_router
    from api.mapa         import router as mapa_router
    from api.validaciones import router as validaciones_router
    from api.diagnosticos import router as diagnosticos_router
    from api.health       import router as health_router

    app.include_router(health_router)

    app.include_router(din_router, prefix="/api/din", tags=["DIN — Cartas dinamométricas"])
    app.include_router(niv_router, prefix="/api/niv", tags=["NIV — Niveles de fluido"])
    app.include_router(mapa_router, prefix="/api/mapa", tags=["Mapa — Sumergencia georreferenciada"])
    app.include_router(validaciones_router, prefix="/api/validaciones", tags=["Validaciones — Sistema de validación de sumergencias"])
    app.include_router(diagnosticos_router, prefix="/api/diagnosticos", tags=["Diagnósticos — IA con OpenAI"])

    yield

    uptime = round(time.time() - _START_TIME, 1)
    print(f"\n  Plataforma DINA cerrando. Uptime: {uptime}s")


app = FastAPI(
    title="Plataforma DINA — API",
    description=(
        "Backend FastAPI para la plataforma de análisis dinamométrico DINA. "
        "Gestiona cartas dinamométricas, sumergencias, diagnósticos IA "
        "y validaciones de pozos petroleros."
    ),
    version=API_VERSION,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
    default_response_class=NanSafeJSONResponse,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    t0 = time.time()
    response = await call_next(request)
    elapsed = round((time.time() - t0) * 1000, 2)
    response.headers["X-Process-Time"] = f"{elapsed}ms"
    return response


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


@app.get("/", include_in_schema=False)
async def root():
    return {"mensaje": "Plataforma DINA — Backend API", "docs": "/api/docs", "version": API_VERSION}


@app.get("/api/health", tags=["Sistema"])
async def health_check():
    return {
        "status": "ok",
        "uptime_seg": round(time.time() - _START_TIME, 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/rutas", tags=["Sistema"])
async def get_rutas():
    rutas = []
    for route in app.routes:
        if hasattr(route, "path") and hasattr(route, "methods"):
            rutas.append({"path": route.path, "methods": sorted(list(route.methods or []))})
    return {"rutas": sorted(rutas, key=lambda r: r["path"])}
