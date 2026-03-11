# ==========================================================
# backend/main.py
# ==========================================================

from __future__ import annotations

import os
import time
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import math
import json as _json

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


from api.din                import router as din_router
from api.niv                import router as niv_router
from api.mapa               import router as mapa_router
from api.validaciones       import router as validaciones_router
from api.diagnosticos       import router as diagnosticos_router
from api.acciones           import router as acciones_router
from api.merma              import router as merma_router
from api.rrhh               import router as rrhh_router
from api.alertas_llenado    import router as alertas_llenado_router
from api.alertas_presion    import router as alertas_presion_router
from api.controles          import router as controles_router
from api.instalacion_fondo  import router as instalacion_fondo_router
from api.chat               import router as chat_router


# ==========================================================
# Configuración de entorno
# ==========================================================

CORS_ORIGINS_ENV = os.environ.get("CORS_ORIGINS", "http://localhost:3000,http://localhost:8501")
CORS_ORIGINS = [o.strip() for o in CORS_ORIGINS_ENV.split(",") if o.strip()]

API_VERSION = "1.0.0"
_START_TIME = time.time()


# ==========================================================
# Background warm-up — todo lo pesado acá, NADA en startup
# ==========================================================

def _warm_all():
    """Precalienta TODA la caché en background. El startup no espera esto."""
    time.sleep(3)  # dar tiempo a que uvicorn esté listo

    # ── DIN / NIV / Snapshot ──────────────────────────────
    din_ok = None
    try:
        from api.din import _load_indexes_with_keys
        from api.mapa import _build_snap_con_coords
        din_ok, niv_ok, _ = _load_indexes_with_keys()
        _build_snap_con_coords(din_ok, niv_ok)
        print("BG Caché:    ✅ Índices + snapshot listos")
    except Exception as e:
        print(f"BG Caché:    ⚠️  Índices/snapshot: {e}")

    # ── Acciones, validaciones, diagnósticos ──────────────
    try:
        from core.acciones import load_acciones
        load_acciones()

        from core.gcs import load_all_validaciones, load_all_diags_from_gcs
        pozos = []
        if din_ok is not None and not din_ok.empty and "NO_key" in din_ok.columns:
            pozos = sorted(din_ok["NO_key"].dropna().unique().tolist())
        if pozos:
            load_all_validaciones(pozos)
            load_all_diags_from_gcs(pozos)

        from api.diagnosticos import _load_din_niv_ok, _get_bat_map
        _load_din_niv_ok()
        _get_bat_map()

        from api.validaciones import _load_snap_map
        _load_snap_map()

        from api.din import _build_snapshot_base
        _build_snapshot_base()

        print("BG Caché:    ✅ Acciones, validaciones, diagnósticos listos")
    except Exception as e:
        print(f"BG Caché:    ⚠️  Extras: {e}")

    # ── RRHH Personal + Períodos ──────────────────────────
    try:
        from api.rrhh import _K_PERSONAL, _K_PERIODOS, _TTL_PERSONAL, _TTL_PERIODOS
        from core.rrhh_db import list_personal, current_period_id, recent_periods
        from core.cache import cache

        personal = list_personal()
        cache.set(_K_PERSONAL, {"personal": personal}, ttl=_TTL_PERSONAL)
        periodos_data = {"actual": current_period_id(), "periodos": recent_periods(8)}
        cache.set(_K_PERIODOS, periodos_data, ttl=_TTL_PERIODOS)
        print(f"BG RRHH:     ✅ Personal ({len(personal)} personas) + períodos listos")
    except Exception as e:
        print(f"BG RRHH:     ⚠️  Personal/períodos: {e}")

    # ── RRHH Bitácora + Pendientes + Consolidado ──────────
    try:
        from api.rrhh import (
            _TTL_BITACORA, _TTL_PENDIENTE, _TTL_CONSOL,
            _k_bitacora, _k_pendientes, _k_consolidado,
        )
        from core.rrhh_db import (
            list_personal, current_period_id, get_leader_legajos,
            list_bitacora, list_pendientes_lider, get_consolidado,
            period_display, period_bounds,
        )
        from core.cache import cache

        personal = list_personal()
        periodo_actual = current_period_id()

        for p in personal:
            try:
                partes = list_bitacora(p["legajo"])
                result_partes = []
                for pt in partes:
                    start, end = period_bounds(pt["periodo"])
                    result_partes.append({
                        **pt,
                        "periodo_display": period_display(pt["periodo"]),
                        "periodo_inicio":  start.isoformat(),
                        "periodo_fin":     end.isoformat(),
                    })
                cache.set(_k_bitacora(p["legajo"]),
                          {"legajo": p["legajo"], "partes": result_partes},
                          ttl=_TTL_BITACORA)
            except Exception:
                pass
        print(f"BG RRHH:     ✅ Bitácora ({len(personal)} empleados) lista")

        leaders = get_leader_legajos()
        for leader in leaders:
            try:
                rows = list_pendientes_lider(leader)
                result_rows = []
                for r in rows:
                    start, end = period_bounds(r["periodo"])
                    result_rows.append({
                        **r,
                        "periodo_display": period_display(r["periodo"]),
                        "periodo_inicio":  start.isoformat(),
                        "periodo_fin":     end.isoformat(),
                    })
                cache.set(_k_pendientes(leader),
                          {"leader_legajo": leader, "pendientes": result_rows},
                          ttl=_TTL_PENDIENTE)
            except Exception:
                pass
            try:
                data = get_consolidado(leader, periodo_actual)
                start, end = period_bounds(periodo_actual)
                cache.set(_k_consolidado(leader, periodo_actual), {
                    "leader_legajo":   leader,
                    "periodo":         periodo_actual,
                    "periodo_display": period_display(periodo_actual),
                    "periodo_inicio":  start.isoformat(),
                    "periodo_fin":     end.isoformat(),
                    "empleados":       data,
                }, ttl=_TTL_CONSOL)
            except Exception:
                pass
        print(f"BG RRHH:     ✅ Pendientes + Consolidado ({len(leaders)} líderes) listos")
    except Exception as e:
        print(f"BG RRHH:     ⚠️  Bitácora/Consolidado: {e}")


# ==========================================================
# Lifespan — startup RÁPIDO, solo lo mínimo sincrónico
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

    # GCS check — solo ping, sin cargar datos
    try:
        from core.gcs import get_gcs_client
        client = get_gcs_client()
        print(f"  GCS Client:  {'✅ Conectado' if client else '⚠️  No disponible'}")
    except Exception as e:
        print(f"  GCS Client:  ❌ Error: {e}")

    # OpenAI key check
    try:
        from ia.diagnostico import get_openai_key
        key = get_openai_key()
        print(f"  OpenAI Key:  {'✅ Configurada' if key else '⚠️  No encontrada'}")
    except Exception as e:
        print(f"  OpenAI Key:  ❌ Error: {e}")

    # Migraciones RRHH — rápido, solo DDL
    try:
        from core.rrhh_db import migrate as rrhh_migrate
        rrhh_migrate()
        print("  RRHH DB:     ✅ Migraciones aplicadas")
    except Exception as e:
        print(f"  RRHH DB:     ⚠️  Error en migración: {e}")

    # Lanzar TODO el precalentamiento en background
    threading.Thread(target=_warm_all, daemon=True).start()
    print("  Caché:       ⏳ Precalentando en background...")
    print(f"  CORS:        {CORS_ORIGINS}")
    print("=" * 60)

    yield

    uptime = round(time.time() - _START_TIME, 1)
    print(f"\n  Plataforma DINA cerrando. Uptime: {uptime}s")


# ==========================================================
# App principal
# ==========================================================

app = FastAPI(
    title       = "Plataforma DINA — API",
    description = "Backend FastAPI para la plataforma de análisis dinamométrico DINA.",
    version          = API_VERSION,
    docs_url         = "/api/docs",
    redoc_url        = "/api/redoc",
    openapi_url      = "/api/openapi.json",
    lifespan         = lifespan,
    default_response_class = NanSafeJSONResponse,
)


# ==========================================================
# Middlewares
# ==========================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["*"],
    allow_credentials = False,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    t0       = time.time()
    response = await call_next(request)
    elapsed  = round((time.time() - t0) * 1000, 2)
    response.headers["X-Process-Time"] = f"{elapsed}ms"
    return response


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"error": "Error interno del servidor", "detalle": str(exc), "path": str(request.url)},
    )


# ==========================================================
# Routers
# ==========================================================

app.include_router(din_router,               prefix="/api/din",               tags=["DIN — Cartas dinamométricas"])
app.include_router(niv_router,               prefix="/api/niv",               tags=["NIV — Niveles de fluido"])
app.include_router(mapa_router,              prefix="/api/mapa",              tags=["Mapa — Sumergencia georreferenciada"])
app.include_router(validaciones_router,      prefix="/api/validaciones",      tags=["Validaciones"])
app.include_router(diagnosticos_router,      prefix="/api/diagnosticos",      tags=["Diagnósticos — IA con OpenAI"])
app.include_router(acciones_router,          prefix="/api/acciones",          tags=["Acciones — Optimización de pozos"])
app.include_router(merma_router,             prefix="/api/merma",             tags=["MERMA"])
app.include_router(alertas_llenado_router,   prefix="/api/alertas-llenado",   tags=["Alertas Llenado de Bomba BM"])
app.include_router(alertas_presion_router,   prefix="/api/alertas-presion",   tags=["Predicción Alta Presión"])
app.include_router(controles_router,         prefix="/api/controles",         tags=["Controles — Histórico de producción"])
app.include_router(instalacion_fondo_router, prefix="/api/instalacion-fondo", tags=["Instalación de Fondo"])
app.include_router(rrhh_router,              prefix="/api/rrhh",              tags=["RRHH — Guardias y partes mensuales"])
app.include_router(chat_router,              prefix="/api/chat",              tags=["Chat — Asistente IA"])


# ==========================================================
# Endpoints raíz
# ==========================================================

@app.get("/", include_in_schema=False)
async def root():
    return {"mensaje": "Plataforma DINA — Backend API", "docs": "/api/docs", "version": API_VERSION}


@app.get("/api/health", tags=["Sistema"])
async def health_check():
    return {"status": "ok", "uptime_seg": round(time.time() - _START_TIME, 1), "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/info", tags=["Sistema"])
async def get_info():
    from core.gcs import get_gcs_client, load_din_index, load_niv_index, GCS_BUCKET, GCS_PREFIX
    from ia.diagnostico import get_openai_key

    try:
        gcs_ok = get_gcs_client() is not None
    except Exception:
        gcs_ok = False
    try:
        din_count = len(load_din_index())
        niv_count = len(load_niv_index())
    except Exception:
        din_count = niv_count = -1
    try:
        openai_ok = bool(get_openai_key())
    except Exception:
        openai_ok = False

    return {
        "version": API_VERSION, "gcs_bucket": GCS_BUCKET or "no configurado",
        "gcs_prefix": GCS_PREFIX or "(vacío)", "gcs_ok": gcs_ok, "openai_ok": openai_ok,
        "din_count": din_count, "niv_count": niv_count,
        "uptime_seg": round(time.time() - _START_TIME, 1),
    }


@app.get("/api/rutas", tags=["Sistema"])
async def get_rutas():
    rutas = []
    for route in app.routes:
        if hasattr(route, "path") and hasattr(route, "methods"):
            rutas.append({"path": route.path, "methods": sorted(list(route.methods or []))})
    return {"rutas": sorted(rutas, key=lambda r: r["path"])}
