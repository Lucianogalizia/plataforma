# ==========================================================
# backend/api/diagnosticos.py
#
# Endpoints REST para diagnósticos IA
#
# IMPORTANTE: Las rutas estáticas (tabla-global, estado-cache, kpis,
# estado-batch, generar-todos) DEBEN estar definidas ANTES que /{pozo}
# para que FastAPI no las interprete como un parámetro de path.
#
# Rutas:
#   GET  /api/diagnosticos/tabla-global     → tabla global (una fila por medición)
#   GET  /api/diagnosticos/estado-cache     → estado del caché GCS
#   GET  /api/diagnosticos/estado-batch     → estado de generación en lote
#   GET  /api/diagnosticos/kpis             → KPIs de la tabla global
#   POST /api/diagnosticos/generar-todos    → genera diagnósticos en lote
#   GET  /api/diagnosticos/{pozo}           → diagnóstico de un pozo
#   POST /api/diagnosticos/{pozo}/generar   → genera/regenera diagnóstico
#   DELETE /api/diagnosticos/{pozo}         → elimina el caché de un pozo
# ==========================================================

from __future__ import annotations

from typing import Optional

import pandas as pd
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel

from core.gcs import (
    GCS_BUCKET,
    GCS_PREFIX,
    load_din_index,
    load_niv_index,
    load_diag_from_gcs,
    load_all_diags_from_gcs,
    load_coords_repo,
    resolve_existing_path,
    get_gcs_client,
)
from core.parsers import normalize_no_exact
from core.consolidado import prepare_indexes
from core.cache import cache
from ia.diagnostico import (
    get_openai_key,
    generar_diagnostico,
    generar_todos,
    necesita_regenerar,
    build_global_table,
    build_bat_map,
    get_kpis_global_table,
    get_estado_cache,
)

router = APIRouter()

_DIAG_IDX_TTL = 3600  # 1 hora — datos cambian ~1 vez por día


# ==========================================================
# Modelos Pydantic
# ==========================================================

class GenerarTodosRequest(BaseModel):
    solo_pendientes: bool = True
    pozos: Optional[list[str]] = None


# ==========================================================
# Estado global de la tarea en lote
# ==========================================================

_batch_status: dict = {
    "corriendo":  False,
    "total":      0,
    "procesados": 0,
    "ok":         [],
    "error":      [],
    "salteados":  [],
    "eta_seg":    None,
    "ultimo":     None,
}


# ==========================================================
# Helpers internos
# ==========================================================

def _load_din_niv_ok():
    cached = cache.get("diag_indexes_ok")
    if cached is not None:
        return (cached[0].copy(), cached[1].copy())

    df_din = load_din_index()
    df_niv = load_niv_index()

    if not df_din.empty and "path" in df_din.columns:
        df_din["path"] = df_din["path"].apply(
            lambda x: resolve_existing_path(x) if pd.notna(x) else None
        )

    df_din_k, df_niv_k, _ = prepare_indexes(df_din, df_niv)

    din_ok = df_din_k.copy()
    niv_ok = df_niv_k.copy()

    if not din_ok.empty and "error" in din_ok.columns:
        din_ok = din_ok[din_ok["error"].isna()]
    if not niv_ok.empty and "error" in niv_ok.columns:
        niv_ok = niv_ok[niv_ok["error"].isna()]

    result = (din_ok, niv_ok)
    cache.set("diag_indexes_ok", result, ttl=_DIAG_IDX_TTL)
    return result


def _get_bat_map() -> dict:
    cached = cache.get("diag_bat_map")
    if cached is not None:
        return cached
    coords = load_coords_repo()
    result = build_bat_map(coords, normalize_no_exact)
    cache.set("diag_bat_map", result, ttl=_DIAG_IDX_TTL)
    return result


def _get_pozos_con_din(din_ok: pd.DataFrame) -> list[str]:
    if din_ok.empty or "NO_key" not in din_ok.columns:
        return []
    return sorted(din_ok["NO_key"].dropna().unique().tolist())


# ==========================================================
# *** RUTAS ESTÁTICAS PRIMERO ***
# (deben ir ANTES de /{pozo} para que FastAPI no las capture)
# ==========================================================

# ----------------------------------------------------------
# GET /api/diagnosticos/tabla-global
# ----------------------------------------------------------

def _get_tabla_global_df():
    """Construye (o devuelve desde caché) el DataFrame de la tabla global."""
    cached = cache.get("diag_tabla_global_df")
    if cached is not None:
        return cached

    din_ok, _ = _load_din_niv_ok()
    pozos     = _get_pozos_con_din(din_ok)

    if not pozos:
        import pandas as pd
        return pd.DataFrame()

    diags   = load_all_diags_from_gcs(pozos)
    bat_map = _get_bat_map()
    df      = build_global_table(diags, bat_map, normalize_no_exact)
    cache.set("diag_tabla_global_df", df, ttl=_DIAG_IDX_TTL)
    return df


@router.get("/tabla-global")
async def get_tabla_global(
    baterias:     Optional[str] = Query(None),
    severidad:    Optional[str] = Query(None),
    solo_activas: bool          = Query(False),
):
    """
    Tabla global de diagnósticos — una fila por medición.
    Devuelve { total, rows, kpis }.
    """
    df = _get_tabla_global_df()

    if df.empty:
        return {"total": 0, "rows": [], "kpis": {}}

    if baterias:
        bat_list = [b.strip() for b in baterias.split(",") if b.strip()]
        if bat_list and "Batería" in df.columns:
            df = df[df["Batería"].isin(bat_list)]

    if severidad and "Sev. máx" in df.columns:
        df = df[df["Sev. máx"] == severidad.upper()]

    if solo_activas and "Act." in df.columns:
        df = df[df["Act."] > 0]

    kpis = get_kpis_global_table(df)
    df   = df.where(pd.notnull(df), None)

    return {
        "total": len(df),
        "rows":  df.to_dict(orient="records"),
        "kpis":  kpis,
    }


# ----------------------------------------------------------
# GET /api/diagnosticos/estado-cache
# ----------------------------------------------------------

@router.get("/estado-cache")
async def get_estado_cache_endpoint():
    """
    Estado del caché de diagnósticos en GCS.
    Devuelve { total_pozos_con_din, con_diagnostico, pendientes }.
    """
    din_ok, _ = _load_din_niv_ok()
    pozos     = _get_pozos_con_din(din_ok)

    if not pozos:
        return {
            "total_pozos_con_din": 0,
            "con_diagnostico":     0,
            "pendientes":          0,
        }

    estado = get_estado_cache(pozos, din_ok)

    return {
        "total_pozos_con_din": estado["total"],
        "con_diagnostico":     estado["listos"],
        "pendientes":          estado["pendientes"],
    }


# ----------------------------------------------------------
# GET /api/diagnosticos/estado-batch
# ----------------------------------------------------------

@router.get("/estado-batch")
async def get_estado_batch():
    """Estado actual de la generación en lote."""
    st   = _batch_status
    tot  = st.get("total", 0)
    proc = st.get("procesados", 0)

    return {
        "corriendo":  st.get("corriendo", False),
        "total":      tot,
        "procesados": proc,
        "pct":        round(proc / tot * 100, 1) if tot > 0 else 0.0,
        "ok":         len(st.get("ok",        [])),
        "error":      len(st.get("error",     [])),
        "salteados":  len(st.get("salteados", [])),
        "eta_seg":    st.get("eta_seg"),
        "ultimo":     st.get("ultimo"),
    }


# ----------------------------------------------------------
# GET /api/diagnosticos/kpis
# ----------------------------------------------------------

@router.get("/kpis")
async def get_kpis_diagnosticos():
    """KPIs principales de la tabla global."""
    din_ok, _ = _load_din_niv_ok()
    pozos     = _get_pozos_con_din(din_ok)

    if not pozos:
        return {
            "pozos_diagnosticados": 0,
            "mediciones_totales":   0,
            "criticos":             0,
            "alta_severidad":       0,
            "sin_problematicas":    0,
        }

    diags   = load_all_diags_from_gcs(pozos)
    bat_map = _get_bat_map()
    df      = build_global_table(diags, bat_map, normalize_no_exact)

    return get_kpis_global_table(df)


# ----------------------------------------------------------
# POST /api/diagnosticos/generar-todos
# ----------------------------------------------------------

def _run_batch(
    pozos:           list[str],
    din_ok:          pd.DataFrame,
    niv_ok:          pd.DataFrame,
    api_key:         str,
    solo_pendientes: bool,
):
    global _batch_status

    _batch_status = {
        "corriendo":  True,
        "total":      len(pozos),
        "procesados": 0,
        "ok":         [],
        "error":      [],
        "salteados":  [],
        "eta_seg":    None,
        "ultimo":     None,
    }

    def progress_cb(idx, total, no_key, resultado, eta_seg=None):
        _batch_status["procesados"] = idx + 1
        _batch_status["ultimo"]     = no_key
        _batch_status["eta_seg"]    = eta_seg
        if resultado == "ok":
            _batch_status["ok"].append(no_key)
        elif resultado == "error":
            _batch_status["error"].append(no_key)
        elif resultado == "salteado":
            _batch_status["salteados"].append(no_key)

    try:
        generar_todos(
            pozos=pozos,
            din_ok=din_ok,
            resolve_path_fn=resolve_existing_path,
            api_key=api_key,
            solo_pendientes=solo_pendientes,
            niv_ok=niv_ok,
            progress_cb=progress_cb,
        )
    finally:
        _batch_status["corriendo"] = False


@router.post("/generar-todos")
async def post_generar_todos(
    body:             GenerarTodosRequest,
    background_tasks: BackgroundTasks,
):
    """Inicia la generación de diagnósticos en lote en background."""
    global _batch_status

    if _batch_status.get("corriendo"):
        raise HTTPException(
            status_code=409,
            detail="Ya hay una generación en lote corriendo."
        )

    api_key = get_openai_key()
    if not api_key:
        raise HTTPException(status_code=503, detail="API key de OpenAI no configurada.")

    din_ok, niv_ok = _load_din_niv_ok()

    pozos = body.pozos or _get_pozos_con_din(din_ok)
    pozos = [normalize_no_exact(p) for p in pozos if p]
    pozos = [p for p in pozos if p]

    if not pozos:
        raise HTTPException(status_code=404, detail="No hay pozos con DIN disponibles.")

    background_tasks.add_task(
        _run_batch,
        pozos=pozos,
        din_ok=din_ok,
        niv_ok=niv_ok,
        api_key=api_key,
        solo_pendientes=body.solo_pendientes,
    )

    return {"iniciado": True, "total_a_procesar": len(pozos)}


# ==========================================================
# *** RUTAS DINÁMICAS AL FINAL ***
# /{pozo} debe ir DESPUÉS de todas las rutas estáticas
# ==========================================================

# ----------------------------------------------------------
# GET /api/diagnosticos/{pozo}
# ----------------------------------------------------------

@router.get("/{pozo}")
async def get_diagnostico_pozo(
    pozo:      str,
    regenerar: bool = Query(False),
):
    """Devuelve el diagnóstico IA de un pozo (desde caché o regenerado)."""
    no_key = normalize_no_exact(pozo)
    if not no_key:
        raise HTTPException(status_code=400, detail="Pozo inválido")

    din_ok, niv_ok = _load_din_niv_ok()

    cache = load_diag_from_gcs(no_key) if GCS_BUCKET else None

    if not regenerar and not necesita_regenerar(cache, din_ok, no_key):
        return cache

    api_key = get_openai_key()
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail="API key de OpenAI no configurada."
        )

    diag = generar_diagnostico(
        no_key=no_key,
        din_ok=din_ok,
        resolve_path_fn=resolve_existing_path,
        api_key=api_key,
        niv_ok=niv_ok,
    )

    if "error" in diag:
        raise HTTPException(status_code=500, detail=diag["error"])

    return diag


# ----------------------------------------------------------
# POST /api/diagnosticos/{pozo}/generar
# ----------------------------------------------------------

@router.post("/{pozo}/generar")
async def post_generar_diagnostico(pozo: str):
    """Fuerza la regeneración del diagnóstico de un pozo."""
    no_key = normalize_no_exact(pozo)
    if not no_key:
        raise HTTPException(status_code=400, detail="Pozo inválido")

    api_key = get_openai_key()
    if not api_key:
        raise HTTPException(status_code=503, detail="API key de OpenAI no configurada.")

    din_ok, niv_ok = _load_din_niv_ok()

    diag = generar_diagnostico(
        no_key=no_key,
        din_ok=din_ok,
        resolve_path_fn=resolve_existing_path,
        api_key=api_key,
        niv_ok=niv_ok,
    )

    if "error" in diag:
        raise HTTPException(status_code=500, detail=diag["error"])

    return diag


# ----------------------------------------------------------
# DELETE /api/diagnosticos/{pozo}
# ----------------------------------------------------------

@router.delete("/{pozo}")
async def delete_diagnostico_pozo(pozo: str):
    """Elimina el diagnóstico cacheado de un pozo en GCS."""
    no_key = normalize_no_exact(pozo)
    if not no_key:
        raise HTTPException(status_code=400, detail="Pozo inválido")

    if not GCS_BUCKET:
        raise HTTPException(status_code=503, detail="GCS no configurado.")

    client = get_gcs_client()
    if not client:
        raise HTTPException(status_code=503, detail="No se pudo conectar a GCS.")

    blob_name = f"diagnosticos/{no_key}/diagnostico.json"
    if GCS_PREFIX:
        blob_name = f"{GCS_PREFIX}/{blob_name}"

    try:
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(blob_name)
        if blob.exists():
            blob.delete()
        return {"ok": True, "pozo": no_key}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error eliminando diagnóstico: {e}")
