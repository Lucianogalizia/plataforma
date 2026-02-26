# ==========================================================
# backend/api/diagnosticos.py
#
# Endpoints REST para diagnósticos IA
#
# Rutas:
#   GET    /api/diagnosticos/tabla-global     → tabla global (una fila por medición)
#   GET    /api/diagnosticos/estado-cache     → estado del caché GCS
#   GET    /api/diagnosticos/kpis             → KPIs de la tabla global
#   POST   /api/diagnosticos/generar-todos    → genera diagnósticos en lote
#   GET    /api/diagnosticos/estado-batch     → estado del batch
#   GET    /api/diagnosticos/{pozo}           → diagnóstico de un pozo
#   POST   /api/diagnosticos/{pozo}/generar   → genera/regenera diagnóstico
#   DELETE /api/diagnosticos/{pozo}           → elimina el caché de un pozo
#
# IMPORTANTE:
#   En FastAPI, rutas dinámicas tipo "/{pozo}" NO deben ir antes que rutas fijas
#   ("/tabla-global", "/estado-cache", etc.) porque se las “come”.
# ==========================================================

from __future__ import annotations

import asyncio
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


# ==========================================================
# Modelos Pydantic
# ==========================================================

class GenerarTodosRequest(BaseModel):
    """Cuerpo para generación en lote."""
    solo_pendientes: bool = True
    pozos:           Optional[list[str]] = None  # None = todos los que tienen DIN


# ==========================================================
# Estado global de la tarea en lote (simple, en memoria)
# ==========================================================
# En producción con múltiples workers esto debería ir a Redis o GCS.
# Para un solo worker (Cloud Run con min-instances=1) alcanza.

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
    """
    Carga y prepara los índices DIN y NIV con keys y sin errores.

    Returns:
        (din_ok, niv_ok)
    """
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

    return din_ok, niv_ok


def _get_bat_map() -> dict:
    """Construye el mapa pozo → batería desde el Excel de coordenadas."""
    coords = load_coords_repo()
    return build_bat_map(coords, normalize_no_exact)


def _get_pozos_con_din(din_ok: pd.DataFrame) -> list[str]:
    """Devuelve la lista de NO_key con archivos DIN disponibles."""
    if din_ok.empty or "NO_key" not in din_ok.columns:
        return []
    return sorted(din_ok["NO_key"].dropna().unique().tolist())


# ==========================================================
# POST /api/diagnosticos/generar-todos
# ==========================================================

def _run_batch(
    pozos:           list[str],
    din_ok:          pd.DataFrame,
    niv_ok:          pd.DataFrame,
    api_key:         str,
    solo_pendientes: bool,
):
    """
    Función que corre en background para la generación en lote.
    Actualiza _batch_status durante la ejecución.
    """
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
    """
    Inicia la generación de diagnósticos en lote en background.
    No bloquea — devuelve inmediatamente y la tarea corre en segundo plano.
    Consultá /api/diagnosticos/estado-batch para ver el progreso.

    Body (GenerarTodosRequest):
        solo_pendientes: bool (default True) → saltea los ya actualizados
        pozos: list[str] (opcional) → si None, procesa todos con DIN

    Returns:
        { "iniciado": bool, "total_a_procesar": int }
    """
    global _batch_status

    if _batch_status.get("corriendo"):
        raise HTTPException(
            status_code=409,
            detail="Ya hay una generación en lote corriendo. "
                   "Esperá a que termine antes de iniciar otra."
        )

    api_key = get_openai_key()
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail="API key de OpenAI no configurada."
        )

    din_ok, niv_ok = _load_din_niv_ok()

    pozos = body.pozos
    if not pozos:
        pozos = _get_pozos_con_din(din_ok)

    pozos = [normalize_no_exact(p) for p in pozos if p]
    pozos = [p for p in pozos if p]

    if not pozos:
        raise HTTPException(
            status_code=404,
            detail="No hay pozos con DIN disponibles para diagnosticar."
        )

    background_tasks.add_task(
        _run_batch,
        pozos=pozos,
        din_ok=din_ok,
        niv_ok=niv_ok,
        api_key=api_key,
        solo_pendientes=body.solo_pendientes,
    )

    return {
        "iniciado":         True,
        "total_a_procesar": len(pozos),
    }


@router.get("/estado-batch")
async def get_estado_batch():
    """
    Devuelve el estado actual de la generación en lote.

    Returns:
        {
            "corriendo":  bool,
            "total":      int,
            "procesados": int,
            "pct":        float,
            "ok":         int,
            "error":      int,
            "salteados":  int,
            "eta_seg":    int | None,
            "ultimo":     str | None,
        }
    """
    st  = _batch_status
    tot = st.get("total", 0)
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


# ==========================================================
# GET /api/diagnosticos/tabla-global
# ==========================================================

@router.get("/tabla-global")
async def get_tabla_global(
    baterias:  Optional[str]  = Query(
        None,
        description="Baterías separadas por coma"
    ),
    severidad: Optional[str]  = Query(
        None,
        description="BAJA | MEDIA | ALTA | CRÍTICA | NINGUNA"
    ),
    solo_activas: bool = Query(
        False,
        description="Si True, devuelve solo mediciones con problemáticas ACTIVAS"
    ),
):
    """
    Devuelve la tabla global de diagnósticos con UNA FILA POR MEDICIÓN.
    Si un pozo tiene 3 DINs analizados → 3 filas.
    """
    din_ok, _ = _load_din_niv_ok()
    pozos     = _get_pozos_con_din(din_ok)

    if not pozos:
        return {"total": 0, "tabla": [], "kpis": {}}

    # Cargar todos los diagnósticos del caché
    diags   = load_all_diags_from_gcs(pozos)
    bat_map = _get_bat_map()

    df = build_global_table(diags, bat_map, normalize_no_exact)

    if df.empty:
        return {"total": 0, "tabla": [], "kpis": {}}

    # --- Filtros ---
    if baterias:
        bat_list = [b.strip() for b in baterias.split(",") if b.strip()]
        if bat_list and "Batería" in df.columns:
            df = df[df["Batería"].isin(bat_list)]

    if severidad and "Sev. máx" in df.columns:
        df = df[df["Sev. máx"] == severidad.upper()]

    if solo_activas and "Act." in df.columns:
        df = df[df["Act."] > 0]

    kpis = get_kpis_global_table(df)

    # Convertir a JSON-safe
    df = df.where(pd.notnull(df), None)

    return {
        "total": len(df),
        "tabla": df.to_dict(orient="records"),
        "kpis":  kpis,
    }


# ==========================================================
# GET /api/diagnosticos/estado-cache
# ==========================================================

@router.get("/estado-cache")
async def get_estado_cache_endpoint():
    """
    Devuelve el estado del caché de diagnósticos en GCS.
    Indica cuántos pozos están listos y cuántos requieren regeneración.
    """
    din_ok, _ = _load_din_niv_ok()
    pozos     = _get_pozos_con_din(din_ok)

    if not pozos:
        return {"total": 0, "listos": 0, "pendientes": 0, "pct_listo": 0.0}

    estado = get_estado_cache(pozos, din_ok)

    return {
        "total":      estado["total"],
        "listos":     estado["listos"],
        "pendientes": estado["pendientes"],
        "pct_listo":  round(
            estado["listos"] / estado["total"] * 100, 1
        ) if estado["total"] > 0 else 0.0,
    }


# ==========================================================
# GET /api/diagnosticos/kpis
# ==========================================================

@router.get("/kpis")
async def get_kpis_diagnosticos():
    """
    Devuelve los KPIs principales de la tabla global de diagnósticos.
    """
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


# ==========================================================
# GET /api/diagnosticos/{pozo}
# (MOVIDO AL FINAL para no pisar rutas fijas)
# ==========================================================

@router.get("/{pozo}")
async def get_diagnostico_pozo(
    pozo:       str,
    regenerar:  bool = Query(
        False,
        description="Si True, regenera aunque el caché sea válido"
    ),
):
    """
    Devuelve el diagnóstico IA de un pozo.
    Si existe caché válido en GCS y regenerar=False, lo devuelve directo.
    Si regenerar=True o el caché está desactualizado, lo regenera en el momento.
    """
    no_key = normalize_no_exact(pozo)
    if not no_key:
        raise HTTPException(status_code=400, detail="Pozo inválido")

    din_ok, niv_ok = _load_din_niv_ok()

    # --- Verificar caché ---
    cache = load_diag_from_gcs(no_key) if GCS_BUCKET else None

    if not regenerar and not necesita_regenerar(cache, din_ok, no_key):
        return cache

    # --- Regenerar ---
    api_key = get_openai_key()
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail="API key de OpenAI no configurada. "
                   "Verificá GCP Secret Manager o variable OPENAI_API_KEY."
        )

    diag = generar_diagnostico(
        no_key=no_key,
        din_ok=din_ok,
        resolve_path_fn=resolve_existing_path,
        api_key=api_key,
        niv_ok=niv_ok,
    )

    if "error" in diag:
        raise HTTPException(
            status_code=500,
            detail=diag["error"]
        )

    return diag


# ==========================================================
# POST /api/diagnosticos/{pozo}/generar
# (MOVIDO AL FINAL para no pisar rutas fijas)
# ==========================================================

@router.post("/{pozo}/generar")
async def post_generar_diagnostico(pozo: str):
    """
    Fuerza la regeneración del diagnóstico de un pozo,
    ignorando el caché existente.
    """
    no_key = normalize_no_exact(pozo)
    if not no_key:
        raise HTTPException(status_code=400, detail="Pozo inválido")

    api_key = get_openai_key()
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail="API key de OpenAI no configurada."
        )

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


# ==========================================================
# DELETE /api/diagnosticos/{pozo}
# (MOVIDO AL FINAL para no pisar rutas fijas)
# ==========================================================

@router.delete("/{pozo}")
async def delete_diagnostico_pozo(pozo: str):
    """
    Elimina el diagnóstico cacheado de un pozo en GCS.
    Útil para forzar regeneración limpia en la próxima consulta.
    """
    no_key = normalize_no_exact(pozo)
    if not no_key:
        raise HTTPException(status_code=400, detail="Pozo inválido")

    if not GCS_BUCKET:
        raise HTTPException(
            status_code=503,
            detail="GCS no configurado — no hay caché que eliminar."
        )

    client = get_gcs_client()
    if not client:
        raise HTTPException(
            status_code=503,
            detail="No se pudo conectar a GCS."
        )

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
        raise HTTPException(
            status_code=500,
            detail=f"Error eliminando diagnóstico: {e}"
        )
