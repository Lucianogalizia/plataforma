# ==========================================================
# backend/api/mapa.py
#
# Endpoints REST para el mapa de sumergencia
# ==========================================================

from __future__ import annotations

from typing import Optional, List, Dict, Any, Tuple

import pandas as pd
from fastapi import APIRouter, Query, Response

from core.cache import ttl_get
from core.gcs import load_din_index, load_niv_index, load_snapshot, load_coords_repo, resolve_existing_path

from core.consolidado import (
    prepare_indexes,
    build_last_snapshot_for_map,
    normalize_no_exact,
)

from api.validaciones import (
    load_all_validaciones,
    make_fecha_key,
    get_validacion,
)

router = APIRouter(prefix="/api/mapa", tags=["mapa"])

# ==========================================================
# Constantes
# ==========================================================

EXTRA_FIELDS = [
    "PB", "NM", "NC", "ND", "PE",
    "Bba Llenado", "SE",
]

# ==========================================================
# Utilidades JSON safe
# ==========================================================

def _to_json_safe(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    out = df.replace([pd.NA], [None])
    out = out.where(pd.notna(out), None)
    return out.to_dict(orient="records")


def _apply_filtros(
    df: pd.DataFrame,
    baterias: Optional[str],
    sum_min: Optional[float],
    sum_max: Optional[float],
    dias_min: Optional[float],
    dias_max: Optional[float],
    origen: Optional[str],
) -> pd.DataFrame:
    snap = df

    if baterias:
        bats = [b.strip() for b in baterias.split(",") if b.strip()]
        if bats and "nivel_5" in snap.columns:
            snap = snap[snap["nivel_5"].isin(bats)].copy()

    if origen:
        if "ORIGEN" in snap.columns:
            snap = snap[snap["ORIGEN"].astype(str).str.upper() == origen.upper()].copy()

    if sum_min is not None and "Sumergencia" in snap.columns:
        snap = snap[pd.to_numeric(snap["Sumergencia"], errors="coerce") >= float(sum_min)].copy()

    if sum_max is not None and "Sumergencia" in snap.columns:
        snap = snap[pd.to_numeric(snap["Sumergencia"], errors="coerce") <= float(sum_max)].copy()

    if dias_min is not None and "Dias_desde_ultima" in snap.columns:
        snap = snap[pd.to_numeric(snap["Dias_desde_ultima"], errors="coerce") >= float(dias_min)].copy()

    if dias_max is not None and "Dias_desde_ultima" in snap.columns:
        snap = snap[pd.to_numeric(snap["Dias_desde_ultima"], errors="coerce") <= float(dias_max)].copy()

    return snap


# ==========================================================
# Cache interno: índices OK
# ==========================================================

def _load_indexes_ok_uncached():
    """
    Carga índices DIN y NIV con keys, sin errores.
    Returns: (din_ok, niv_ok, col_map)
    """
    df_din = load_din_index()
    df_niv = load_niv_index()

    if not df_din.empty and "path" in df_din.columns:
        df_din["path"] = df_din["path"].apply(
            lambda x: resolve_existing_path(x) if pd.notna(x) else None
        )

    df_din_k, df_niv_k, col_map = prepare_indexes(df_din, df_niv)

    din_ok = df_din_k.copy()
    niv_ok = df_niv_k.copy()

    if not din_ok.empty and "error" in din_ok.columns:
        din_ok = din_ok[din_ok["error"].isna()]
    if not niv_ok.empty and "error" in niv_ok.columns:
        niv_ok = niv_ok[niv_ok["error"].isna()]

    return din_ok, niv_ok, col_map


def _load_indexes_ok():
    # 5 minutos cache (índices cambian poco)
    return ttl_get("mapa:indexes_ok", _load_indexes_ok_uncached, ttl_s=300)


# ==========================================================
# Cache interno: snapshot base del mapa (sin filtros)
# ==========================================================

def _build_snap_con_coords_uncached(
    din_ok: pd.DataFrame,
    niv_ok: pd.DataFrame,
) -> pd.DataFrame:
    """
    Construye el snapshot (1 fila por pozo) mergeado con coordenadas,
    batería y extras del snapshot nocturno.
    """
    snap_map = build_last_snapshot_for_map(din_ok, niv_ok)

    if snap_map.empty:
        return pd.DataFrame()

    snap_map["DT_plot"] = pd.to_datetime(snap_map["DT_plot"], errors="coerce")
    snap_map = snap_map.dropna(subset=["DT_plot"])

    now = pd.Timestamp.now()
    snap_map["Dias_desde_ultima"] = (
        (now - snap_map["DT_plot"]).dt.total_seconds() / 86400.0
    )
    snap_map["Sumergencia"] = pd.to_numeric(snap_map["Sumergencia"], errors="coerce")

    # --- Merge con snapshot nocturno (extras) ---
    snap_pre = load_snapshot()
    if not snap_pre.empty and "NO_key" in snap_pre.columns:
        extra_cols = [c for c in EXTRA_FIELDS if c in snap_pre.columns]
        if extra_cols:
            snap_pre_slim = snap_pre[["NO_key"] + extra_cols].drop_duplicates("NO_key")
            snap_map = snap_map.merge(
                snap_pre_slim, on="NO_key", how="left", suffixes=("", "_pre")
            )
            for c in extra_cols:
                c_pre = f"{c}_pre"
                if c_pre in snap_map.columns:
                    snap_map[c] = snap_map[c_pre].combine_first(snap_map[c])
                    snap_map = snap_map.drop(columns=[c_pre])

    for c in EXTRA_FIELDS:
        if c not in snap_map.columns:
            snap_map[c] = None

    # --- Merge con coordenadas ---
    coords = load_coords_repo()
    if coords.empty:
        return snap_map

    coords = coords.copy()
    coords["NO_key"] = coords["nombre_corto"].apply(normalize_no_exact)
    snap_map["NO_key"] = snap_map["NO_key"].apply(normalize_no_exact)

    snap_map = snap_map.merge(
        coords[[
            "NO_key", "nombre_corto", "nivel_5",
            "GEO_LATITUDE", "GEO_LONGITUDE",
        ]],
        on="NO_key",
        how="left",
    ).rename(columns={
        "GEO_LATITUDE": "lat",
        "GEO_LONGITUDE": "lon",
    })

    return snap_map


def _build_snap_con_coords(din_ok: pd.DataFrame, niv_ok: pd.DataFrame) -> pd.DataFrame:
    # 60 segundos cache (es el “dataset base” del mapa)
    # Clave única “base”: dependemos de TTL del snapshot y coords
    def _loader():
        return _build_snap_con_coords_uncached(din_ok, niv_ok)
    return ttl_get("mapa:snap_base", _loader, ttl_s=60)


# ==========================================================
# GET /api/mapa/baterias
# ==========================================================

@router.get("/baterias")
async def get_baterias(response: Response):
    # Cache corto (browser) + React Query ya hace el resto
    response.headers["Cache-Control"] = "public, max-age=300"

    din_ok, niv_ok, _ = _load_indexes_ok()
    snap = _build_snap_con_coords(din_ok, niv_ok)
    if snap.empty or "nivel_5" not in snap.columns:
        return {"baterias": []}

    bats = sorted([b for b in snap["nivel_5"].dropna().unique().tolist() if str(b).strip()])
    return {"baterias": [{"nombre": b} for b in bats]}


# ==========================================================
# GET /api/mapa/puntos
# ==========================================================

@router.get("/puntos")
async def get_puntos_mapa(
    response: Response,
    baterias: Optional[str] = Query(None, description="Baterías separadas por coma"),
    sum_min: Optional[float] = Query(None, description="Sumergencia mínima (m)"),
    sum_max: Optional[float] = Query(None, description="Sumergencia máxima (m)"),
    dias_min: Optional[float] = Query(None, description="Días desde última medición mínimo"),
    dias_max: Optional[float] = Query(None, description="Días desde última medición máximo"),
    origen: Optional[str] = Query(None, description="DIN | NIV"),
    solo_con_coords: bool = Query(True, description="Solo pozos con coordenadas válidas"),
    solo_validadas: Optional[bool] = Query(
        None,
        description="None=todos | True=solo validadas | False=solo no validadas"
    ),
):
    # Cache corto (browser). El dataset puede cambiar, no lo pongas enorme.
    response.headers["Cache-Control"] = "public, max-age=60"

    din_ok, niv_ok, _ = _load_indexes_ok()
    snap = _build_snap_con_coords(din_ok, niv_ok)

    if snap.empty:
        return {"total": 0, "puntos": []}

    if solo_con_coords and "lat" in snap.columns and "lon" in snap.columns:
        snap = snap[snap["lat"].notna() & snap["lon"].notna()].copy()

    snap = _apply_filtros(snap, baterias, sum_min, sum_max, dias_min, dias_max, origen)

    # Filtro de validación
    if solo_validadas is not None and not snap.empty:
        pozos_val = snap["NO_key"].dropna().unique().tolist()
        todas_val = load_all_validaciones(pozos_val)

        def _es_valida(row) -> bool:
            nk = normalize_no_exact(str(row.get("NO_key", "")))
            fk = make_fecha_key(row.get("DT_plot"))
            vd = todas_val.get(nk, {})
            est = get_validacion(vd, fk)
            return est.get("validada", True)

        mask = snap.apply(_es_valida, axis=1)
        snap = snap[mask].copy() if solo_validadas else snap[~mask].copy()

    # Marcar validación en cada punto
    if not snap.empty:
        pozos_val = snap["NO_key"].dropna().unique().tolist()
        todas_val = load_all_validaciones(pozos_val)

        def _get_val(row) -> bool:
            nk = normalize_no_exact(str(row.get("NO_key", "")))
            fk = make_fecha_key(row.get("DT_plot"))
            vd = todas_val.get(nk, {})
            return get_validacion(vd, fk).get("validada", True)

        snap["validada"] = snap.apply(_get_val, axis=1)
    else:
        snap["validada"] = True

    snap["DT_plot_str"] = pd.to_datetime(snap["DT_plot"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")

    keep = [
        c for c in [
            "NO_key", "nivel_5", "nombre_corto", "ORIGEN",
            "DT_plot_str", "Sumergencia", "Sumergencia_base",
            "Dias_desde_ultima", "lat", "lon",
            "PE", "PB", "NM", "NC", "ND",
            "Bba Llenado", "SE", "validada",
        ]
        if c in snap.columns
    ]

    snap_out = snap[keep].copy()

    return {"total": len(snap_out), "puntos": _to_json_safe(snap_out)}
