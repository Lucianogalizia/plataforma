# ==========================================================
# backend/api/din.py
#
# Endpoints REST para todo lo relacionado con archivos DIN
#
# Rutas:
#   GET  /api/din/pozos                     → lista de pozos disponibles
#   GET  /api/din/mediciones/{pozo}         → mediciones consolidadas de un pozo
#   GET  /api/din/carta-superficie          → puntos CS de un archivo .din
#   GET  /api/din/extras                    → campos extra de un archivo .din
#   GET  /api/din/historico-sumergencia/{pozo} → serie temporal de sumergencia
#   GET  /api/din/snapshot                  → última medición por pozo (global)
#   GET  /api/din/snapshot-mapa             → snapshot para el mapa (con coordenadas)
#   GET  /api/din/tendencias                → tendencia lineal por variable
#   GET  /api/din/pozos-por-mes             → pozos medidos por mes
#   GET  /api/din/cobertura                 → cobertura DIN vs NIV en ventana
# ==========================================================

from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from core.gcs import (
    load_din_index,
    load_niv_index,
    load_snapshot,
    load_coords_repo,
    load_all_validaciones,
    resolve_existing_path,
    gcs_download_to_temp,
    is_gs_path,
)
from core.validaciones import (
    make_fecha_key,
    get_validacion,
)
from core.parsers import (
    parse_din_surface_points,
    parse_din_extras,
    normalize_no_exact,
    find_col,
    safe_to_float,
    EXTRA_FIELDS,
)
from core.consolidado import (
    prepare_indexes,
    build_pozo_consolidado,
    build_global_consolidated,
    build_last_snapshot_for_map,
    trend_linear_per_month,
    make_display_label,
    compute_sumergencia_and_base,
)
from core.semaforo import (
    get_pozos_por_mes,
    get_cobertura_din_niv,
)
from core.cache import cache

router = APIRouter()

_SNAP_TTL = 600


# ==========================================================
# Helpers internos
# ==========================================================

def _load_indexes_with_keys():
    """
    Carga y prepara los índices DIN y NIV con sus keys de normalización.
    Filtra filas con error.

    Returns:
        (din_ok, niv_ok, col_map)
    """
    cached = cache.get("indexes_with_keys")
    if cached is not None:
        return cached

    df_din = load_din_index()
    df_niv = load_niv_index()

    # Resolver paths DIN
    if not df_din.empty and "path" in df_din.columns:
        df_din["path"] = df_din["path"].apply(
            lambda x: resolve_existing_path(x) if pd.notna(x) else None
        )

    # Preparar keys
    df_din_k, df_niv_k, col_map = prepare_indexes(df_din, df_niv)

    # Filtrar errores
    din_ok = df_din_k.copy()
    niv_ok = df_niv_k.copy()

    if not din_ok.empty and "error" in din_ok.columns:
        din_ok = din_ok[din_ok["error"].isna()]
    if not niv_ok.empty and "error" in niv_ok.columns:
        niv_ok = niv_ok[niv_ok["error"].isna()]

    result = (din_ok, niv_ok, col_map)
    cache.set("indexes_with_keys", result, ttl=_SNAP_TTL)
    return result


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    import math
    df = df.copy()
    for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S").where(df[col].notna(), None)
    for col in df.columns:
        if hasattr(df[col], "dt") and hasattr(df[col].dt, "to_timestamp"):
            try:
                df[col] = df[col].astype(str)
            except Exception:
                pass
    df = df.where(pd.notnull(df), None)
    records = df.to_dict(orient="records")
    # Limpiar NaN/inf que sobreviven en floats
    def clean(v):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v
    return [{k: clean(v) for k, v in r.items()} for r in records]


# ==========================================================
# GET /api/din/pozos
# ==========================================================

@router.get("/pozos")
async def get_pozos(
    solo_con_din: bool = Query(
        False,
        description="Si True, devuelve solo pozos con archivos DIN disponibles"
    ),
):
    """
    Lista todos los pozos disponibles en los índices DIN y NIV.

    Query params:
        solo_con_din: bool (default False)

    Returns:
        { "pozos": [...], "total": int }
    """
    din_ok, niv_ok, _ = _load_indexes_with_keys()

    pozos_din = (
        din_ok["NO_key"].dropna().unique().tolist()
        if not din_ok.empty else []
    )
    pozos_niv = (
        niv_ok["NO_key"].dropna().unique().tolist()
        if not niv_ok.empty and not solo_con_din else []
    )

    pozos = sorted(set(pozos_din + pozos_niv))
    pozos = [p for p in pozos if p]  # quitar vacíos

    return {"pozos": pozos, "total": len(pozos)}


# ==========================================================
# GET /api/din/mediciones/{pozo}
# ==========================================================

@router.get("/mediciones/{pozo}")
async def get_mediciones_pozo(pozo: str):
    """
    Devuelve las mediciones consolidadas DIN+NIV de un pozo.
    Incluye Sumergencia, DT_plot, extras de cada .din, y Batería.

    Path params:
        pozo: NO_key normalizado del pozo

    Returns:
        {
            "pozo":       str,
            "total":      int,
            "mediciones": [...],
            "opciones_din": [{ "id": path, "label": "fecha | hora | DIN" }]
        }
    """
    din_ok, niv_ok, col_map = _load_indexes_with_keys()

    pozo_norm = normalize_no_exact(pozo)
    if not pozo_norm:
        raise HTTPException(status_code=400, detail="Pozo inválido")

    dfp = build_pozo_consolidado(
        din_ok, niv_ok, pozo_norm,
        col_map["din_no_col"], col_map["din_fe_col"], col_map["din_ho_col"],
        col_map["niv_no_col"], col_map["niv_fe_col"], col_map["niv_ho_col"],
    )

    if dfp.empty:
        return {"pozo": pozo_norm, "total": 0, "mediciones": [], "opciones_din": []}

    # --- Agregar extras DIN ---
    extra_rows = []
    if "path" in dfp.columns:
        for _, row in dfp.iterrows():
            if row.get("ORIGEN") == "DIN":
                pth = row.get("path")
                try:
                    p_res = resolve_existing_path(pth)
                    if p_res:
                        extra_rows.append(parse_din_extras(str(p_res)))
                    else:
                        extra_rows.append({k: None for k in EXTRA_FIELDS})
                except Exception:
                    extra_rows.append({k: None for k in EXTRA_FIELDS})
            else:
                extra_rows.append({k: None for k in EXTRA_FIELDS})
    else:
        extra_rows = [{k: None for k in EXTRA_FIELDS} for _ in range(len(dfp))]

    df_extra = pd.DataFrame(extra_rows)
    for c in df_extra.columns:
        if c in dfp.columns:
            dfp = dfp.drop(columns=[c])
    dfp = pd.concat(
        [dfp.reset_index(drop=True), df_extra.reset_index(drop=True)],
        axis=1
    )

    # --- Agregar Batería desde Excel de coordenadas ---
    coords = load_coords_repo()
    if (
        not coords.empty
        and "nombre_corto" in coords.columns
        and "nivel_5" in coords.columns
    ):
        coords_bat = coords[["nombre_corto", "nivel_5"]].copy()
        coords_bat["NO_key"] = coords_bat["nombre_corto"].apply(normalize_no_exact)
        coords_bat = coords_bat.drop_duplicates(subset=["NO_key"])
        dfp = dfp.merge(
            coords_bat[["NO_key", "nivel_5"]].rename(columns={"nivel_5": "Batería"}),
            on="NO_key",
            how="left",
        )
    else:
        dfp["Batería"] = None

    # --- Opciones DIN para el selector de carta ---
    opciones_din = []
    if "path" in dfp.columns and "ORIGEN" in dfp.columns:
        din_rows = dfp[dfp["ORIGEN"] == "DIN"].copy()
        din_rows["option_label"] = din_rows.apply(make_display_label, axis=1)
        for _, r in din_rows.iterrows():
            path = r.get("path")
            if path:
                opciones_din.append({
                    "id":    str(path),
                    "label": r.get("option_label", str(path)),
                })

    return {
        "pozo":        pozo_norm,
        "total":       len(dfp),
        "mediciones":  _df_to_records(dfp),
        "opciones_din": opciones_din,
    }


# ==========================================================
# GET /api/din/carta-superficie
# ==========================================================

@router.get("/carta-superficie")
async def get_carta_superficie(
    path: str = Query(..., description="Path local o gs:// del archivo .din"),
):
    """
    Devuelve los puntos X/Y de la sección [CS] de un archivo .din.
    Usado para graficar la carta dinamométrica de superficie.

    Query params:
        path: path local o gs:// del archivo .din

    Returns:
        {
            "n_puntos": int,
            "puntos":   [{"i": int, "X": float, "Y": float}, ...]
        }
    """
    p_res = resolve_existing_path(path)
    if not p_res:
        raise HTTPException(
            status_code=404,
            detail=f"Archivo no encontrado: {path}"
        )

    try:
        pts = parse_din_surface_points(str(p_res))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error parseando carta: {e}"
        )

    if pts.empty:
        return {"n_puntos": 0, "puntos": []}

    return {
        "n_puntos": len(pts),
        "puntos":   pts.to_dict(orient="records"),
    }


# ==========================================================
# GET /api/din/extras
# ==========================================================

@router.get("/extras")
async def get_extras_din(
    path: str = Query(..., description="Path local o gs:// del archivo .din"),
):
    """
    Devuelve los campos extra de un archivo .din
    (AIB, BOMBA, MOTOR, RARE, RARR, RBO, RAEB).

    Query params:
        path: path local o gs:// del archivo .din

    Returns:
        dict con todos los campos de EXTRA_FIELDS
    """
    p_res = resolve_existing_path(path)
    if not p_res:
        raise HTTPException(
            status_code=404,
            detail=f"Archivo no encontrado: {path}"
        )

    try:
        extras = parse_din_extras(str(p_res))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error extrayendo extras: {e}"
        )

    return extras


# ==========================================================
# GET /api/din/historico-sumergencia/{pozo}
# ==========================================================

@router.get("/historico-sumergencia/{pozo}")
async def get_historico_sumergencia(pozo: str):
    """
    Devuelve la serie temporal de Sumergencia para un pozo.
    Incluye PB, nivel usado, base y origen de cada medición.

    Path params:
        pozo: NO_key normalizado del pozo

    Returns:
        {
            "pozo":  str,
            "serie": [
                {
                    "dt":            str (ISO),
                    "sumergencia":   float | None,
                    "base":          str | None,
                    "pb":            float | None,
                    "nivel_usado":   float | None,
                    "origen":        str,
                }
            ]
        }
    """
    din_ok, niv_ok, col_map = _load_indexes_with_keys()

    pozo_norm = normalize_no_exact(pozo)
    if not pozo_norm:
        raise HTTPException(status_code=400, detail="Pozo inválido")

    dfp = build_pozo_consolidado(
        din_ok, niv_ok, pozo_norm,
        col_map["din_no_col"], col_map["din_fe_col"], col_map["din_ho_col"],
        col_map["niv_no_col"], col_map["niv_fe_col"], col_map["niv_ho_col"],
    )

    if dfp.empty or "Sumergencia" not in dfp.columns:
        return {"pozo": pozo_norm, "serie": []}

    hist = dfp.copy()
    hist["Sumergencia"] = pd.to_numeric(hist["Sumergencia"], errors="coerce")
    hist = hist.dropna(subset=["DT_plot", "Sumergencia"]).sort_values("DT_plot")

    def pick_nivel(row):
        base = row.get("Sumergencia_base")
        if base == "NC": return safe_to_float(row.get("NC"))
        if base == "NM": return safe_to_float(row.get("NM"))
        if base == "ND": return safe_to_float(row.get("ND"))
        return None

    serie = []
    for _, row in hist.iterrows():
        dt = row.get("DT_plot")
        serie.append({
            "dt":          dt.isoformat() if hasattr(dt, "isoformat") else str(dt),
            "sumergencia": float(row["Sumergencia"]),
            "base":        row.get("Sumergencia_base"),
            "pb":          safe_to_float(row.get("PB")),
            "nivel_usado": pick_nivel(row),
            "origen":      row.get("ORIGEN", ""),
        })

    return {"pozo": pozo_norm, "serie": serie}


# ==========================================================
# GET /api/din/snapshot
# ==========================================================

@router.get("/snapshot")
async def get_snapshot(
    origen: Optional[str] = Query(
        None,
        description="Filtrar por origen: DIN, NIV o vacío para todos"
    ),
    sum_min: Optional[float] = Query(None, description="Sumergencia mínima"),
    sum_max: Optional[float] = Query(None, description="Sumergencia máxima"),
    est_min: Optional[float] = Query(None, description="%Estructura mínimo"),
    est_max: Optional[float] = Query(None, description="%Estructura máximo"),
    bal_min: Optional[float] = Query(None, description="%Balance mínimo"),
    bal_max: Optional[float] = Query(None, description="%Balance máximo"),
):
    """
    Devuelve el snapshot global: última medición por pozo.
    Incluye extras del snapshot pregenerado por build_snapshot.py.

    Query params:
        origen:  "DIN" | "NIV" (opcional)
        sum_min / sum_max: rango de Sumergencia
        est_min / est_max: rango de %Estructura
        bal_min / bal_max: rango de %Balance

    Returns:
        {
            "total":  int,
            "snap":   [...],
            "kpis":   { total_pozos, ultima_din, ultima_niv, con_sumergencia, con_pb }
        }
    """
    din_ok, niv_ok, col_map = _load_indexes_with_keys()

    snap = build_global_consolidated(
        din_ok, niv_ok,
        col_map["din_no_col"], col_map["din_fe_col"], col_map["din_ho_col"],
        col_map["niv_no_col"], col_map["niv_fe_col"], col_map["niv_ho_col"],
    )

    if snap.empty:
        return {"total": 0, "snap": [], "kpis": {}}

    snap["DT_plot"] = pd.to_datetime(snap["DT_plot"], errors="coerce")
    snap = (
        snap.sort_values(["NO_key", "DT_plot"], na_position="last")
        .dropna(subset=["DT_plot"])
        .groupby("NO_key", as_index=False)
        .tail(1)
        .copy()
    )

    # --- Merge con snapshot pregenerado (extras nocturnos) ---
    snap_pre = load_snapshot()
    if not snap_pre.empty and "NO_key" in snap_pre.columns:
        extra_cols = [c for c in EXTRA_FIELDS if c in snap_pre.columns]
        if extra_cols:
            snap_pre_slim = snap_pre[["NO_key"] + extra_cols].drop_duplicates("NO_key")
            snap = snap.merge(snap_pre_slim, on="NO_key", how="left", suffixes=("", "_pre"))
            for c in extra_cols:
                c_pre = f"{c}_pre"
                if c_pre in snap.columns:
                    snap[c] = snap[c_pre].combine_first(snap[c])
                    snap = snap.drop(columns=[c_pre])

    for c in EXTRA_FIELDS:
        if c not in snap.columns:
            snap[c] = None

    # --- Antigüedad ---
    now = pd.Timestamp.now()
    snap["Dias_desde_ultima"] = (
        (now - snap["DT_plot"]).dt.total_seconds() / 86400.0
    )

    # --- Merge con Batería ---
    coords = load_coords_repo()
    if (
        not coords.empty
        and "nombre_corto" in coords.columns
        and "nivel_5" in coords.columns
    ):
        coords_bat = coords[["nombre_corto", "nivel_5"]].copy()
        coords_bat["NO_key"] = coords_bat["nombre_corto"].apply(normalize_no_exact)
        coords_bat = coords_bat.drop_duplicates(subset=["NO_key"])
        snap = snap.merge(
            coords_bat[["NO_key", "nivel_5"]].rename(columns={"nivel_5": "Bateria"}),
            on="NO_key",
            how="left",
        )
    else:
        snap["Bateria"] = None

    # --- Filtros ---
    if origen:
        snap = snap[snap["ORIGEN"] == origen.upper()]
    if sum_min is not None:
        snap = snap[snap["Sumergencia"].isna() | (snap["Sumergencia"] >= sum_min)]
    if sum_max is not None:
        snap = snap[snap["Sumergencia"].isna() | (snap["Sumergencia"] <= sum_max)]
    if est_min is not None:
        snap = snap[snap["%Estructura"].isna() | (snap["%Estructura"] >= est_min)]
    if est_max is not None:
        snap = snap[snap["%Estructura"].isna() | (snap["%Estructura"] <= est_max)]
    if bal_min is not None:
        snap = snap[snap["%Balance"].isna() | (snap["%Balance"] >= bal_min)]
    if bal_max is not None:
        snap = snap[snap["%Balance"].isna() | (snap["%Balance"] <= bal_max)]

    # --- KPIs ---
    kpis = {
        "total_pozos":     len(snap),
        "ultima_din":      int((snap["ORIGEN"] == "DIN").sum()) if "ORIGEN" in snap.columns else 0,
        "ultima_niv":      int((snap["ORIGEN"] == "NIV").sum()) if "ORIGEN" in snap.columns else 0,
        "con_sumergencia": int(snap["Sumergencia"].notna().sum()) if "Sumergencia" in snap.columns else 0,
        "con_pb":          int(snap["PB"].notna().sum()) if "PB" in snap.columns else 0,
    }

    return {
        "total": len(snap),
        "snap":  _df_to_records(snap),
        "kpis":  kpis,
    }


# ==========================================================
# GET /api/din/snapshot-mapa
# ==========================================================

@router.get("/snapshot-mapa")
async def get_snapshot_mapa(
    sum_min:  Optional[float] = Query(None),
    sum_max:  Optional[float] = Query(None),
    dias_min: Optional[float] = Query(None),
    dias_max: Optional[float] = Query(None),
    baterias: Optional[str]   = Query(
        None,
        description="Baterías separadas por coma, ej: 'BAT1,BAT2'"
    ),
    solo_validadas: Optional[bool] = Query(
        None,
        description="None=todos | True=solo validadas | False=solo no validadas"
    ),
):
    """
    Devuelve el snapshot para el mapa de sumergencia.
    Incluye coordenadas lat/lon y Batería.
    Listo para renderizar con Deck.gl en el frontend.

    Query params:
        sum_min / sum_max:  rango de Sumergencia
        dias_min / dias_max: rango de días desde última medición
        baterias: lista de baterías separadas por coma

    Returns:
        {
            "total":  int,
            "puntos": [
                {
                    "NO_key":           str,
                    "nivel_5":          str,
                    "ORIGEN":           str,
                    "DT_plot_str":      str,
                    "Sumergencia":      float,
                    "Dias_desde_ultima":float,
                    "lat":              float,
                    "lon":              float,
                }
            ]
        }
    """
    din_ok, niv_ok, _ = _load_indexes_with_keys()

    m = cache.get("snap_con_coords")
    if m is None:
        snap_map = build_last_snapshot_for_map(din_ok, niv_ok)

        if snap_map.empty:
            return {"total": 0, "puntos": []}

        snap_map["DT_plot"] = pd.to_datetime(snap_map["DT_plot"], errors="coerce")
        snap_map = snap_map.dropna(subset=["DT_plot"])

        now = pd.Timestamp.now()
        snap_map["Dias_desde_ultima"] = (
            (now - snap_map["DT_plot"]).dt.total_seconds() / 86400.0
        )
        snap_map["Sumergencia"] = pd.to_numeric(snap_map["Sumergencia"], errors="coerce")

        coords = load_coords_repo()
        if coords.empty:
            raise HTTPException(
                status_code=503,
                detail="Excel de coordenadas no disponible"
            )

        coords = coords.copy()
        coords["NO_key"] = coords["nombre_corto"].apply(normalize_no_exact)
        snap_map["NO_key"] = snap_map["NO_key"].apply(normalize_no_exact)

        m = snap_map.merge(
            coords[["NO_key", "nombre_corto", "nivel_5", "GEO_LATITUDE", "GEO_LONGITUDE"]],
            on="NO_key",
            how="left",
        ).rename(columns={"GEO_LATITUDE": "lat", "GEO_LONGITUDE": "lon"})

        m["lat"] = pd.to_numeric(m["lat"], errors="coerce")
        m["lon"] = pd.to_numeric(m["lon"], errors="coerce")

        m = m[m["lat"].notna() & m["lon"].notna()].copy()

        if "nivel_5" in m.columns:
            m["nivel_5"] = m["nivel_5"].astype("string").str.strip()

        cache.set("snap_con_coords", m, ttl=_SNAP_TTL)

    # Filtros sobre copia
    m = m.copy()

    if baterias:
        bat_list = [b.strip() for b in baterias.split(",") if b.strip()]
        if bat_list and "nivel_5" in m.columns:
            m = m[m["nivel_5"].isin(bat_list)]

    if sum_min is not None:
        m = m[m["Sumergencia"].between(sum_min, float("inf"), inclusive="both")]
    if sum_max is not None:
        m = m[m["Sumergencia"].between(float("-inf"), sum_max, inclusive="both")]
    if dias_min is not None:
        m = m[m["Dias_desde_ultima"] >= dias_min]
    if dias_max is not None:
        m = m[m["Dias_desde_ultima"] <= dias_max]

    # --- Filtro por validación ---
    if solo_validadas is not None:
        pozos_val = m["NO_key"].dropna().unique().tolist()
        todas_val = load_all_validaciones(pozos_val)

        def _es_valida(row) -> bool:
            nk  = str(row.get("NO_key", ""))
            fk  = make_fecha_key(row.get("DT_plot"))
            vd  = todas_val.get(nk, {})
            return get_validacion(vd, fk).get("validada", True)

        mask = m.apply(_es_valida, axis=1)
        m = m[mask].copy() if solo_validadas else m[~mask].copy()

    # --- Preparar respuesta JSON-safe ---
    m["DT_plot_str"] = pd.to_datetime(
        m["DT_plot"], errors="coerce"
    ).dt.strftime("%Y-%m-%d %H:%M")

    keep_cols = [
        c for c in [
            "NO_key", "nivel_5", "ORIGEN", "DT_plot_str",
            "Sumergencia", "Dias_desde_ultima", "lat", "lon",
            "PE", "PB", "NM", "NC", "ND", "Sumergencia_base",
        ]
        if c in m.columns
    ]

    m_out = m[keep_cols].copy()
    m_out = m_out.where(pd.notnull(m_out), None)

    return {
        "total":  len(m_out),
        "puntos": _df_to_records(m_out),
    }


# ==========================================================
# GET /api/din/tendencias
# ==========================================================

@router.get("/tendencias")
async def get_tendencias(
    variable: str   = Query("Sumergencia", description="Variable a analizar"),
    min_pts:  int   = Query(4,             description="Mínimo de puntos para calcular tendencia"),
    solo_positiva: bool = Query(True,      description="Solo pendiente positiva"),
    top:      int   = Query(30,            description="Máximo de pozos a devolver"),
):
    """
    Calcula la tendencia lineal por mes de una variable para todos los pozos.

    Query params:
        variable:       nombre de la columna (Sumergencia, PB, %Balance, etc.)
        min_pts:        mínimo de puntos para calcular (default 4)
        solo_positiva:  si True, devuelve solo pendiente > 0 (default True)
        top:            máximo de pozos en el resultado (default 30)

    Returns:
        {
            "variable": str,
            "pozos":    [
                {
                    "NO_key":             str,
                    "n_puntos":           int,
                    "pendiente_por_mes":  float,
                    "valor_inicial":      float,
                    "valor_final":        float,
                    "delta_total":        float,
                    "fecha_inicial":      str,
                    "fecha_final":        str,
                }
            ]
        }
    """
    din_ok, niv_ok, col_map = _load_indexes_with_keys()

    df_all = build_global_consolidated(
        din_ok, niv_ok,
        col_map["din_no_col"], col_map["din_fe_col"], col_map["din_ho_col"],
        col_map["niv_no_col"], col_map["niv_fe_col"], col_map["niv_ho_col"],
    )

    if df_all.empty:
        return {"variable": variable, "pozos": []}

    df_all["DT_plot"] = pd.to_datetime(df_all["DT_plot"], errors="coerce")
    df_all = df_all.dropna(subset=["DT_plot"])

    # Si la variable viene del snapshot pregenerado
    if variable not in df_all.columns:
        snap_pre = load_snapshot()
        if not snap_pre.empty and variable in snap_pre.columns and "NO_key" in snap_pre.columns:
            col_pre = snap_pre[["NO_key", variable]].drop_duplicates("NO_key")
            df_all  = df_all.merge(col_pre, on="NO_key", how="left")

    if variable not in df_all.columns:
        raise HTTPException(
            status_code=404,
            detail=f"Variable '{variable}' no disponible"
        )

    df_all[variable] = pd.to_numeric(df_all[variable], errors="coerce")

    rows = []
    for no_key, g in df_all.groupby("NO_key"):
        res = trend_linear_per_month(g, variable)
        if res is None:
            continue
        slope_m, y0, y1, npts = res
        if npts < min_pts:
            continue
        if solo_positiva and slope_m <= 0:
            continue
        rows.append({
            "NO_key":            no_key,
            "n_puntos":          npts,
            "pendiente_por_mes": round(slope_m, 4),
            "valor_inicial":     round(y0, 2),
            "valor_final":       round(y1, 2),
            "delta_total":       round(y1 - y0, 2),
            "fecha_inicial":     g["DT_plot"].min().isoformat(),
            "fecha_final":       g["DT_plot"].max().isoformat(),
        })

    rows.sort(key=lambda r: r["pendiente_por_mes"], reverse=True)

    return {"variable": variable, "pozos": rows[:top]}


# ==========================================================
# GET /api/din/pozos-por-mes
# ==========================================================

@router.get("/pozos-por-mes")
async def get_pozos_por_mes_endpoint():
    """
    Devuelve la cantidad de pozos únicos medidos por mes.

    Returns:
        {
            "ultimo_mes":   str,
            "ultimo_valor": int,
            "serie":        [{"Mes": str, "Pozos_medidos": int}]
        }
    """
    din_ok, niv_ok, col_map = _load_indexes_with_keys()

    df_all = build_global_consolidated(
        din_ok, niv_ok,
        col_map["din_no_col"], col_map["din_fe_col"], col_map["din_ho_col"],
        col_map["niv_no_col"], col_map["niv_fe_col"], col_map["niv_ho_col"],
    )

    p_counts = get_pozos_por_mes(df_all)

    if p_counts.empty:
        return {"ultimo_mes": None, "ultimo_valor": 0, "serie": []}

    last      = p_counts.sort_values("Mes").tail(1)
    ultimo_mes = last["Mes"].values[0]
    ultimo_val = int(last["Pozos_medidos"].values[0])

    return {
        "ultimo_mes":   ultimo_mes,
        "ultimo_valor": ultimo_val,
        "serie":        p_counts.to_dict(orient="records"),
    }


# ==========================================================
# GET /api/din/cobertura
# ==========================================================

@router.get("/cobertura")
async def get_cobertura(
    fecha_desde: date = Query(..., description="Fecha inicio (YYYY-MM-DD)"),
    fecha_hasta: date = Query(..., description="Fecha fin   (YYYY-MM-DD)"),
    modo: str = Query(
        "historico",
        description="'historico' = todas las mediciones | 'snapshot' = última por pozo"
    ),
):
    """
    Calcula la cobertura DIN vs NIV en una ventana de fechas.

    Query params:
        fecha_desde: YYYY-MM-DD
        fecha_hasta: YYYY-MM-DD
        modo:        "historico" | "snapshot"

    Returns:
        {
            "total_pozos":      int,
            "pozos_con_din":    int,
            "pozos_sin_din":    int,
            "lista_sin_din":    [str, ...]
        }
    """
    din_ok, niv_ok, col_map = _load_indexes_with_keys()

    df_all = build_global_consolidated(
        din_ok, niv_ok,
        col_map["din_no_col"], col_map["din_fe_col"], col_map["din_ho_col"],
        col_map["niv_no_col"], col_map["niv_fe_col"], col_map["niv_ho_col"],
    )

    cov_from = pd.Timestamp(fecha_desde)
    cov_to   = pd.Timestamp(fecha_hasta) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    resultado = get_cobertura_din_niv(df_all, cov_from, cov_to, modo)

    return resultado
