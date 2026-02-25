# ==========================================================
# backend/api/mapa.py
#
# Endpoints REST para el mapa de sumergencia
#
# Rutas:
#   GET  /api/mapa/puntos           → puntos para Deck.gl con heatmap
#   GET  /api/mapa/baterias         → lista de baterías disponibles
#   GET  /api/mapa/semaforo-aib     → puntos con estado semáforo AIB
#   GET  /api/mapa/stats            → estadísticas generales del mapa
#   GET  /api/mapa/pozo/{pozo}      → detalle de un pozo para popup
# ==========================================================

from __future__ import annotations

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
)
from core.parsers import (
    normalize_no_exact,
    safe_to_float,
    EXTRA_FIELDS,
)
from core.consolidado import (
    prepare_indexes,
    build_last_snapshot_for_map,
    build_pozo_consolidado,
)
from core.semaforo import (
    apply_semaforo_aib,
    get_semaforo_counts,
    get_calidad_resumen,
    get_kpis_snapshot,
)
from core.validaciones import (
    make_fecha_key,
    get_validacion,
)

router = APIRouter()


# ==========================================================
# Helpers internos
# ==========================================================

def _load_indexes_ok():
    """
    Carga índices DIN y NIV con keys, sin errores.

    Returns:
        (din_ok, niv_ok, col_map)
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


def _build_snap_con_coords(
    din_ok: pd.DataFrame,
    niv_ok: pd.DataFrame,
) -> pd.DataFrame:
    """
    Construye el snapshot (1 fila por pozo) mergeado con coordenadas,
    batería y extras del snapshot nocturno.

    Returns:
        DataFrame listo para el mapa, con columnas:
            NO_key, ORIGEN, DT_plot, Sumergencia, Sumergencia_base,
            PB, NM, NC, ND, PE, lat, lon, nivel_5, nombre_corto,
            Dias_desde_ultima, + todos los campos de EXTRA_FIELDS
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
    snap_map["Sumergencia"] = pd.to_numeric(
        snap_map["Sumergencia"], errors="coerce"
    )

    # --- Merge con snapshot nocturno (extras) ---
    snap_pre = load_snapshot()
    if not snap_pre.empty and "NO_key" in snap_pre.columns:
        extra_cols = [c for c in EXTRA_FIELDS if c in snap_pre.columns]
        if extra_cols:
            snap_pre_slim = (
                snap_pre[["NO_key"] + extra_cols]
                .drop_duplicates("NO_key")
            )
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
        "GEO_LATITUDE":  "lat",
        "GEO_LONGITUDE": "lon",
    })

    snap_map["lat"] = pd.to_numeric(snap_map["lat"], errors="coerce")
    snap_map["lon"] = pd.to_numeric(snap_map["lon"], errors="coerce")

    if "nivel_5" in snap_map.columns:
        snap_map["nivel_5"] = snap_map["nivel_5"].astype("string").str.strip()

    return snap_map


def _apply_filtros(
    df:       pd.DataFrame,
    baterias: Optional[str],
    sum_min:  Optional[float],
    sum_max:  Optional[float],
    dias_min: Optional[float],
    dias_max: Optional[float],
    origen:   Optional[str],
) -> pd.DataFrame:
    """
    Aplica los filtros comunes del mapa a un DataFrame.
    Todos los filtros son opcionales.
    """
    if baterias and "nivel_5" in df.columns:
        bat_list = [b.strip() for b in baterias.split(",") if b.strip()]
        if bat_list:
            df = df[df["nivel_5"].isin(bat_list)]

    if sum_min is not None and "Sumergencia" in df.columns:
        df = df[df["Sumergencia"].isna() | (df["Sumergencia"] >= sum_min)]
    if sum_max is not None and "Sumergencia" in df.columns:
        df = df[df["Sumergencia"].isna() | (df["Sumergencia"] <= sum_max)]

    if dias_min is not None and "Dias_desde_ultima" in df.columns:
        df = df[df["Dias_desde_ultima"] >= dias_min]
    if dias_max is not None and "Dias_desde_ultima" in df.columns:
        df = df[df["Dias_desde_ultima"] <= dias_max]

    if origen and "ORIGEN" in df.columns:
        df = df[df["ORIGEN"] == origen.upper()]

    return df.copy()


def _to_json_safe(df: pd.DataFrame) -> list[dict]:
    """
    Convierte un DataFrame a lista de dicts JSON-safe.
    Timestamps → ISO string, NaN/NaT/Inf → None.
    """
    import math
    df = df.copy()

    for col in df.select_dtypes(
        include=["datetime64[ns]", "datetimetz"]
    ).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S").where(
            df[col].notna(), None
        )

    records = df.to_dict(orient="records")
    clean = []
    for row in records:
        clean_row = {}
        for k, v in row.items():
            if v is None:
                clean_row[k] = None
            elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                clean_row[k] = None
            else:
                clean_row[k] = v
        clean.append(clean_row)
    return clean


# ==========================================================
# GET /api/mapa/baterias
# ==========================================================

@router.get("/baterias")
async def get_baterias():
    """
    Devuelve la lista de baterías disponibles con cantidad de pozos.
    Basado en el Excel de coordenadas.

    Returns:
        {
            "baterias": [
                { "nombre": str, "pozos": int }
            ],
            "total": int
        }
    """
    coords = load_coords_repo()

    if coords.empty or "nivel_5" not in coords.columns:
        return {"baterias": [], "total": 0}

    bat_counts = (
        coords["nivel_5"]
        .astype("string")
        .str.strip()
        .value_counts()
        .reset_index()
    )
    bat_counts.columns = ["nombre", "pozos"]
    bat_counts = bat_counts.sort_values("nombre")

    return {
        "baterias": bat_counts.to_dict(orient="records"),
        "total":    len(bat_counts),
    }


# ==========================================================
# GET /api/mapa/puntos
# ==========================================================

@router.get("/puntos")
async def get_puntos_mapa(
    baterias:  Optional[str]   = Query(None, description="Baterías separadas por coma"),
    sum_min:   Optional[float] = Query(None, description="Sumergencia mínima (m)"),
    sum_max:   Optional[float] = Query(None, description="Sumergencia máxima (m)"),
    dias_min:  Optional[float] = Query(None, description="Días desde última medición mínimo"),
    dias_max:  Optional[float] = Query(None, description="Días desde última medición máximo"),
    origen:    Optional[str]   = Query(None, description="DIN | NIV"),
    solo_con_coords: bool      = Query(True,  description="Solo pozos con coordenadas válidas"),
    solo_validadas:  Optional[bool] = Query(
        None,
        description="None=todos | True=solo validadas | False=solo no validadas"
    ),
):
    """
    Devuelve los puntos del mapa de sumergencia para Deck.gl.
    Incluye todos los campos necesarios para el heatmap y los popups.

    Query params:
        baterias:        lista separada por coma (opcional)
        sum_min/max:     rango de sumergencia
        dias_min/max:    rango de días desde última medición
        origen:          DIN | NIV (opcional)
        solo_con_coords: True (default) → solo pozos georeferenciados
        solo_validadas:  None=todos | True=validadas | False=no validadas

    Returns:
        {
            "total":   int,
            "puntos":  [
                {
                    NO_key, nivel_5, nombre_corto, ORIGEN,
                    DT_plot_str, Sumergencia, Sumergencia_base,
                    Dias_desde_ultima, lat, lon,
                    PE, PB, NM, NC, ND,
                    Bba_Llenado, SE, validada
                }
            ]
        }
    """
    din_ok, niv_ok, _ = _load_indexes_ok()
    snap = _build_snap_con_coords(din_ok, niv_ok)

    if snap.empty:
        return {"total": 0, "puntos": []}

    # Solo con coordenadas
    if solo_con_coords and "lat" in snap.columns and "lon" in snap.columns:
        snap = snap[snap["lat"].notna() & snap["lon"].notna()].copy()

    # Filtros comunes
    snap = _apply_filtros(snap, baterias, sum_min, sum_max, dias_min, dias_max, origen)

    # Filtro de validación
    if solo_validadas is not None:
        pozos_val = snap["NO_key"].dropna().unique().tolist()
        todas_val = load_all_validaciones(pozos_val)

        def _es_valida(row) -> bool:
            nk  = normalize_no_exact(str(row.get("NO_key", "")))
            fk  = make_fecha_key(row.get("DT_plot"))
            vd  = todas_val.get(nk, {})
            est = get_validacion(vd, fk)
            return est.get("validada", True)

        mask = snap.apply(_es_valida, axis=1)
        snap = snap[mask].copy() if solo_validadas else snap[~mask].copy()

    # Marcar validación en cada punto
    if not snap.empty:
        pozos_val = snap["NO_key"].dropna().unique().tolist()
        todas_val = load_all_validaciones(pozos_val)

        def _get_val(row) -> bool:
            nk  = normalize_no_exact(str(row.get("NO_key", "")))
            fk  = make_fecha_key(row.get("DT_plot"))
            vd  = todas_val.get(nk, {})
            return get_validacion(vd, fk).get("validada", True)

        snap["validada"] = snap.apply(_get_val, axis=1)
    else:
        snap["validada"] = True

    # DT_plot → string para JSON
    snap["DT_plot_str"] = pd.to_datetime(
        snap["DT_plot"], errors="coerce"
    ).dt.strftime("%Y-%m-%d %H:%M")

    # Columnas de salida
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

    return {
        "total":  len(snap_out),
        "puntos": _to_json_safe(snap_out),
    }


# ==========================================================
# GET /api/mapa/semaforo-aib
# ==========================================================

@router.get("/semaforo-aib")
async def get_semaforo_aib_mapa(
    baterias:  Optional[str]   = Query(None),
    sum_media: float           = Query(200.0, description="Umbral sumergencia media (m)"),
    sum_alta:  float           = Query(250.0, description="Umbral sumergencia alta (m)"),
    llen_ok:   float           = Query(70.0,  description="Llenado OK (%)"),
    llen_bajo: float           = Query(50.0,  description="Llenado bajo (%)"),
):
    """
    Devuelve los puntos del mapa con estado del semáforo AIB.
    Solo incluye pozos con SE=AIB y coordenadas válidas.

    Query params:
        baterias:  lista separada por coma (opcional)
        sum_media: umbral de sumergencia media (default 200m)
        sum_alta:  umbral de sumergencia alta  (default 250m)
        llen_ok:   llenado OK (default 70%)
        llen_bajo: llenado bajo (default 50%)

    Returns:
        {
            "total":   int,
            "counts":  { total_aib, normal, alerta, critico, sin_datos, no_aplica },
            "puntos":  [
                {
                    NO_key, nivel_5, lat, lon,
                    Sumergencia, Bba_Llenado, SE,
                    Semaforo_AIB, DT_plot_str, Dias_desde_ultima
                }
            ]
        }
    """
    din_ok, niv_ok, _ = _load_indexes_ok()
    snap = _build_snap_con_coords(din_ok, niv_ok)

    if snap.empty:
        return {"total": 0, "counts": {}, "puntos": []}

    # Solo con coordenadas
    if "lat" in snap.columns and "lon" in snap.columns:
        snap = snap[snap["lat"].notna() & snap["lon"].notna()].copy()

    # Filtro batería
    if baterias and "nivel_5" in snap.columns:
        bat_list = [b.strip() for b in baterias.split(",") if b.strip()]
        if bat_list:
            snap = snap[snap["nivel_5"].isin(bat_list)]

    # Aplicar semáforo
    snap = apply_semaforo_aib(
        snap,
        sum_media=sum_media,
        sum_alta=sum_alta,
        llen_ok=llen_ok,
        llen_bajo=llen_bajo,
    )

    counts = get_semaforo_counts(snap)

    # DT_plot como string para JSON
    snap["DT_plot_str"] = pd.to_datetime(
        snap["DT_plot"], errors="coerce"
    ).dt.strftime("%Y-%m-%d %H:%M")

    keep = [
        c for c in [
            "NO_key", "nivel_5", "ORIGEN",
            "Sumergencia", "Sumergencia_base", "SE",
            "Semaforo_AIB", "DT_plot_str", "Dias_desde_ultima",
            "PB", "PE", "NM", "NC", "ND",
            "Bba Llenado", "%Estructura", "%Balance",
            "GPM", "Caudal bruto efec",
        ]
        if c in snap.columns
    ]

    snap_out = snap[keep].copy()

    puntos = _to_json_safe(snap_out)

    # Renombrar DT_plot_str -> DT_plot para el frontend
    for p in puntos:
        p["DT_plot"] = p.pop("DT_plot_str", None)

    return {
        "total":  len(puntos),
        "counts": counts,
        "puntos": puntos,
    }


# ==========================================================
# GET /api/mapa/stats
# ==========================================================

@router.get("/stats")
async def get_stats_mapa(
    baterias: Optional[str]   = Query(None),
    sum_min:  Optional[float] = Query(None),
    sum_max:  Optional[float] = Query(None),
    dias_min: Optional[float] = Query(None),
    dias_max: Optional[float] = Query(None),
):
    """
    Devuelve estadísticas generales del mapa para los KPIs del dashboard.

    Returns:
        {
            "kpis":    { total_pozos, ultima_din, ultima_niv, con_sumergencia, con_pb },
            "calidad": { sum_negativa, pb_anomalo, pb_faltante, pb_low, pb_high },
            "sumergencia": {
                "media":   float | None,
                "mediana": float | None,
                "min":     float | None,
                "max":     float | None,
                "p25":     float | None,
                "p75":     float | None,
            },
            "semaforo": { total_aib, normal, alerta, critico, sin_datos, no_aplica },
        }
    """
    din_ok, niv_ok, _ = _load_indexes_ok()
    snap = _build_snap_con_coords(din_ok, niv_ok)

    if snap.empty:
        return {
            "kpis":        {},
            "calidad":     {},
            "sumergencia": {},
            "semaforo":    {},
        }

    # Filtros
    snap = _apply_filtros(snap, baterias, sum_min, sum_max, dias_min, dias_max, None)

    # Solo coordenadas válidas para el mapa
    snap_geo = snap[
        snap["lat"].notna() & snap["lon"].notna()
    ].copy() if "lat" in snap.columns and "lon" in snap.columns else snap.copy()

    kpis    = get_kpis_snapshot(snap_geo)
    calidad = get_calidad_resumen(snap_geo)

    # Estadísticas de sumergencia
    sumer_stats: dict = {}
    if "Sumergencia" in snap_geo.columns:
        s = snap_geo["Sumergencia"].dropna()
        if not s.empty:
            sumer_stats = {
                "media":   round(float(s.mean()),   1),
                "mediana": round(float(s.median()), 1),
                "min":     round(float(s.min()),     1),
                "max":     round(float(s.max()),     1),
                "p25":     round(float(s.quantile(0.25)), 1),
                "p75":     round(float(s.quantile(0.75)), 1),
            }

    # Semáforo AIB
    snap_sem = apply_semaforo_aib(snap_geo)
    semaforo = get_semaforo_counts(snap_sem)

    return {
        "kpis":        kpis,
        "calidad":     calidad,
        "sumergencia": sumer_stats,
        "semaforo":    semaforo,
    }


# ==========================================================
# GET /api/mapa/pozo/{pozo}
# ==========================================================

@router.get("/pozo/{pozo}")
async def get_detalle_pozo_mapa(pozo: str):
    """
    Devuelve el detalle completo de un pozo para el popup del mapa.
    Incluye última medición, histórico de sumergencia (últimos 10 puntos),
    estado de semáforo AIB y validación.

    Path params:
        pozo: NO_key del pozo

    Returns:
        {
            "pozo":        str,
            "bateria":     str | None,
            "lat":         float | None,
            "lon":         float | None,
            "ultima":      { ORIGEN, DT_plot_str, Sumergencia, PB, NM, NC, ND, ...extras },
            "historico":   [ { dt, sumergencia, origen } ],
            "semaforo":    str,
            "validada":    bool,
        }
    """
    no_key = normalize_no_exact(pozo)
    if not no_key:
        raise HTTPException(status_code=400, detail="Pozo inválido")

    din_ok, niv_ok, col_map = _load_indexes_ok()

    # Snapshot para este pozo
    snap = _build_snap_con_coords(din_ok, niv_ok)
    snap_pozo = (
        snap[snap["NO_key"] == no_key].copy()
        if not snap.empty else pd.DataFrame()
    )

    if snap_pozo.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Pozo '{no_key}' no encontrado en el índice."
        )

    row = snap_pozo.iloc[0]

    # Coords
    lat     = safe_to_float(row.get("lat"))
    lon     = safe_to_float(row.get("lon"))
    bateria = row.get("nivel_5")

    # Semáforo AIB
    snap_sem = apply_semaforo_aib(snap_pozo)
    semaforo = snap_sem.iloc[0].get("Semaforo_AIB", "NO APLICA")

    # Validación
    val_data = __import__(
        "core.gcs", fromlist=["load_validaciones"]
    ).load_validaciones(no_key)
    dt_plot  = row.get("DT_plot")
    fecha_key = make_fecha_key(dt_plot) if dt_plot else ""
    val_estado = get_validacion(val_data, fecha_key)
    validada   = val_estado.get("validada", True)

    # Última medición
    dt_str = (
        pd.Timestamp(dt_plot).strftime("%Y-%m-%d %H:%M")
        if dt_plot and not pd.isna(dt_plot) else None
    )

    ultima = {
        "ORIGEN":          row.get("ORIGEN"),
        "DT_plot_str":     dt_str,
        "Sumergencia":     safe_to_float(row.get("Sumergencia")),
        "Sumergencia_base": row.get("Sumergencia_base"),
        "PB":              safe_to_float(row.get("PB")),
        "PE":              safe_to_float(row.get("PE")),
        "NM":              safe_to_float(row.get("NM")),
        "NC":              safe_to_float(row.get("NC")),
        "ND":              safe_to_float(row.get("ND")),
        "Dias_desde_ultima": safe_to_float(row.get("Dias_desde_ultima")),
    }

    # Extras del snapshot
    for campo in EXTRA_FIELDS:
        ultima[campo] = safe_to_float(row.get(campo))

    # Histórico de sumergencia (últimos 10 puntos)
    dfp = build_pozo_consolidado(
        din_ok, niv_ok, no_key,
        col_map["din_no_col"], col_map["din_fe_col"], col_map["din_ho_col"],
        col_map["niv_no_col"], col_map["niv_fe_col"], col_map["niv_ho_col"],
    )

    historico = []
    if not dfp.empty and "Sumergencia" in dfp.columns and "DT_plot" in dfp.columns:
        hist = (
            dfp[["DT_plot", "Sumergencia", "ORIGEN"]]
            .dropna(subset=["DT_plot", "Sumergencia"])
            .sort_values("DT_plot")
            .tail(10)
        )
        for _, hr in hist.iterrows():
            dt_h = hr["DT_plot"]
            historico.append({
                "dt":          pd.Timestamp(dt_h).strftime("%Y-%m-%d %H:%M")
                               if not pd.isna(dt_h) else None,
                "sumergencia": safe_to_float(hr["Sumergencia"]),
                "origen":      hr.get("ORIGEN", ""),
            })

    return {
        "pozo":      no_key,
        "bateria":   str(bateria) if bateria else None,
        "lat":       lat,
        "lon":       lon,
        "ultima":    ultima,
        "historico": historico,
        "semaforo":  semaforo,
        "validada":  validada,
    }
