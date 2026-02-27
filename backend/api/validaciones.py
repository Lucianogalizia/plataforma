# ==========================================================
# backend/api/validaciones.py
#
# Endpoints REST para el sistema de validaciones de sumergencias
#
# IMPORTANTE: Las rutas estáticas (tabla, historial, resumen, bulk)
# DEBEN estar definidas ANTES que /{pozo} para que FastAPI no las
# interprete como un parámetro de path.
#
# Rutas:
#   GET  /api/validaciones/tabla            → tabla completa para visualización
#   GET  /api/validaciones/historial        → historial completo para export
#   GET  /api/validaciones/resumen          → estadísticas globales
#   POST /api/validaciones/bulk             → guardar múltiples validaciones
#   GET  /api/validaciones/{pozo}           → estado de validaciones de un pozo
#   POST /api/validaciones/{pozo}           → guardar validación de una medición
# ==========================================================

from __future__ import annotations

from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from core.gcs import (
    load_validaciones,
    save_validaciones,
    load_all_validaciones,
    load_din_index,
    load_niv_index,
    load_coords_repo,
    resolve_existing_path,
)
from core.parsers import normalize_no_exact
from core.consolidado import (
    prepare_indexes,
    build_last_snapshot_for_map,
)
from core.validaciones import (
    make_fecha_key,
    get_validacion,
    get_estado_validacion,
    set_validacion,
    set_validacion_bulk,
    build_tabla_validaciones,
    build_historial_completo,
    resumen_validaciones,
)

router = APIRouter()


# ==========================================================
# Modelos Pydantic (cuerpos de POST)
# ==========================================================

class ValidacionItem(BaseModel):
    """Cuerpo para guardar una validación individual."""
    fecha_key:  str
    validada:   bool
    comentario: str  = ""
    usuario:    str  = "anónimo"


class ValidacionBulkItem(BaseModel):
    """Un cambio dentro de un bulk."""
    no_key:     str
    fecha_key:  str
    validada:   bool
    comentario: str = ""


class ValidacionBulkRequest(BaseModel):
    """Cuerpo para guardar múltiples validaciones en un solo request."""
    cambios:  list[ValidacionBulkItem]
    usuario:  str = "anónimo"


# ==========================================================
# Helpers internos
# ==========================================================

def _load_snap_map() -> pd.DataFrame:
    """
    Carga el snapshot para el mapa (1 fila por pozo).
    Usado como base para construir la tabla de validaciones.
    """
    df_din = load_din_index()
    df_niv = load_niv_index()

    if not df_din.empty and "path" in df_din.columns:
        df_din["path"] = df_din["path"].apply(
            lambda x: resolve_existing_path(x) if pd.notna(x) else None
        )

    # Forma correcta — prepare_indexes devuelve 3 valores
    df_din_k, df_niv_k, col_map = prepare_indexes(df_din, df_niv)

    din_ok = df_din_k.copy()
    niv_ok = df_niv_k.copy()

    if not din_ok.empty and "error" in din_ok.columns:
        din_ok = din_ok[din_ok["error"].isna()]
    if not niv_ok.empty and "error" in niv_ok.columns:
        niv_ok = niv_ok[niv_ok["error"].isna()]

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
        snap_map.get("Sumergencia"), errors="coerce"
    )

    # Merge coordenadas y Batería
    coords = load_coords_repo()
    if (
        not coords.empty
        and "nombre_corto" in coords.columns
        and "nivel_5" in coords.columns
    ):
        coords = coords.copy()
        coords["NO_key"] = coords["nombre_corto"].apply(normalize_no_exact)
        snap_map["NO_key"] = snap_map["NO_key"].apply(normalize_no_exact)

        snap_map = snap_map.merge(
            coords[["NO_key", "nombre_corto", "nivel_5",
                    "GEO_LATITUDE", "GEO_LONGITUDE"]],
            on="NO_key",
            how="left",
        ).rename(columns={"GEO_LATITUDE": "lat", "GEO_LONGITUDE": "lon"})

        snap_map["lat"] = pd.to_numeric(snap_map["lat"], errors="coerce")
        snap_map["lon"] = pd.to_numeric(snap_map["lon"], errors="coerce")

        if "nivel_5" in snap_map.columns:
            snap_map["nivel_5"] = snap_map["nivel_5"].astype("string").str.strip()

    return snap_map


# ==========================================================
# *** RUTAS ESTÁTICAS PRIMERO ***
# ==========================================================

# ----------------------------------------------------------
# GET /api/validaciones/tabla
# ----------------------------------------------------------

@router.get("/tabla")
async def get_tabla_validaciones(
    sum_min:  Optional[float] = Query(None),
    sum_max:  Optional[float] = Query(None),
    dias_min: Optional[float] = Query(None),
    dias_max: Optional[float] = Query(None),
    baterias: Optional[str]   = Query(
        None,
        description="Baterías separadas por coma"
    ),
    solo_validadas:    Optional[bool] = Query(None),
    solo_no_validadas: Optional[bool] = Query(None),
):
    snap_map = _load_snap_map()

    if snap_map.empty:
        return {"total": 0, "filas": []}

    if "lat" in snap_map.columns and "lon" in snap_map.columns:
        snap_map = snap_map[
            snap_map["lat"].notna() & snap_map["lon"].notna()
        ].copy()

    if "nivel_5" in snap_map.columns:
        snap_map["nivel_5"] = snap_map["nivel_5"].astype("string").str.strip()

    if baterias and "nivel_5" in snap_map.columns:
        bat_list = [b.strip() for b in baterias.split(",") if b.strip()]
        if bat_list:
            snap_map = snap_map[snap_map["nivel_5"].isin(bat_list)]

    if sum_min is not None and "Sumergencia" in snap_map.columns:
        snap_map = snap_map[
            snap_map["Sumergencia"].isna() | (snap_map["Sumergencia"] >= sum_min)
        ]
    if sum_max is not None and "Sumergencia" in snap_map.columns:
        snap_map = snap_map[
            snap_map["Sumergencia"].isna() | (snap_map["Sumergencia"] <= sum_max)
        ]
    if dias_min is not None and "Dias_desde_ultima" in snap_map.columns:
        snap_map = snap_map[snap_map["Dias_desde_ultima"] >= dias_min]
    if dias_max is not None and "Dias_desde_ultima" in snap_map.columns:
        snap_map = snap_map[snap_map["Dias_desde_ultima"] <= dias_max]

    if "Sumergencia" in snap_map.columns:
        snap_map = snap_map.sort_values(
            ["Sumergencia"], ascending=False, na_position="last"
        ).reset_index(drop=True)

    pozos_tabla = snap_map["NO_key"].dropna().unique().tolist()
    todas_val   = load_all_validaciones(pozos_tabla)

    filas = build_tabla_validaciones(snap_map, todas_val, normalize_no_exact)

    lat_map  = {}
    lon_map  = {}
    dias_map = {}

    if "lat" in snap_map.columns:
        lat_map = dict(zip(snap_map["NO_key"], snap_map["lat"]))
    if "lon" in snap_map.columns:
        lon_map = dict(zip(snap_map["NO_key"], snap_map["lon"]))
    if "Dias_desde_ultima" in snap_map.columns:
        dias_map = dict(zip(snap_map["NO_key"], snap_map["Dias_desde_ultima"]))

    for fila in filas:
        nk = fila.get("_no_key", "")
        fila["lat"]               = lat_map.get(nk)
        fila["lon"]               = lon_map.get(nk)
        fila["Dias_desde_ultima"] = dias_map.get(nk)

    if solo_validadas:
        filas = [f for f in filas if f.get("validada", True)]
    elif solo_no_validadas:
        filas = [f for f in filas if not f.get("validada", True)]

    return {"total": len(filas), "filas": filas}


# ----------------------------------------------------------
# GET /api/validaciones/historial
# ----------------------------------------------------------

@router.get("/historial")
async def get_historial_validaciones(
    pozos: Optional[str] = Query(
        None,
        description="NO_keys separados por coma. Si vacío, devuelve todos."
    ),
):
    if pozos:
        pozos_list = [
            normalize_no_exact(p.strip())
            for p in pozos.split(",")
            if p.strip()
        ]
    else:
        snap_map   = _load_snap_map()
        pozos_list = snap_map["NO_key"].dropna().unique().tolist() if not snap_map.empty else []

    if not pozos_list:
        return {"total": 0, "historial": []}

    todas_val = load_all_validaciones(pozos_list)
    historial = build_historial_completo(todas_val)

    return {"total": len(historial), "historial": historial}


# ----------------------------------------------------------
# GET /api/validaciones/resumen
# ----------------------------------------------------------

@router.get("/resumen")
async def get_resumen_validaciones():
    snap_map   = _load_snap_map()
    pozos_list = (
        snap_map["NO_key"].dropna().unique().tolist()
        if not snap_map.empty else []
    )

    if not pozos_list:
        return {
            "total_pozos":      0,
            "total_mediciones": 0,
            "validadas":        0,
            "no_validadas":     0,
            "con_comentario":   0,
            "total_cambios":    0,
        }

    todas_val = load_all_validaciones(pozos_list)
    return resumen_validaciones(todas_val)


# ----------------------------------------------------------
# POST /api/validaciones/bulk
# ----------------------------------------------------------

@router.post("/bulk")
async def post_validaciones_bulk(body: ValidacionBulkRequest):
    if not body.cambios:
        return {"ok": True, "guardados": 0, "errores": 0, "detalle": []}

    cambios_por_pozo: dict[str, list] = {}
    for c in body.cambios:
        no_key = normalize_no_exact(c.no_key)
        if not no_key:
            continue
        cambios_por_pozo.setdefault(no_key, []).append(c)

    guardados = 0
    errores   = 0
    detalle   = []

    for no_key, cambios_pozo in cambios_por_pozo.items():
        val_data = load_validaciones(no_key)

        for c in cambios_pozo:
            val_data = set_validacion(
                val_data=val_data,
                no_key=no_key,
                fecha_key=c.fecha_key,
                validada=c.validada,
                comentario=c.comentario.strip(),
                usuario=body.usuario.strip() or "anónimo",
            )

        ok = save_validaciones(no_key, val_data)

        for c in cambios_pozo:
            if ok:
                guardados += 1
                detalle.append({
                    "no_key":    no_key,
                    "fecha_key": c.fecha_key,
                    "ok":        True,
                    "error":     None,
                })
            else:
                errores += 1
                detalle.append({
                    "no_key":    no_key,
                    "fecha_key": c.fecha_key,
                    "ok":        False,
                    "error":     "Error al guardar en GCS",
                })

    return {
        "ok":        errores == 0,
        "guardados": guardados,
        "errores":   errores,
        "detalle":   detalle,
    }


# ==========================================================
# *** RUTAS DINÁMICAS AL FINAL ***
# ==========================================================

# ----------------------------------------------------------
# GET /api/validaciones/{pozo}
# ----------------------------------------------------------

@router.get("/{pozo}")
async def get_validaciones_pozo(pozo: str):
    no_key = normalize_no_exact(pozo)
    if not no_key:
        raise HTTPException(status_code=400, detail="Pozo inválido")

    val_data = load_validaciones(no_key)

    return {
        "pozo":       no_key,
        "mediciones": val_data.get("mediciones", {}),
    }


# ----------------------------------------------------------
# POST /api/validaciones/{pozo}
# ----------------------------------------------------------

@router.post("/{pozo}")
async def post_validacion_pozo(pozo: str, body: ValidacionItem):
    no_key = normalize_no_exact(pozo)
    if not no_key:
        raise HTTPException(status_code=400, detail="Pozo inválido")

    val_data = load_validaciones(no_key)

    val_data = set_validacion(
        val_data=val_data,
        no_key=no_key,
        fecha_key=body.fecha_key,
        validada=body.validada,
        comentario=body.comentario.strip(),
        usuario=body.usuario.strip() or "anónimo",
    )

    ok = save_validaciones(no_key, val_data)
    if not ok:
        raise HTTPException(
            status_code=500,
            detail="Error al guardar en GCS. Verificá la conexión."
        )

    estado = get_validacion(val_data, body.fecha_key)

    return {
        "ok":        True,
        "pozo":      no_key,
        "fecha_key": body.fecha_key,
        "estado":    estado,
    }
