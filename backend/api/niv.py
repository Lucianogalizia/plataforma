# ==========================================================
# backend/api/niv.py
#
# Endpoints REST para datos NIV (niveles de fluido)
#
# Rutas:
#   GET /api/niv/pozos          → pozos con datos NIV
#   GET /api/niv/mediciones/{pozo} → mediciones NIV de un pozo
#   GET /api/niv/historico/{pozo}  → serie temporal NIV
# ==========================================================

from __future__ import annotations

import pandas as pd
from fastapi import APIRouter, HTTPException

from core.gcs import load_niv_index, load_coords_repo
from core.parsers import normalize_no_exact, find_col, safe_to_float
from core.consolidado import prepare_indexes

router = APIRouter()


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    df = df.copy()
    for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S").where(df[col].notna(), None)
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


@router.get("/pozos")
async def get_pozos_niv():
    """Lista pozos con datos NIV disponibles."""
    df_niv = load_niv_index()
    if df_niv.empty:
        return {"pozos": [], "total": 0}
    no_col = find_col(df_niv, ["pozo", "NO"])
    if not no_col:
        return {"pozos": [], "total": 0}
    pozos = sorted(
        df_niv[no_col].dropna().apply(normalize_no_exact).loc[lambda s: s != ""].unique().tolist()
    )
    return {"pozos": pozos, "total": len(pozos)}


@router.get("/mediciones/{pozo}")
async def get_mediciones_niv(pozo: str):
    """Devuelve las mediciones NIV de un pozo."""
    df_niv = load_niv_index()
    if df_niv.empty:
        raise HTTPException(status_code=404, detail="No hay índice NIV")

    pozo_norm = normalize_no_exact(pozo)
    no_col = find_col(df_niv, ["pozo", "NO"])
    if not no_col:
        return {"pozo": pozo_norm, "total": 0, "mediciones": []}

    df_niv["NO_key"] = df_niv[no_col].apply(normalize_no_exact)
    df_p = df_niv[df_niv["NO_key"] == pozo_norm].copy()

    # Filtrar errores
    if "error" in df_p.columns:
        df_p = df_p[df_p["error"].isna()]

    # Ordenar por fecha
    sort_cols = [c for c in ["niv_datetime", "mtime"] if c in df_p.columns]
    if sort_cols:
        df_p = df_p.sort_values(sort_cols, na_position="last")

    # Deduplar
    df_p = df_p.drop_duplicates(
        subset=[c for c in ["NO_key", "FE_key", "HO_key"] if c in df_p.columns],
        keep="last"
    ).reset_index(drop=True)

    # Niveles numéricos
    for c in ["NM", "NC", "ND", "PE", "PB"]:
        if c in df_p.columns:
            df_p[c] = df_p[c].apply(safe_to_float)

    return {
        "pozo":      pozo_norm,
        "total":     len(df_p),
        "mediciones": _df_to_records(df_p),
    }


@router.get("/historico/{pozo}")
async def get_historico_niv(pozo: str):
    """Devuelve la serie temporal NIV de un pozo."""
    df_niv = load_niv_index()
    if df_niv.empty:
        return {"pozo": pozo, "serie": []}

    pozo_norm = normalize_no_exact(pozo)
    no_col = find_col(df_niv, ["pozo", "NO"])
    if not no_col:
        return {"pozo": pozo_norm, "serie": []}

    df_niv["NO_key"] = df_niv[no_col].apply(normalize_no_exact)
    df_p = df_niv[df_niv["NO_key"] == pozo_norm].copy()
    if df_p.empty:
        return {"pozo": pozo_norm, "serie": []}

    dt_col = next((c for c in ["niv_datetime", "mtime"] if c in df_p.columns), None)
    if not dt_col:
        return {"pozo": pozo_norm, "serie": []}

    df_p["_dt"] = pd.to_datetime(df_p[dt_col], errors="coerce")
    df_p = df_p.dropna(subset=["_dt"]).sort_values("_dt")

    for c in ["NM", "NC", "ND", "PE", "PB"]:
        if c in df_p.columns:
            df_p[c] = df_p[c].apply(safe_to_float)

    # Calcular Sumergencia
    def calc_sumer(row):
        pb = safe_to_float(row.get("PB"))
        if pb is None:
            return None, None
        for nk in ["NC", "NM", "ND"]:
            nv = safe_to_float(row.get(nk))
            if nv is not None:
                return pb - nv, nk
        return None, None

    serie = []
    for _, row in df_p.iterrows():
        s, base = calc_sumer(row)
        serie.append({
            "dt":          row["_dt"].isoformat(),
            "NM":          safe_to_float(row.get("NM")),
            "NC":          safe_to_float(row.get("NC")),
            "ND":          safe_to_float(row.get("ND")),
            "PE":          safe_to_float(row.get("PE")),
            "PB":          safe_to_float(row.get("PB")),
            "sumergencia": s,
            "base":        base,
        })

    return {"pozo": pozo_norm, "serie": serie}
