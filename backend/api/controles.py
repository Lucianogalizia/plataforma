# ==========================================================
# backend/api/controles.py
#
# Endpoints para Controles Históricos de Producción
#
# GET /api/controles/info           → metadata del dataset
# GET /api/controles/historico      → todos los controles (filtrable)
# GET /api/controles/merma          → resumen de merma por pozo
# ==========================================================

from __future__ import annotations

import io
import math

import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from core.gcs import get_gcs_client, GCS_BUCKET, GCS_PREFIX

router = APIRouter()

HISTORICO_BLOB = "controles/historico_CRUDO.csv"
MERMA_BLOB     = "controles/merma_por_pozo.csv"


def _blob(name: str) -> str:
    if GCS_PREFIX:
        return f"{GCS_PREFIX}/{name}"
    return name


def _clean(records: list) -> list:
    """Elimina NaN/Inf para serialización JSON segura."""
    return [
        {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
        for row in records
    ]


def _read_csv(blob_name: str) -> pd.DataFrame:
    """Lee un CSV desde GCS. Lanza HTTPException si no está disponible."""
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        raise HTTPException(status_code=503, detail="GCS no configurado.")
    bucket = client.bucket(GCS_BUCKET)
    blob   = bucket.blob(_blob(blob_name))
    if not blob.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Archivo no encontrado: {_blob(blob_name)}. Ejecutá fetch_controles para generarlo."
        )
    content = blob.download_as_bytes()
    return pd.read_csv(io.BytesIO(content), low_memory=False)


# ==========================================================
# GET /api/controles/info
# ==========================================================

@router.get("/info")
async def controles_info():
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return JSONResponse(content={"exists": False, "error": "GCS no configurado"})
    try:
        bucket = client.bucket(GCS_BUCKET)

        # Info del histórico
        blob_h = bucket.blob(_blob(HISTORICO_BLOB))
        blob_m = bucket.blob(_blob(MERMA_BLOB))

        historico_exists = blob_h.exists()
        merma_exists     = blob_m.exists()

        updated_at = None
        rows       = 0
        fecha_min  = None
        fecha_max  = None
        pozos      = 0
        en_merma   = 0

        if historico_exists:
            blob_h.reload()
            updated_at = blob_h.updated.isoformat() if blob_h.updated else None
            content    = blob_h.download_as_bytes()
            df         = pd.read_csv(io.BytesIO(content), low_memory=False)
            rows       = len(df)
            if "Fecha y Hora" in df.columns:
                fechas    = pd.to_datetime(df["Fecha y Hora"], errors="coerce").dropna()
                fecha_min = str(fechas.min().date()) if not fechas.empty else None
                fecha_max = str(fechas.max().date()) if not fechas.empty else None

        if merma_exists:
            content_m = blob_m.download_as_bytes()
            df_m      = pd.read_csv(io.BytesIO(content_m), low_memory=False)
            pozos     = len(df_m)
            if "EN_MERMA_NETA" in df_m.columns:
                en_merma = int(df_m["EN_MERMA_NETA"].sum())

        return JSONResponse(content={
            "exists":     historico_exists,
            "updated_at": updated_at,
            "rows":       rows,
            "pozos":      pozos,
            "en_merma":   en_merma,
            "fecha_min":  fecha_min,
            "fecha_max":  fecha_max,
        })
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"exists": False, "error": str(e)})


# ==========================================================
# GET /api/controles/historico
# ==========================================================

@router.get("/historico")
async def controles_historico(
    pozo:         str | None = None,
    bateria:      str | None = None,
    estado_pozo:  str | None = None,
    fecha_desde:  str | None = None,
    fecha_hasta:  str | None = None,
    limit:        int        = 10000,
):
    try:
        df = _read_csv(HISTORICO_BLOB)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Parsear fecha
    if "Fecha y Hora" in df.columns:
        df["Fecha y Hora"] = pd.to_datetime(df["Fecha y Hora"], errors="coerce")

    # Filtros
    if pozo and "Pozo" in df.columns:
        df = df[df["Pozo"].str.upper() == pozo.strip().upper()]
    if bateria and "BATERIA" in df.columns:
        df = df[df["BATERIA"].str.upper() == bateria.strip().upper()]
    if estado_pozo and "ESTADO_POZO" in df.columns:
        df = df[df["ESTADO_POZO"].str.upper() == estado_pozo.strip().upper()]
    if fecha_desde and "Fecha y Hora" in df.columns:
        df = df[df["Fecha y Hora"] >= pd.to_datetime(fecha_desde, errors="coerce")]
    if fecha_hasta and "Fecha y Hora" in df.columns:
        df = df[df["Fecha y Hora"] <= pd.to_datetime(fecha_hasta, errors="coerce")]

    # Formatear fechas para JSON
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    df = df.head(limit)
    df = df.where(pd.notna(df), other=None)
    records = _clean(df.to_dict(orient="records"))

    return JSONResponse(content={"total": len(records), "data": records})


# ==========================================================
# GET /api/controles/merma
# ==========================================================

@router.get("/merma")
async def controles_merma(
    solo_merma:   bool       = False,
    bateria:      str | None = None,
    estado_pozo:  str | None = None,
    limit:        int        = 5000,
):
    try:
        df = _read_csv(MERMA_BLOB)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if solo_merma and "EN_MERMA_NETA" in df.columns:
        df = df[df["EN_MERMA_NETA"] == True]
    if bateria and "BATERIA" in df.columns:
        df = df[df["BATERIA"].str.upper() == bateria.strip().upper()]
    if estado_pozo and "ESTADO_POZO" in df.columns:
        df = df[df["ESTADO_POZO"].str.upper() == estado_pozo.strip().upper()]

    df = df.head(limit)
    df = df.where(pd.notna(df), other=None)
    records = _clean(df.to_dict(orient="records"))

    return JSONResponse(content={"total": len(records), "data": records})
