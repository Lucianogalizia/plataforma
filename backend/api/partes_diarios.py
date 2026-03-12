from __future__ import annotations

import io
import math

import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from core.gcs import (
    get_gcs_client,
    GCS_BUCKET,
    GCS_PREFIX,
)

router = APIRouter()

PARTES_FOLDER = "partes_diarios"


def _get_all_data() -> pd.DataFrame:
    """
    Lee todos los CSV de la carpeta partes_diarios/ y los combina en un DataFrame.
    """
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return pd.DataFrame()

    prefix = f"{GCS_PREFIX}/{PARTES_FOLDER}/" if GCS_PREFIX else f"{PARTES_FOLDER}/"
    blobs  = list(client.bucket(GCS_BUCKET).list_blobs(prefix=prefix))
    csv_blobs = [b for b in blobs if b.name.endswith(".csv")]

    if not csv_blobs:
        return pd.DataFrame()

    dfs = []
    for blob in csv_blobs:
        try:
            content = blob.download_as_bytes()
            df = pd.read_csv(io.BytesIO(content), low_memory=False)
            dfs.append(df)
        except Exception:
            continue

    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)
    # Eliminar duplicados si los hay
    combined = combined.drop_duplicates()
    return combined


@router.get("/info")
async def intervenciones_info():
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return JSONResponse(content={"exists": False, "error": "GCS no configurado"})
    try:
        df = _get_all_data()
        if df.empty:
            return JSONResponse(content={
                "exists": False,
                "updated_at": None,
                "file": None,
                "size_kb": 0,
                "rows": 0,
            })
        return JSONResponse(content={
            "exists":   True,
            "rows":     len(df),
            "columns":  list(df.columns),
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={"exists": False, "error": str(e)})


@router.get("/datos")
async def intervenciones_datos(
    pozo:        str | None = None,
    fecha_desde: str | None = None,
    fecha_hasta: str | None = None,
    status:      str | None = None,
    limit:       int        = 50000,
):
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        raise HTTPException(status_code=503, detail="GCS no configurado.")

    try:
        df = _get_all_data()
        if df.empty:
            raise HTTPException(status_code=404, detail="No hay partes diarios en GCS.")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error leyendo CSV de GCS: {e}")

    if pozo and "well_legal_name" in df.columns:
        df = df[df["well_legal_name"].str.upper() == pozo.strip().upper()]

    if fecha_desde and "date_report" in df.columns:
        df = df[df["date_report"] >= fecha_desde]

    if fecha_hasta and "date_report" in df.columns:
        df = df[df["date_report"] <= fecha_hasta]

    if status and "status_end" in df.columns:
        df = df[df["status_end"].str.upper() == status.strip().upper()]

    df = df.head(limit).where(pd.notna(df), other=None)
    records = df.to_dict(orient="records")
    clean = [
        {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
        for row in records
    ]

    return JSONResponse(content={"total": len(clean), "data": clean})


@router.get("/pozos")
async def intervenciones_pozos():
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        raise HTTPException(status_code=503, detail="GCS no configurado.")
    try:
        df = _get_all_data()
        if df.empty:
            return JSONResponse(content={"pozos": []})
        pozos = (
            df.groupby("well_legal_name")["status_end"]
            .last()
            .reset_index()
            .rename(columns={"well_legal_name": "pozo", "status_end": "status"})
            .to_dict(orient="records")
        )
        return JSONResponse(content={"pozos": pozos})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))