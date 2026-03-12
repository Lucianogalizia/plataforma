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


def _blob_name(filename: str) -> str:
    path = f"{PARTES_FOLDER}/{filename}"
    if GCS_PREFIX:
        return f"{GCS_PREFIX}/{path}"
    return path


def _get_latest_blob():
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return None, None

    prefix = f"{GCS_PREFIX}/{PARTES_FOLDER}/" if GCS_PREFIX else f"{PARTES_FOLDER}/"
    blobs  = list(client.bucket(GCS_BUCKET).list_blobs(prefix=prefix))

    csv_blobs = [b for b in blobs if b.name.endswith(".csv")]
    if not csv_blobs:
        return None, None

    latest = sorted(csv_blobs, key=lambda b: b.updated, reverse=True)[0]
    return client, latest


@router.get("/info")
async def intervenciones_info():
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return JSONResponse(content={"exists": False, "error": "GCS no configurado"})
    try:
        _, blob = _get_latest_blob()
        if not blob:
            return JSONResponse(content={
                "exists": False,
                "updated_at": None,
                "file": None,
                "size_kb": 0,
                "rows": 0,
            })
        blob.reload()
        content = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(content), low_memory=False)
        return JSONResponse(content={
            "exists":     True,
            "updated_at": blob.updated.isoformat() if blob.updated else None,
            "file":       blob.name,
            "size_kb":    round((blob.size or 0) / 1024, 1),
            "rows":       len(df),
            "columns":    list(df.columns),
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={"exists": False, "error": str(e)})


@router.get("/datos")
async def intervenciones_datos(
    pozo:        str | None = None,
    fecha_desde: str | None = None,
    fecha_hasta: str | None = None,
    status:      str | None = None,
    limit:       int        = 5000,
):
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        raise HTTPException(status_code=503, detail="GCS no configurado.")

    try:
        _, blob = _get_latest_blob()
        if not blob:
            raise HTTPException(status_code=404, detail="No hay partes diarios en GCS.")
        content = blob.download_as_bytes()
        df      = pd.read_csv(io.BytesIO(content), low_memory=False)
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
        _, blob = _get_latest_blob()
        if not blob:
            return JSONResponse(content={"pozos": []})
        df = pd.read_csv(io.BytesIO(blob.download_as_bytes()), low_memory=False,
                         usecols=["well_legal_name", "status_end"])
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