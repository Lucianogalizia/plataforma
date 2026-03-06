# ==========================================================
# backend/api/merma.py
#
# Endpoints para el módulo de Análisis de MERMA
#
# El dashboard HTML es generado por un script local (Jupyter)
# y subido a GCS. Este módulo lo lee y lo sirve.
#
# Endpoints:
#   GET /api/merma/info              → metadata dashboard HTML
#   GET /api/merma/dashboard         → sirve el HTML completo
#   GET /api/merma/downtimes/info    → metadata CSV histórico de pérdidas
#   GET /api/merma/downtimes         → datos JSON con filtros opcionales
# ==========================================================

from __future__ import annotations

import io

import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

from core.gcs import (
    get_gcs_client,
    GCS_BUCKET,
    GCS_PREFIX,
)

router = APIRouter()

# ── Blobs en GCS ──────────────────────────────────────────
MERMA_BLOB     = "merma/dashboard_master.html"
DOWNTIMES_BLOB = "merma/wellDowntimes_CRUDO.csv"


def _merma_blob_name() -> str:
    if GCS_PREFIX:
        return f"{GCS_PREFIX}/{MERMA_BLOB}"
    return MERMA_BLOB


def _downtimes_blob_name() -> str:
    if GCS_PREFIX:
        return f"{GCS_PREFIX}/{DOWNTIMES_BLOB}"
    return DOWNTIMES_BLOB


# ==========================================================
# GET /api/merma/info
# ==========================================================

@router.get("/info")
async def merma_info():
    """
    Devuelve metadata del dashboard de MERMA en GCS.
    Si no existe, devuelve exists=false.
    """
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return JSONResponse(content={
            "exists": False,
            "error": "GCS no configurado (DINAS_BUCKET vacío o google-cloud-storage no disponible)",
        })

    try:
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(_merma_blob_name())

        if not blob.exists():
            return JSONResponse(content={
                "exists":     False,
                "updated_at": None,
                "file":       _merma_blob_name(),
                "size_kb":    0,
            })

        blob.reload()

        return JSONResponse(content={
            "exists":     True,
            "updated_at": blob.updated.isoformat() if blob.updated else None,
            "file":       _merma_blob_name(),
            "size_kb":    round((blob.size or 0) / 1024, 1),
        })

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"exists": False, "error": str(e)},
        )


# ==========================================================
# GET /api/merma/dashboard
# ==========================================================

@router.get("/dashboard")
async def merma_dashboard():
    """
    Sirve el HTML del dashboard de MERMA directamente desde GCS.
    Se devuelve como HTMLResponse para que el frontend lo muestre en un iframe.
    """
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        raise HTTPException(
            status_code=503,
            detail="GCS no configurado. Verificá la variable DINAS_BUCKET.",
        )

    try:
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(_merma_blob_name())

        if not blob.exists():
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Dashboard no encontrado en gs://{GCS_BUCKET}/{_merma_blob_name()}. "
                    "Ejecutá el script de generación para subirlo."
                ),
            )

        html_content = blob.download_as_text(encoding="utf-8")
        return HTMLResponse(content=html_content)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error leyendo dashboard de GCS: {e}")


# ==========================================================
# GET /api/merma/downtimes/info
# ==========================================================

@router.get("/downtimes/info")
async def downtimes_info():
    """
    Metadata del CSV de pérdidas en GCS:
    existe, fecha de última actualización, tamaño, cantidad de filas, rango de fechas.
    """
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return JSONResponse(content={
            "exists": False,
            "error":  "GCS no configurado",
        })

    try:
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(_downtimes_blob_name())

        if not blob.exists():
            return JSONResponse(content={
                "exists":     False,
                "updated_at": None,
                "file":       _downtimes_blob_name(),
                "size_kb":    0,
                "rows":       0,
            })

        blob.reload()
        content = blob.download_as_bytes()
        df      = pd.read_csv(io.BytesIO(content), low_memory=False)

        fecha_min = None
        fecha_max = None
        if "FECHA DESDE" in df.columns:
            fechas    = pd.to_datetime(df["FECHA DESDE"], errors="coerce").dropna()
            fecha_min = str(fechas.min()) if not fechas.empty else None
            fecha_max = str(fechas.max()) if not fechas.empty else None

        return JSONResponse(content={
            "exists":     True,
            "updated_at": blob.updated.isoformat() if blob.updated else None,
            "file":       _downtimes_blob_name(),
            "size_kb":    round((blob.size or 0) / 1024, 1),
            "rows":       len(df),
            "columns":    list(df.columns),
            "fecha_min":  fecha_min,
            "fecha_max":  fecha_max,
        })

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"exists": False, "error": str(e)},
        )


# ==========================================================
# GET /api/merma/downtimes
# ==========================================================

@router.get("/downtimes")
async def downtimes_data(
    pozo:        str | None = None,
    fecha_desde: str | None = None,
    fecha_hasta: str | None = None,
    limit:       int        = 5000,
):
    """
    Devuelve el histórico de pérdidas como JSON.

    Query params opcionales:
      - pozo        → filtrar por nombre de pozo (ej: YPF.SC.BB-63)
      - fecha_desde → filtrar desde esta fecha (YYYY-MM-DD)
      - fecha_hasta → filtrar hasta esta fecha (YYYY-MM-DD)
      - limit       → máximo de filas a devolver (default 5000)
    """
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        raise HTTPException(
            status_code=503,
            detail="GCS no configurado. Verificá la variable DINAS_BUCKET.",
        )

    try:
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(_downtimes_blob_name())

        if not blob.exists():
            raise HTTPException(
                status_code=404,
                detail=(
                    f"CSV no encontrado en gs://{GCS_BUCKET}/{_downtimes_blob_name()}. "
                    "Ejecutá fetch_downtimes para generarlo."
                ),
            )

        content = blob.download_as_bytes()
        df      = pd.read_csv(io.BytesIO(content), low_memory=False)

        # Parsear fechas
        if "FECHA DESDE" in df.columns:
            df["FECHA DESDE"] = pd.to_datetime(df["FECHA DESDE"], errors="coerce")

        # Filtros opcionales
        if pozo and "POZO" in df.columns:
            df = df[df["POZO"].str.upper() == pozo.strip().upper()]

        if fecha_desde and "FECHA DESDE" in df.columns:
            df = df[df["FECHA DESDE"] >= pd.to_datetime(fecha_desde, errors="coerce")]

        if fecha_hasta and "FECHA DESDE" in df.columns:
            df = df[df["FECHA DESDE"] <= pd.to_datetime(fecha_hasta, errors="coerce")]

        # Serializar fechas a string
        for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        df = df.head(limit)

        return JSONResponse(content={
            "total": len(df),
            "data":  df.where(pd.notna(df), None).to_dict(orient="records"),
        })

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error leyendo downtimes de GCS: {e}")
