# ==========================================================
# backend/api/merma.py
# ==========================================================

from __future__ import annotations

import io
import time

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
# CACHÉ EN MEMORIA — downtimes
# El CSV se lee de GCS una sola vez y se guarda en RAM.
# Se refresca automáticamente cada 1 hora.
# ==========================================================

_CACHE_TTL = 60 * 60  # 1 hora en segundos

_downtimes_cache: dict = {
    "df":         None,   # DataFrame completo
    "updated_at": None,   # ISO string de última modificación en GCS
    "loaded_at":  0.0,    # timestamp de cuando se cargó en memoria
}


def _load_downtimes_df() -> pd.DataFrame:
    """
    Devuelve el DataFrame de downtimes.
    Si el caché es válido (< 1 hora), lo reutiliza.
    Si no, lo descarga de GCS y lo guarda en caché.
    """
    now = time.time()
    cache = _downtimes_cache

    # Caché válido
    if cache["df"] is not None and (now - cache["loaded_at"]) < _CACHE_TTL:
        return cache["df"]

    # Descargar de GCS
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        raise HTTPException(status_code=503, detail="GCS no configurado.")

    bucket = client.bucket(GCS_BUCKET)
    blob   = bucket.blob(_downtimes_blob_name())

    if not blob.exists():
        raise HTTPException(
            status_code=404,
            detail=f"CSV no encontrado en gs://{GCS_BUCKET}/{_downtimes_blob_name()}. Ejecutá fetch_downtimes para generarlo.",
        )

    blob.reload()
    content = blob.download_as_bytes()
    df      = pd.read_csv(io.BytesIO(content), low_memory=False)

    if "FECHA DESDE" in df.columns:
        df["FECHA DESDE"] = pd.to_datetime(df["FECHA DESDE"], errors="coerce")
    if "FECHA HASTA" in df.columns:
        df["FECHA HASTA"] = pd.to_datetime(df["FECHA HASTA"], errors="coerce")

    # Guardar en caché
    cache["df"]         = df
    cache["updated_at"] = blob.updated.isoformat() if blob.updated else None
    cache["loaded_at"]  = now

    return df


# ==========================================================
# GET /api/merma/info
# ==========================================================

@router.get("/info")
async def merma_info():
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return JSONResponse(content={
            "exists": False,
            "error": "GCS no configurado",
        })

    try:
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(_merma_blob_name())

        if not blob.exists():
            return JSONResponse(content={
                "exists": False, "updated_at": None,
                "file": _merma_blob_name(), "size_kb": 0,
            })

        blob.reload()
        return JSONResponse(content={
            "exists":     True,
            "updated_at": blob.updated.isoformat() if blob.updated else None,
            "file":       _merma_blob_name(),
            "size_kb":    round((blob.size or 0) / 1024, 1),
        })

    except Exception as e:
        return JSONResponse(status_code=500, content={"exists": False, "error": str(e)})


# ==========================================================
# GET /api/merma/dashboard
# ==========================================================

@router.get("/dashboard")
async def merma_dashboard():
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        raise HTTPException(status_code=503, detail="GCS no configurado.")

    try:
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(_merma_blob_name())

        if not blob.exists():
            raise HTTPException(status_code=404, detail=f"Dashboard no encontrado en gs://{GCS_BUCKET}/{_merma_blob_name()}.")

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
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return JSONResponse(content={"exists": False, "error": "GCS no configurado"})

    try:
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(_downtimes_blob_name())

        if not blob.exists():
            return JSONResponse(content={
                "exists": False, "updated_at": None,
                "file": _downtimes_blob_name(), "size_kb": 0, "rows": 0,
            })

        blob.reload()

        # Usar caché si está disponible para no releer el CSV
        df = _downtimes_cache.get("df")
        if df is None:
            content = blob.download_as_bytes()
            df = pd.read_csv(io.BytesIO(content), low_memory=False)

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
        return JSONResponse(status_code=500, content={"exists": False, "error": str(e)})


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
    try:
        df = _load_downtimes_df()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error cargando downtimes: {e}")

    # Filtros opcionales
    if pozo and "POZO" in df.columns:
        df = df[df["POZO"].str.upper() == pozo.strip().upper()]

    if fecha_desde and "FECHA DESDE" in df.columns:
        df = df[df["FECHA DESDE"] >= pd.to_datetime(fecha_desde, errors="coerce")]

    if fecha_hasta and "FECHA DESDE" in df.columns:
        df = df[df["FECHA DESDE"] <= pd.to_datetime(fecha_hasta, errors="coerce")]

    # Serializar fechas a string
    df = df.copy()
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    df = df.head(limit)

    # Reemplazar todos los NaN/inf por None antes de serializar
    df = df.where(pd.notna(df), other=None)
    records = df.to_dict(orient="records")
    # Segunda pasada: limpiar cualquier float nan que haya quedado
    import math
    clean = [
        {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
        for row in records
    ]

    return JSONResponse(content={
        "total": len(clean),
        "data":  clean,
    })
