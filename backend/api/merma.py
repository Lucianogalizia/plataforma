# ==========================================================
# backend/api/merma.py
#
# Endpoints para el módulo de Análisis de MERMA
#
# El dashboard HTML es generado por un script local (Jupyter)
# y subido a GCS. Este módulo lo lee y lo sirve.
#
# Endpoints:
#   GET /api/merma/info       → metadata (existe, fecha, tamaño)
#   GET /api/merma/dashboard  → sirve el HTML completo
# ==========================================================

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

from core.gcs import (
    get_gcs_client,
    GCS_BUCKET,
    GCS_PREFIX,
)

router = APIRouter()

# Ruta del blob en GCS
MERMA_BLOB = "merma/dashboard_master.html"


def _merma_blob_name() -> str:
    """Construye el nombre completo del blob respetando el prefijo."""
    if GCS_PREFIX:
        return f"{GCS_PREFIX}/{MERMA_BLOB}"
    return MERMA_BLOB


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
        blob = bucket.blob(_merma_blob_name())

        if not blob.exists():
            return JSONResponse(content={
                "exists": False,
                "updated_at": None,
                "file": _merma_blob_name(),
                "size_kb": 0,
            })

        # Recargar metadata para tener updated
        blob.reload()

        return JSONResponse(content={
            "exists": True,
            "updated_at": blob.updated.isoformat() if blob.updated else None,
            "file": _merma_blob_name(),
            "size_kb": round((blob.size or 0) / 1024, 1),
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
        blob = bucket.blob(_merma_blob_name())

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
