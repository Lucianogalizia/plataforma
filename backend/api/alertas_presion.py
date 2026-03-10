# ==========================================================
# backend/api/alertas_presion.py
#
# Endpoints para el módulo de Predicción Alta Presión
#
# El dashboard HTML es generado por presion_final_completo.py
# (se corre en Jupyter local) y subido a GCS.
# Este módulo lo lee y lo sirve.
#
# Endpoints:
#   GET /api/alertas-presion/info       → metadata (existe, fecha, tamaño)
#   GET /api/alertas-presion/dashboard  → sirve el HTML completo
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

# Ruta del blob en GCS — tiene que coincidir con GCS_BLOB_NAME en presion_final_completo.py
ALERTAS_BLOB = "merma/alertas_presion.html"


def _blob_name() -> str:
    if GCS_PREFIX:
        return f"{GCS_PREFIX}/{ALERTAS_BLOB}"
    return ALERTAS_BLOB


# ==========================================================
# GET /api/alertas-presion/info
# ==========================================================

@router.get("/info")
async def alertas_presion_info():
    """
    Devuelve metadata del dashboard de Predicción Alta Presión en GCS.
    """
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return JSONResponse(content={
            "exists": False,
            "error": "GCS no configurado (DINAS_BUCKET vacío o google-cloud-storage no disponible)",
        })

    try:
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(_blob_name())

        if not blob.exists():
            return JSONResponse(content={
                "exists":     False,
                "updated_at": None,
                "file":       _blob_name(),
                "size_kb":    0,
            })

        blob.reload()

        return JSONResponse(content={
            "exists":     True,
            "updated_at": blob.updated.isoformat() if blob.updated else None,
            "file":       _blob_name(),
            "size_kb":    round((blob.size or 0) / 1024, 1),
        })

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"exists": False, "error": str(e)},
        )


# ==========================================================
# GET /api/alertas-presion/dashboard
# ==========================================================

@router.get("/dashboard")
async def alertas_presion_dashboard():
    """
    Sirve el HTML del dashboard de Predicción Alta Presión desde GCS.
    """
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        raise HTTPException(
            status_code=503,
            detail="GCS no configurado. Verificá la variable DINAS_BUCKET.",
        )

    try:
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(_blob_name())

        if not blob.exists():
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Dashboard no encontrado en gs://{GCS_BUCKET}/{_blob_name()}. "
                    "Ejecutá presion_final_completo.py para subirlo."
                ),
            )

        html_content = blob.download_as_text(encoding="utf-8")
        return HTMLResponse(content=html_content)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error leyendo dashboard de GCS: {e}")
