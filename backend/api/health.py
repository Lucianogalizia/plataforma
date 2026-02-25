from __future__ import annotations

import os
from pathlib import Path
from fastapi import APIRouter

router = APIRouter(prefix="/api/health", tags=["health"])


@router.get("/build")
async def build_info():
    """
    Diagnóstico para ver desde la app:
    - si cache.py existe dentro de la imagen (build context OK)
    - qué versión de app está corriendo (opcional)
    - y datos de instancia
    """
    app_dir = Path(__file__).resolve().parent.parent  # /app (aprox)
    cache_path = app_dir / "core" / "cache.py"

    return {
        "ok": True,
        "cache_py_exists": cache_path.exists(),
        "cache_py_path": str(cache_path),
        "cwd": os.getcwd(),
        "hostname": os.environ.get("HOSTNAME"),
        "revision": os.environ.get("K_REVISION"),  # Cloud Run
        "service": os.environ.get("K_SERVICE"),
    }
