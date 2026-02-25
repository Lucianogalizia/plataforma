from __future__ import annotations

from pathlib import Path
from fastapi import APIRouter

router = APIRouter(prefix="/api/health", tags=["health"])

@router.get("/build")
def build_info():
    # Esto te dice si cache.py existe adentro del container (imagen final)
    base = Path(__file__).resolve().parent.parent  # backend/
    cache_path = base / "core" / "cache.py"
    return {
        "ok": True,
        "cache_py_exists": cache_path.exists(),
        "cache_py_path": str(cache_path),
    }
