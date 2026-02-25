from __future__ import annotations

from pathlib import Path
from fastapi import APIRouter

router = APIRouter(prefix="/api/health", tags=["Sistema"])

@router.get("/build")
def build_info():
    base = Path(__file__).resolve().parent.parent  # backend/
    cache_path = base / "core" / "cache.py"
    return {
        "ok": True,
        "cache_py_exists": cache_path.exists(),
        "cache_py_path": str(cache_path),
    }
