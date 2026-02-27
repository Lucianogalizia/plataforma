# ==========================================================
# backend/core/cache.py
#
# Caché en memoria con TTL para el backend FastAPI.
# Evita recalcular snapshots y datos pesados en cada request.
# ==========================================================

from __future__ import annotations

import time
from typing import Any


class TTLCache:
    def __init__(self):
        self._store: dict[str, dict] = {}

    def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        if time.time() > entry["expires_at"]:
            del self._store[key]
            return None
        return entry["value"]

    def set(self, key: str, value: Any, ttl: int = 600) -> None:
        self._store[key] = {
            "value":      value,
            "expires_at": time.time() + ttl,
            "created_at": time.time(),
        }

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def clear(self) -> None:
        self._store.clear()


# Instancia global compartida entre todos los módulos
cache = TTLCache()
