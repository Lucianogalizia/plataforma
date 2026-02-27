# ==========================================================
# backend/core/cache.py
#
# Caché en memoria con TTL para el backend FastAPI.
# Evita recalcular snapshots y datos pesados en cada request.
#
# Uso:
#   from core.cache import cache
#
#   result = cache.get("snapshot_mapa")
#   if result is None:
#       result = calcular_algo_pesado()
#       cache.set("snapshot_mapa", result, ttl=600)
# ==========================================================

from __future__ import annotations

import time
from typing import Any


class TTLCache:
    """Caché en memoria simple con TTL por clave."""

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
        """Guarda un valor con TTL en segundos (default: 10 minutos)."""
        self._store[key] = {
            "value":      value,
            "expires_at": time.time() + ttl,
            "created_at": time.time(),
        }

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def clear(self) -> None:
        self._store.clear()

    def info(self) -> dict:
        now = time.time()
        activas = {
            k: {
                "expira_en_seg":   round(v["expires_at"] - now),
                "creado_hace_seg": round(now - v["created_at"]),
            }
            for k, v in self._store.items()
            if v["expires_at"] > now
        }
        return {
            "entradas_activas": len(activas),
            "claves": activas,
        }


# Instancia global compartida entre todos los módulos
cache = TTLCache()
