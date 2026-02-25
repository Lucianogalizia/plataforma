# ==========================================================
# backend/core/cache.py
#
# Cache TTL en memoria (por instancia).
# Útil en Cloud Run y local para evitar recalcular/leer cosas
# pesadas en cada request.
# ==========================================================

from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, Tuple

class TTLCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str) -> Any | None:
        now = time.time()
        with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            exp, value = item
            if exp < now:
                self._data.pop(key, None)
                return None
            return value

    def set(self, key: str, value: Any, ttl_s: int) -> Any:
        exp = time.time() + max(1, int(ttl_s))
        with self._lock:
            self._data[key] = (exp, value)
        return value

    def get_or_set(self, key: str, loader: Callable[[], Any], ttl_s: int) -> Any:
        hit = self.get(key)
        if hit is not None:
            return hit
        value = loader()
        return self.set(key, value, ttl_s)

CACHE = TTLCache()

def ttl_get(key: str, loader: Callable[[], Any], ttl_s: int) -> Any:
    return CACHE.get_or_set(key, loader, ttl_s)
