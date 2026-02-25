import os
from typing import List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.requests import Request

from core.settings import settings

# ✅ CAMBIO: antes era routers.*, ahora api.*
from api.din import router as din_router
from api.niv import router as niv_router
from api.mapa import router as mapa_router
from api.validaciones import router as validaciones_router
from api.diagnosticos import router as diagnosticos_router

app = FastAPI(
    title="DINA Backend",
    version="1.0.0",
)

def _parse_cors_origins(value: str | None) -> List[str]:
    """
    Permite:
    - "https://a.com,https://b.com"
    - '["https://a.com","https://b.com"]'
    - vacío -> []
    """
    if not value:
        return []
    v = value.strip()
    if not v:
        return []
    if v.startswith("[") and v.endswith("]"):
        # Intento simple de parseo tipo JSON sin depender de json (por compat)
        v2 = v.strip("[]").strip()
        if not v2:
            return []
        parts = [p.strip().strip('"').strip("'") for p in v2.split(",")]
        return [p for p in parts if p]
    parts = [p.strip() for p in v.split(",")]
    return [p for p in parts if p]

cors_env = os.getenv("CORS_ORIGINS", "")
cors_origins = _parse_cors_origins(cors_env)

if cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
else:
    # si no seteás CORS_ORIGINS, por defecto no abrimos a todo el mundo
    pass


@app.get("/health")
def health():
    return {"status": "ok"}


# Routers
app.include_router(din_router, prefix="/din", tags=["din"])
app.include_router(niv_router, prefix="/niv", tags=["niv"])
app.include_router(mapa_router, prefix="/mapa", tags=["mapa"])
app.include_router(validaciones_router, prefix="/validaciones", tags=["validaciones"])
app.include_router(diagnosticos_router, prefix="/diagnosticos", tags=["diagnosticos"])


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    # Esto ayuda a que si algo revienta, quede logueado y no “silencioso”
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
    )
