# =============================================================
# backend/api/chat.py  — v4 multi-agente
# Recibe el mensaje y delega al orquestador.
# orquestar() es síncrono — se corre en un thread via run_in_executor.
# =============================================================
from __future__ import annotations
import asyncio
from functools import partial

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class ChatRequest(BaseModel):
    mensaje: str
    historial: list[dict] = []


class ChatResponse(BaseModel):
    respuesta: str
    tools_usadas: list[str] = []
    agentes_usados: list[str] = []


@router.get("/debug")
async def chat_debug():
    """Verifica que cada agente pueda ejecutar una tool básica."""
    from ia.agents.orchestrator import _AGENTES

    tests = {
        "pozos":        ("get_lista_baterias",   {}),
        "diagnosticos": ("get_semaforo_aib",     {}),
        "produccion":   ("get_kpis_controles",   {}),
        "operaciones":  ("get_kpis_acciones",    {}),
        "rrhh":         ("get_rrhh_periodos",    {}),
    }

    loop = asyncio.get_event_loop()
    resultados = {}

    for nombre, (tool_name, tool_args) in tests.items():
        agente = _AGENTES[nombre]
        try:
            r = await loop.run_in_executor(None, partial(agente._ejecutar_tool, tool_name, tool_args))
            resultados[nombre] = {"ok": "error" not in r, "sample": str(r)[:200]}
        except Exception as e:
            resultados[nombre] = {"ok": False, "error": str(e)}

    return resultados


@router.post("", response_model=ChatResponse)
async def chat(req: ChatRequest):
    from ia.agents import orquestar

    loop = asyncio.get_event_loop()

    # orquestar() es síncrono (usa OpenAI blocking) — lo corremos en thread pool
    resultado = await loop.run_in_executor(
        None,
        partial(orquestar, req.mensaje, req.historial),
    )

    return ChatResponse(
        respuesta=resultado.get("respuesta", ""),
        tools_usadas=resultado.get("tools_usadas", []),
        agentes_usados=resultado.get("agentes_usados", []),
    )
