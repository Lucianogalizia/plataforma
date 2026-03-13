# =============================================================
# backend/ia/agents/orchestrator.py
# Orquestador central: clasifica intención, despacha agentes,
# sintetiza la respuesta final.
# =============================================================
from __future__ import annotations
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from .agent_pozos        import AgentePozos
from .agent_diagnosticos import AgenteDiagnosticos
from .agent_produccion   import AgenteProduccion
from .agent_operaciones  import AgenteOperaciones
from .agent_rrhh         import AgenteRRHH


# ─── Instancias singleton ────────────────────────────────────────────────────
_AGENTES = {
    "pozos":        AgentePozos(),
    "diagnosticos": AgenteDiagnosticos(),
    "produccion":   AgenteProduccion(),
    "operaciones":  AgenteOperaciones(),
    "rrhh":         AgenteRRHH(),
}

# Un solo executor compartido por proceso
_EXECUTOR = ThreadPoolExecutor(max_workers=5, thread_name_prefix="dina_agent")


# ─── Prompt de clasificación ─────────────────────────────────────────────────
_CLASSIFY_PROMPT = """Sos el clasificador de intención de la Plataforma DINA.
Tu único trabajo es determinar qué dominios cubre la pregunta del usuario.

DOMINIOS disponibles:
- "pozos":        mediciones DIN/NIV, sumergencia, llenado, caudal, snapshot, tendencias, baterías, cobertura, sistema
- "diagnosticos": diagnósticos IA, problemáticas, severidades, semáforo AIB, validaciones, pozos críticos
- "produccion":   controles de producción, merma, pérdidas, downtimes, alertas de llenado, alertas de presión
- "operaciones":  acciones de optimización, partes diarios de torre, intervenciones, instalación de fondo
- "rrhh":         personal, partes de guardia, aprobaciones, bitácoras, consolidados de equipo

REGLA: Respondé ÚNICAMENTE con un JSON array con los dominios relevantes.
Ejemplos:
- "¿qué pozos son críticos?" → ["diagnosticos"]
- "sumergencia de BB-106" → ["pozos"]
- "diagnóstico y producción de BB-106" → ["diagnosticos", "pozos", "produccion"]
- "partes pendientes de aprobar" → ["rrhh"]
- "estado del campo y acciones en proceso" → ["pozos", "operaciones"]
- "cuánto petróleo perdimos este mes" → ["produccion"]
- "qué intervenciones hubo en enero" → ["operaciones"]
- saludos o preguntas generales → ["pozos"]

NO incluyas texto extra. Solo el JSON array."""


def _clasificar_intencion(mensaje: str, historial: list[dict]) -> list[str]:
    """Llama al LLM para clasificar qué agentes necesita el mensaje."""
    try:
        from ia.diagnostico import get_openai_key
        from openai import OpenAI
        api_key = get_openai_key()
        if not api_key:
            return ["pozos"]

        client = OpenAI(api_key=api_key)
        context_msgs = []
        for msg in (historial or [])[-4:]:
            if msg.get("role") in ("user", "assistant") and msg.get("content"):
                context_msgs.append({"role": msg["role"], "content": msg["content"]})

        messages = [
            {"role": "system", "content": _CLASSIFY_PROMPT},
            *context_msgs,
            {"role": "user", "content": mensaje},
        ]
        resp = client.chat.completions.create(
            model="gpt-4o-mini", messages=messages, max_tokens=60, temperature=0,
        )
        raw = (resp.choices[0].message.content or "").strip()
        agentes = json.loads(raw)
        validos = [a for a in agentes if a in _AGENTES]
        return validos if validos else ["pozos"]
    except Exception:
        return ["pozos"]


def _correr_agente(nombre: str, mensaje: str, historial: list[dict]) -> dict:
    """Corre un agente y retorna su respuesta con metadata."""
    try:
        resultado = _AGENTES[nombre].consultar(mensaje, historial)
        return {"agente": nombre, **resultado}
    except Exception as e:
        return {"agente": nombre, "respuesta": f"Error en agente {nombre}: {e}", "tools_usadas": []}


def _sintetizar(mensaje: str, respuestas: list[dict]) -> str:
    """
    Si hay más de un agente activo, sintetiza las respuestas parciales.
    Si hay una sola, la devuelve directamente.
    """
    validas = [r for r in respuestas if r.get("respuesta") and not r["respuesta"].startswith("Error en agente")]
    if not validas:
        return "No se pudo obtener información en este momento."
    if len(validas) == 1:
        return validas[0]["respuesta"]

    try:
        from ia.diagnostico import get_openai_key
        from openai import OpenAI
        api_key = get_openai_key()
        if not api_key:
            return "\n\n---\n\n".join(r["respuesta"] for r in validas)

        client = OpenAI(api_key=api_key)
        partes = "\n\n".join(
            f"[{r['agente'].upper()}]\n{r['respuesta']}" for r in validas
        )
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": (
                    "Sos el asistente técnico de la Plataforma DINA. "
                    "Recibís las respuestas de múltiples agentes especializados e integrás todo "
                    "en una única respuesta coherente en español. "
                    "No repitas información duplicada. Usá markdown cuando ayude."
                )},
                {"role": "user", "content": (
                    f"Pregunta: {mensaje}\n\n"
                    f"Respuestas de los agentes:\n{partes}\n\n"
                    "Integrá en una sola respuesta clara y completa."
                )},
            ],
            max_tokens=1500,
            temperature=0,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception:
        return "\n\n".join(r["respuesta"] for r in validas)


def orquestar(mensaje: str, historial: list[dict] | None = None) -> dict:
    """
    Punto de entrada del sistema multi-agente.
    Corre todo de forma síncrona — FastAPI lo llama desde un thread
    usando run_in_executor, así que no hay conflicto de event loop.

    Retorna: {"respuesta": str, "tools_usadas": list[str], "agentes_usados": list[str]}
    """
    historial = historial or []

    # 1. Clasificar intención
    agentes_requeridos = _clasificar_intencion(mensaje, historial)

    # 2. Invocar agentes
    if len(agentes_requeridos) == 1:
        # Un solo agente: llamada directa, sin overhead
        respuestas = [_correr_agente(agentes_requeridos[0], mensaje, historial)]
    else:
        # Múltiples agentes: en paralelo con ThreadPoolExecutor
        futures = {
            _EXECUTOR.submit(_correr_agente, nombre, mensaje, historial): nombre
            for nombre in agentes_requeridos
        }
        respuestas = []
        for future in as_completed(futures):
            try:
                respuestas.append(future.result())
            except Exception as e:
                nombre = futures[future]
                respuestas.append({"agente": nombre, "respuesta": f"Error: {e}", "tools_usadas": []})

    # 3. Sintetizar
    respuesta_final = _sintetizar(mensaje, respuestas)

    tools_usadas   = []
    agentes_usados = []
    for r in respuestas:
        tools_usadas.extend(r.get("tools_usadas", []))
        if r.get("tools_usadas"):
            agentes_usados.append(r["agente"])

    return {
        "respuesta":      respuesta_final,
        "tools_usadas":   tools_usadas,
        "agentes_usados": agentes_usados,
    }
