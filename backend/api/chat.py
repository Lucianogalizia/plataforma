# ==========================================================
# backend/api/chat.py
#
# Endpoint de chat asistido con herramientas (tool calling).
#
# El modelo SOLO responde con datos reales obtenidos via tools.
# Si no tiene datos, lo dice explícitamente — nunca inventa.
#
# Rutas:
#   POST /api/chat   → recibe mensaje, devuelve respuesta IA
#
# Tools disponibles:
#   - get_kpis_diagnosticos   → KPIs globales de diagnósticos
#   - get_pozos_criticos       → pozos con severidad CRÍTICA o ALTA
#   - get_diagnostico_pozo     → diagnóstico completo de un pozo
#   - buscar_por_problematica  → pozos con una problemática específica
#   - get_kpis_acciones        → KPIs del módulo de acciones
#   - get_acciones             → lista de acciones con filtros opcionales
# ==========================================================

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


# ==========================================================
# Modelos de entrada/salida
# ==========================================================

class ChatRequest(BaseModel):
    mensaje: str
    historial: list[dict] = []   # [{role: "user"|"assistant", content: "..."}]


class ChatResponse(BaseModel):
    respuesta: str
    tools_usadas: list[str] = []


# ==========================================================
# Definición de tools para OpenAI
# ==========================================================

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_kpis_diagnosticos",
            "description": (
                "Obtiene los KPIs globales del módulo de diagnósticos: "
                "cantidad total de pozos diagnosticados, cuántos son críticos, "
                "cuántos tienen alta severidad, y cuántos no tienen problemáticas."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_pozos_criticos",
            "description": (
                "Devuelve la lista de pozos con severidad CRÍTICA o ALTA, "
                "incluyendo su batería, las problemáticas activas y la fecha del último DIN."
                "Usar para preguntas como: '¿qué pozos son críticos?', "
                "'¿cuáles tienen problemas graves?', 'mostrame los más urgentes'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "severidad": {
                        "type": "string",
                        "enum": ["CRÍTICA", "ALTA", "MEDIA", "BAJA"],
                        "description": "Filtrar por nivel de severidad. Por defecto devuelve CRÍTICA y ALTA.",
                    }
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_diagnostico_pozo",
            "description": (
                "Obtiene el diagnóstico completo de un pozo específico: "
                "problemáticas, severidades, sumergencia, llenado, caudal y recomendación."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "pozo": {
                        "type": "string",
                        "description": "Nombre o identificador del pozo (ej: 'Pe-123', 'ai-45').",
                    }
                },
                "required": ["pozo"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "buscar_por_problematica",
            "description": (
                "Busca todos los pozos que tienen activa una problemática específica. "
                "Usar para: '¿qué pozos tienen golpeo de fondo?', "
                "'pozos con gas en bomba', 'llenado bajo activo', etc."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "problema": {
                        "type": "string",
                        "description": (
                            "Nombre o fragmento de la problemática a buscar. "
                            "Ejemplos: 'golpeo', 'gas en bomba', 'llenado bajo', "
                            "'sumergencia crítica', 'fuga válvula'."
                        ),
                    }
                },
                "required": ["problema"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_kpis_acciones",
            "description": (
                "Obtiene los KPIs del módulo de acciones de optimización: "
                "total de acciones, pendientes, en curso, completadas."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_acciones",
            "description": (
                "Lista acciones de optimización registradas. "
                "Permite filtrar por estado, batería o pozo."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "estado": {
                        "type": "string",
                        "description": "Filtrar por estado: 'Pendiente', 'En curso', 'Completada'.",
                    },
                    "bateria": {
                        "type": "string",
                        "description": "Filtrar por nombre de batería.",
                    },
                    "pozo": {
                        "type": "string",
                        "description": "Filtrar por nombre de pozo.",
                    },
                },
                "required": [],
            },
        },
    },
]


# ==========================================================
# Implementación de cada tool
# (Llama directamente a las funciones core — sin HTTP interno)
# ==========================================================

def _ejecutar_tool(nombre: str, args: dict) -> Any:
    """
    Ejecuta la tool solicitada y devuelve su resultado como dict.
    Nunca lanza excepción — siempre devuelve {"error": "..."} si falla.
    """

    # ----------------------------------------------------------
    # get_kpis_diagnosticos
    # ----------------------------------------------------------
    if nombre == "get_kpis_diagnosticos":
        try:
            from api.diagnosticos import _load_din_niv_ok, _get_bat_map
            from ia.diagnostico import (
                build_global_table, build_bat_map,
                get_kpis_global_table, get_estado_cache,
            )
            from core.gcs import load_all_diags_from_gcs
            from core.parsers import normalize_no_exact

            din_ok, _ = _load_din_niv_ok()
            if din_ok is None or din_ok.empty:
                return {"error": "No hay datos DIN disponibles."}

            pozos = sorted(din_ok["NO_key"].dropna().unique().tolist())
            diags = load_all_diags_from_gcs(pozos)
            bat_map = _get_bat_map()
            df = build_global_table(diags, bat_map, normalize_no_exact)
            return get_kpis_global_table(df)
        except Exception as e:
            return {"error": str(e)}

    # ----------------------------------------------------------
    # get_pozos_criticos
    # ----------------------------------------------------------
    elif nombre == "get_pozos_criticos":
        try:
            from api.diagnosticos import _load_din_niv_ok, _get_bat_map
            from ia.diagnostico import build_global_table
            from core.gcs import load_all_diags_from_gcs
            from core.parsers import normalize_no_exact

            severidad_filtro = args.get("severidad", None)
            severidades_buscar = (
                [severidad_filtro] if severidad_filtro
                else ["CRÍTICA", "ALTA"]
            )

            din_ok, _ = _load_din_niv_ok()
            if din_ok is None or din_ok.empty:
                return {"error": "No hay datos DIN disponibles."}

            pozos = sorted(din_ok["NO_key"].dropna().unique().tolist())
            diags = load_all_diags_from_gcs(pozos)
            bat_map = _get_bat_map()
            df = build_global_table(diags, bat_map, normalize_no_exact)

            if df.empty:
                return {"pozos": [], "total": 0}

            df_fil = df[df["Sev. máx"].isin(severidades_buscar)].copy()
            df_fil = df_fil.sort_values(["Sev. máx", "Batería", "Pozo"])

            resultado = []
            for _, row in df_fil.iterrows():
                resultado.append({
                    "pozo":          row["Pozo"],
                    "bateria":       row["Batería"],
                    "severidad":     row["Sev. máx"],
                    "fecha_din":     row["Fecha DIN"],
                    "llenado":       row["Llenado %"],
                    "sumergencia":   row["Sumergencia"],
                    "problematicas": row["Problemáticas"],
                    "recomendacion": row["Recomendación"],
                })

            return {"pozos": resultado, "total": len(resultado)}
        except Exception as e:
            return {"error": str(e)}

    # ----------------------------------------------------------
    # get_diagnostico_pozo
    # ----------------------------------------------------------
    elif nombre == "get_diagnostico_pozo":
        try:
            from core.gcs import load_diag_from_gcs
            from core.parsers import normalize_no_exact

            pozo_raw = args.get("pozo", "").strip()
            if not pozo_raw:
                return {"error": "Parámetro 'pozo' requerido."}

            no_key = normalize_no_exact(pozo_raw)
            diag = load_diag_from_gcs(no_key)

            if not diag:
                return {"error": f"No se encontró diagnóstico para el pozo '{pozo_raw}'."}
            if "error" in diag:
                return {"error": diag["error"]}

            # Extraer info relevante sin sobrecargar el contexto
            mediciones_resumidas = []
            for med in diag.get("mediciones", []):
                probs_activas = [
                    p for p in med.get("problemáticas", [])
                    if p.get("estado") == "ACTIVA"
                ]
                mediciones_resumidas.append({
                    "fecha":           med.get("fecha"),
                    "llenado_pct":     med.get("llenado_pct"),
                    "sumergencia_m":   med.get("sumergencia_m"),
                    "caudal_bruto":    med.get("caudal_bruto"),
                    "problematicas_activas": [
                        {
                            "nombre":    p.get("nombre"),
                            "severidad": p.get("severidad"),
                            "detalle":   p.get("detalle", ""),
                        }
                        for p in probs_activas
                    ],
                })

            meta = diag.get("_meta", {})
            return {
                "pozo":          no_key,
                "recomendacion": diag.get("recomendacion", "Sin recomendación."),
                "confianza":     diag.get("confianza", "N/D"),
                "generado_utc":  meta.get("generado_utc", "?")[:19],
                "mediciones":    mediciones_resumidas,
            }
        except Exception as e:
            return {"error": str(e)}

    # ----------------------------------------------------------
    # buscar_por_problematica
    # ----------------------------------------------------------
    elif nombre == "buscar_por_problematica":
        try:
            from api.diagnosticos import _load_din_niv_ok, _get_bat_map
            from ia.diagnostico import build_global_table
            from core.gcs import load_all_diags_from_gcs
            from core.parsers import normalize_no_exact

            problema = args.get("problema", "").strip().lower()
            if not problema:
                return {"error": "Parámetro 'problema' requerido."}

            din_ok, _ = _load_din_niv_ok()
            if din_ok is None or din_ok.empty:
                return {"error": "No hay datos DIN disponibles."}

            pozos = sorted(din_ok["NO_key"].dropna().unique().tolist())
            diags = load_all_diags_from_gcs(pozos)
            bat_map = _get_bat_map()
            df = build_global_table(diags, bat_map, normalize_no_exact)

            if df.empty:
                return {"pozos": [], "total": 0}

            # Buscar en la columna _prob_lista (lista de nombres de problemáticas)
            def tiene_problema(lista):
                if not isinstance(lista, list):
                    return False
                return any(problema in str(p).lower() for p in lista)

            mascara = df["_prob_lista"].apply(tiene_problema)
            df_fil = df[mascara][["Pozo", "Batería", "Fecha DIN", "Sev. máx", "Problemáticas"]].copy()

            resultado = df_fil.to_dict(orient="records")
            return {
                "problema_buscado": problema,
                "pozos":            resultado,
                "total":            len(resultado),
            }
        except Exception as e:
            return {"error": str(e)}

    # ----------------------------------------------------------
    # get_kpis_acciones
    # ----------------------------------------------------------
    elif nombre == "get_kpis_acciones":
        try:
            from core.acciones import get_kpis_acciones
            return get_kpis_acciones()
        except Exception as e:
            return {"error": str(e)}

    # ----------------------------------------------------------
    # get_acciones
    # ----------------------------------------------------------
    elif nombre == "get_acciones":
        try:
            from core.acciones import get_acciones_filtradas

            estado  = args.get("estado",  None)
            bateria = args.get("bateria", None)
            pozo    = args.get("pozo",    None)

            acciones = get_acciones_filtradas(
                estado=estado,
                bateria=bateria,
                pozo=pozo,
            )
            # Limitar a 30 resultados para no sobrecargar el contexto
            return {
                "acciones": acciones[:30],
                "total":    len(acciones),
                "nota":     "Se muestran máximo 30 resultados." if len(acciones) > 30 else "",
            }
        except Exception as e:
            return {"error": str(e)}

    return {"error": f"Tool desconocida: {nombre}"}


# ==========================================================
# System prompt
# ==========================================================

SYSTEM_PROMPT = """Sos el asistente técnico de la Plataforma DINA, 
un sistema de análisis dinamométrico de pozos petroleros.

REGLAS ESTRICTAS:
1. SOLO respondés con datos reales obtenidos de las tools disponibles.
2. Si una pregunta requiere datos que no tenés, decís exactamente: 
   "No tengo esa información disponible en el sistema."
3. NUNCA inventás valores, pozos, fechas ni problemáticas.
4. Si una tool devuelve error, lo informás claramente.
5. Respondés siempre en español, de forma concisa y técnica.
6. Para preguntas sobre pozos específicos, usás get_diagnostico_pozo.
7. Para preguntas generales sobre el estado del campo, usás get_kpis_diagnosticos primero.

Sos directo. Nada de frases de relleno. Si no sabés, lo decís."""


# ==========================================================
# POST /api/chat
# ==========================================================

@router.post("", response_model=ChatResponse)
async def chat(req: ChatRequest):
    """
    Endpoint principal del chat asistido.

    Flujo:
        1. Construye el historial de mensajes
        2. Llama a OpenAI con las tools definidas
        3. Si el modelo quiere usar una tool, la ejecuta localmente
        4. Devuelve la respuesta final al frontend
    """
    from ia.diagnostico import get_openai_key
    from openai import OpenAI

    api_key = get_openai_key()
    if not api_key:
        return ChatResponse(
            respuesta="⚠️ No hay clave OpenAI configurada en el servidor.",
            tools_usadas=[],
        )

    client = OpenAI(api_key=api_key)

    # Construir historial completo
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for msg in req.historial[-10:]:   # máximo 10 turnos de contexto
        if msg.get("role") in ("user", "assistant") and msg.get("content"):
            messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": req.mensaje})

    tools_usadas: list[str] = []

    # Primera llamada al modelo
    response = client.chat.completions.create(
        model="gpt-4o-mini",          # modelo económico y rápido para chat
        messages=messages,
        tools=TOOLS,
        tool_choice="auto",
        max_tokens=1500,
        temperature=0,                # 0 = máxima fidelidad, sin creatividad
    )

    msg_resp = response.choices[0].message

    # Si el modelo quiere usar tools, ejecutarlas
    if msg_resp.tool_calls:
        messages.append(msg_resp)     # agregar respuesta del asistente al historial

        for tc in msg_resp.tool_calls:
            nombre_tool = tc.function.name
            tools_usadas.append(nombre_tool)

            try:
                args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                args = {}

            resultado = _ejecutar_tool(nombre_tool, args)

            messages.append({
                "role":         "tool",
                "tool_call_id": tc.id,
                "content":      json.dumps(resultado, ensure_ascii=False),
            })

        # Segunda llamada con los resultados de las tools
        response2 = client.chat.completions.create(
            model="gpt-5.2",
            messages=messages,
            max_tokens=1500,
            temperature=0,
        )
        respuesta_final = response2.choices[0].message.content or ""

    else:
        # El modelo respondió sin usar tools
        respuesta_final = msg_resp.content or ""

    return ChatResponse(
        respuesta=respuesta_final.strip(),
        tools_usadas=tools_usadas,
    )
