# =============================================================
# backend/ia/agents/agent_rrhh.py
# Agente: personal, partes de guardia, aprobaciones, consolidados.
# =============================================================
from __future__ import annotations
from typing import Any
from .base_agent import BaseAgent


class AgenteRRHH(BaseAgent):

    NOMBRE = "rrhh"

    TOOLS = [
        {"type": "function", "function": {
            "name": "get_rrhh_personal",
            "description": "Lista del personal registrado: nombre, legajo, función, líder.",
            "parameters": {"type": "object", "properties": {}, "required": []}}},

        {"type": "function", "function": {
            "name": "get_rrhh_periodos",
            "description": "Períodos disponibles en el sistema de RRHH (últimos 8 con fechas de inicio y fin).",
            "parameters": {"type": "object", "properties": {}, "required": []}}},

        {"type": "function", "function": {
            "name": "get_rrhh_parte",
            "description": "Parte de guardia de un empleado en un período: día a día con guardias (G), feriados (F), días libres (D), horas extra (HE), horas viaje (HV).",
            "parameters": {"type": "object", "properties": {
                "legajo":  {"type": "string", "description": "Legajo del empleado."},
                "periodo": {"type": "string", "description": "Período YYYY-MM. Sin valor usa el actual."},
            }, "required": ["legajo"]}}},

        {"type": "function", "function": {
            "name": "get_rrhh_bitacora",
            "description": "Historial completo de partes de un empleado: todos los períodos con su estado (BORRADOR/ENVIADO/APROBADO/RECHAZADO).",
            "parameters": {"type": "object", "properties": {
                "legajo": {"type": "string", "description": "Legajo del empleado."},
            }, "required": ["legajo"]}}},

        {"type": "function", "function": {
            "name": "get_rrhh_pendientes",
            "description": "Partes de guardia pendientes de aprobación para un líder.",
            "parameters": {"type": "object", "properties": {
                "leader_legajo": {"type": "string", "description": "Legajo del líder."},
            }, "required": ["leader_legajo"]}}},

        {"type": "function", "function": {
            "name": "get_rrhh_consolidado",
            "description": "Consolidado de partes del equipo de un líder: horas, guardias, feriados y estado por empleado.",
            "parameters": {"type": "object", "properties": {
                "leader_legajo": {"type": "string", "description": "Legajo del líder."},
                "periodo":       {"type": "string", "description": "Período YYYY-MM."},
            }, "required": ["leader_legajo"]}}},
    ]

    SYSTEM_PROMPT = """Sos el Agente de RRHH de la Plataforma DINA.
Tu especialidad es la gestión del personal: partes de guardia mensuales,
aprobaciones, bitácoras y consolidados de equipo.

REGLAS:
- Siempre usás tools para obtener datos reales.
- Para partes pendientes de un líder: get_rrhh_pendientes(leader_legajo=...).
- Para el detalle del parte de un empleado: get_rrhh_parte(legajo=...).
- Para el historial de un empleado: get_rrhh_bitacora(legajo=...).
- Respondés en español, de forma clara y concisa.

ESTADOS: BORRADOR → ENVIADO → APROBADO (o RECHAZADO)
TIPOS DE DÍA: G = guardia, F = feriado, D = día libre, HE = hora extra, HV = hora viaje."""

    def _ejecutar_tool(self, nombre: str, args: dict) -> Any:

        if nombre == "get_rrhh_personal":
            try:
                from core.rrhh_db import list_personal
                personal = list_personal()
                return {"total": len(personal), "personal": personal[:50]}
            except Exception as e:
                return {"error": f"get_rrhh_personal: {e}"}

        elif nombre == "get_rrhh_periodos":
            try:
                from core.rrhh_db import recent_periods, current_period_id
                return {"periodo_actual": current_period_id(), "periodos": recent_periods(8)}
            except Exception as e:
                return {"error": f"get_rrhh_periodos: {e}"}

        elif nombre == "get_rrhh_parte":
            try:
                from api.rrhh import _build_parte_response
                from core.rrhh_db import current_period_id
                legajo = args.get("legajo", "").strip()
                if not legajo: return {"error": "Parámetro 'legajo' requerido."}
                periodo = args.get("periodo", "").strip() or current_period_id()
                return _build_parte_response(legajo, periodo)
            except Exception as e:
                return {"error": f"get_rrhh_parte: {e}"}

        elif nombre == "get_rrhh_bitacora":
            try:
                from core.rrhh_db import list_bitacora
                legajo = args.get("legajo", "").strip()
                if not legajo: return {"error": "Parámetro 'legajo' requerido."}
                partes = list_bitacora(legajo)
                return {"legajo": legajo, "total_partes": len(partes), "partes": partes}
            except Exception as e:
                return {"error": f"get_rrhh_bitacora: {e}"}

        elif nombre == "get_rrhh_pendientes":
            try:
                from core.rrhh_db import list_pendientes_lider
                leader = args.get("leader_legajo", "").strip()
                if not leader: return {"error": "Parámetro 'leader_legajo' requerido."}
                pendientes = list_pendientes_lider(leader)
                return {"leader_legajo": leader, "total": len(pendientes), "pendientes": pendientes}
            except Exception as e:
                return {"error": f"get_rrhh_pendientes: {e}"}

        elif nombre == "get_rrhh_consolidado":
            try:
                from core.rrhh_db import get_consolidado, period_display, period_bounds, current_period_id
                leader  = args.get("leader_legajo", "").strip()
                periodo = args.get("periodo", "").strip()
                if not leader: return {"error": "Parámetro 'leader_legajo' requerido."}
                if not periodo: periodo = current_period_id()
                data = get_consolidado(leader, periodo)
                start, end = period_bounds(periodo)
                return {
                    "leader_legajo": leader,
                    "periodo": periodo,
                    "periodo_display": period_display(periodo),
                    "periodo_inicio": start.isoformat(),
                    "periodo_fin": end.isoformat(),
                    "empleados": data,
                }
            except Exception as e:
                return {"error": f"get_rrhh_consolidado: {e}"}

        return {"error": f"Tool desconocida en AgenteRRHH: {nombre}"}
