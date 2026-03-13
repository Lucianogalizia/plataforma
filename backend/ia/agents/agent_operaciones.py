# =============================================================
# backend/ia/agents/agent_operaciones.py
# Agente: acciones de optimización, partes diarios, instalación de fondo.
# =============================================================
from __future__ import annotations
from typing import Any
import pandas as pd
from .base_agent import BaseAgent, buscar_pozo_fuzzy, clean_records


class AgenteOperaciones(BaseAgent):

    NOMBRE = "operaciones"

    TOOLS = [
        {"type": "function", "function": {
            "name": "get_kpis_acciones",
            "description": "KPIs de acciones de optimización: total registradas, en proceso, finalizadas.",
            "parameters": {"type": "object", "properties": {}, "required": []}}},

        {"type": "function", "function": {
            "name": "get_acciones",
            "description": "Lista de acciones de optimización. Filtrable por estado, batería o nombre de pozo.",
            "parameters": {"type": "object", "properties": {
                "nombre_pozo": {"type": "string", "description": "Nombre del pozo."},
                "bateria":     {"type": "string", "description": "Nombre de batería."},
                "estado":      {"type": "string", "description": "Estado de la acción."},
            }, "required": []}}},

        {"type": "function", "function": {
            "name": "get_partes_diarios",
            "description": "Partes diarios de torre: intervenciones en pozos (workover, pulling, servicios). Filtrable por pozo, fecha y status.",
            "parameters": {"type": "object", "properties": {
                "pozo":        {"type": "string", "description": "Nombre del pozo (well_legal_name)."},
                "fecha_desde": {"type": "string", "description": "Fecha desde YYYY-MM-DD."},
                "fecha_hasta": {"type": "string", "description": "Fecha hasta YYYY-MM-DD."},
                "status":      {"type": "string", "description": "Estado del parte (opcional)."},
            }, "required": []}}},

        {"type": "function", "function": {
            "name": "get_pozos_intervenidos",
            "description": "Lista de pozos que tuvieron intervención (workover, pulling) en un período.",
            "parameters": {"type": "object", "properties": {
                "fecha_desde": {"type": "string", "description": "Fecha desde YYYY-MM-DD."},
                "fecha_hasta": {"type": "string", "description": "Fecha hasta YYYY-MM-DD."},
            }, "required": []}}},

        {"type": "function", "function": {
            "name": "get_instalacion_fondo",
            "description": "Estado del módulo de instalación de fondo: si el dashboard fue generado y su fecha. El dashboard contiene tipo de bomba, profundidad, diámetro por pozo.",
            "parameters": {"type": "object", "properties": {}, "required": []}}},
    ]

    SYSTEM_PROMPT = """Sos el Agente de Operaciones de la Plataforma DINA.
Tu especialidad son las acciones de optimización registradas,
los partes diarios de torre (intervenciones) y la instalación de fondo de los pozos.

REGLAS:
- Siempre usás tools para obtener datos reales.
- Para acciones: get_kpis_acciones y get_acciones.
- Para intervenciones en pozos: get_partes_diarios.
- Para instalación de equipos: get_instalacion_fondo (informa si el dashboard existe).
- Respondés en español, de forma técnica y concisa.

CONCEPTOS:
- Parte diario de torre: registro de actividades de intervención (workover, pulling).
- Instalación de fondo: datos del equipo de bombeo instalado. Es un dashboard HTML externo."""

    def _ejecutar_tool(self, nombre: str, args: dict) -> Any:

        if nombre == "get_kpis_acciones":
            try:
                from core.acciones import get_kpis_acciones
                return get_kpis_acciones()
            except Exception as e:
                return {"error": f"get_kpis_acciones: {e}"}

        elif nombre == "get_acciones":
            try:
                from core.acciones import get_acciones_filtradas
                acciones = get_acciones_filtradas(
                    nombre_pozo=args.get("nombre_pozo"),
                    bateria=args.get("bateria"),
                    estado=args.get("estado"),
                )
                return {"acciones": acciones[:30], "total": len(acciones),
                        "nota": "Máximo 30 resultados." if len(acciones) > 30 else ""}
            except Exception as e:
                return {"error": f"get_acciones: {e}"}

        elif nombre == "get_partes_diarios":
            try:
                from api.partes_diarios import _get_all_data
                df = _get_all_data()
                if df.empty:
                    return {"error": "No hay partes diarios disponibles en GCS (carpeta partes_diarios/)."}
                pozo        = args.get("pozo", "").strip()
                fecha_desde = args.get("fecha_desde", "")
                fecha_hasta = args.get("fecha_hasta", "")
                status      = args.get("status", "").strip()
                if pozo and "well_legal_name" in df.columns:
                    df = df[df["well_legal_name"].str.upper().str.contains(pozo.upper(), na=False)]
                if status and "status" in df.columns:
                    df = df[df["status"].str.upper() == status.upper()]
                fe_col = next((c for c in ["date", "fecha", "start_date"] if c in df.columns), None)
                if fe_col and (fecha_desde or fecha_hasta):
                    df[fe_col] = pd.to_datetime(df[fe_col], errors="coerce")
                    if fecha_desde:
                        df = df[df[fe_col] >= pd.to_datetime(fecha_desde, errors="coerce")]
                    if fecha_hasta:
                        df = df[df[fe_col] <= pd.to_datetime(fecha_hasta, errors="coerce")]
                    df[fe_col] = df[fe_col].dt.strftime("%Y-%m-%d")
                return {"total": len(df), "mostrando": min(50, len(df)),
                        "registros": clean_records(df, 50)}
            except Exception as e:
                return {"error": f"get_partes_diarios: {e}"}

        elif nombre == "get_pozos_intervenidos":
            try:
                from api.partes_diarios import _get_all_data
                df = _get_all_data()
                if df.empty:
                    return {"error": "No hay partes diarios disponibles."}
                fecha_desde = args.get("fecha_desde", "")
                fecha_hasta = args.get("fecha_hasta", "")
                fe_col = next((c for c in ["date", "fecha", "start_date"] if c in df.columns), None)
                if fe_col and (fecha_desde or fecha_hasta):
                    df[fe_col] = pd.to_datetime(df[fe_col], errors="coerce")
                    if fecha_desde:
                        df = df[df[fe_col] >= pd.to_datetime(fecha_desde, errors="coerce")]
                    if fecha_hasta:
                        df = df[df[fe_col] <= pd.to_datetime(fecha_hasta, errors="coerce")]
                pozo_col = next((c for c in ["well_legal_name", "pozo", "POZO"] if c in df.columns), None)
                if not pozo_col:
                    return {"error": "No se encontró columna de nombre de pozo en partes diarios."}
                pozos = sorted(df[pozo_col].dropna().unique().tolist())
                return {"periodo": f"{fecha_desde or '?'} → {fecha_hasta or '?'}",
                        "total_pozos_intervenidos": len(pozos),
                        "pozos": pozos}
            except Exception as e:
                return {"error": f"get_pozos_intervenidos: {e}"}

        elif nombre == "get_instalacion_fondo":
            # El módulo es un dashboard HTML generado externamente (if/visualizador_if.html)
            # No hay JSON estructurado — solo reportamos si existe y su metadata
            try:
                from core.gcs import get_gcs_client, GCS_BUCKET, GCS_PREFIX
                client = get_gcs_client()
                if not client or not GCS_BUCKET:
                    return {"error": "GCS no configurado."}
                blob_name = f"{GCS_PREFIX}/if/visualizador_if.html" if GCS_PREFIX else "if/visualizador_if.html"
                blob = client.bucket(GCS_BUCKET).blob(blob_name)
                if not blob.exists():
                    return {
                        "dashboard_existe": False,
                        "mensaje": "El dashboard de instalación de fondo aún no fue generado. "
                                   "Ejecutá el script correspondiente para crearlo.",
                        "blob": blob_name,
                    }
                blob.reload()
                return {
                    "dashboard_existe": True,
                    "updated_at": blob.updated.isoformat() if blob.updated else None,
                    "size_kb": round((blob.size or 0) / 1024, 1),
                    "blob": blob_name,
                    "nota": "El dashboard HTML está disponible en /api/instalacion-fondo/dashboard",
                }
            except Exception as e:
                return {"error": f"get_instalacion_fondo: {e}"}

        return {"error": f"Tool desconocida en AgenteOperaciones: {nombre}"}
