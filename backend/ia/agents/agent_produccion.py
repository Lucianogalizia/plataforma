# =============================================================
# backend/ia/agents/agent_produccion.py
# Agente: controles de producción, merma, pérdidas, alertas.
# =============================================================
from __future__ import annotations
from typing import Any
import pandas as pd
from .base_agent import BaseAgent, read_csv_gcs, clean_records


class AgenteProduccion(BaseAgent):

    NOMBRE = "produccion"

    TOOLS = [
        {"type": "function", "function": {
            "name": "get_controles_merma",
            "description": "Pozos con merma (caída) de producción: % pérdida neta y bruta, fecha último control, días sin control.",
            "parameters": {"type": "object", "properties": {
                "solo_en_merma": {"type": "boolean", "description": "Si true, solo pozos con merma activa."},
                "bateria":       {"type": "string",  "description": "Filtrar por batería."},
            }, "required": []}}},

        {"type": "function", "function": {
            "name": "get_controles_historico",
            "description": "Controles de producción históricos: petróleo, gas y líquido por pozo y fecha.",
            "parameters": {"type": "object", "properties": {
                "pozo":        {"type": "string", "description": "Nombre del pozo."},
                "bateria":     {"type": "string", "description": "Nombre de batería."},
                "fecha_desde": {"type": "string", "description": "Fecha desde YYYY-MM-DD."},
                "fecha_hasta": {"type": "string", "description": "Fecha hasta YYYY-MM-DD."},
            }, "required": []}}},

        {"type": "function", "function": {
            "name": "get_kpis_controles",
            "description": "KPIs del módulo controles: total registros, pozos únicos, días promedio sin control, pozos en merma neta.",
            "parameters": {"type": "object", "properties": {}, "required": []}}},

        {"type": "function", "function": {
            "name": "get_downtimes_perdidas",
            "description": "Pérdidas de producción (downtimes): shortfall de petróleo, gas, líquido por pozo y fecha.",
            "parameters": {"type": "object", "properties": {
                "pozo":        {"type": "string", "description": "Nombre del pozo (opcional)."},
                "fecha_desde": {"type": "string", "description": "Fecha desde YYYY-MM-DD."},
                "fecha_hasta": {"type": "string", "description": "Fecha hasta YYYY-MM-DD."},
            }, "required": []}}},

        {"type": "function", "function": {
            "name": "get_kpis_perdidas",
            "description": "KPIs acumulados de pérdidas: total oilShortfall, gasShortfall, liquidShortfall. '¿Cuánto petróleo perdimos?'",
            "parameters": {"type": "object", "properties": {
                "fecha_desde": {"type": "string", "description": "Fecha desde YYYY-MM-DD (opcional)."},
                "fecha_hasta": {"type": "string", "description": "Fecha hasta YYYY-MM-DD (opcional)."},
            }, "required": []}}},

        {"type": "function", "function": {
            "name": "get_alertas_llenado",
            "description": "Estado del módulo de alertas de llenado de bomba: si el dashboard fue generado, fecha y tamaño. Para pozos con llenado bajo usa get_snapshot_pozos del agente de pozos.",
            "parameters": {"type": "object", "properties": {}, "required": []}}},

        {"type": "function", "function": {
            "name": "get_alertas_presion",
            "description": "Estado del módulo de predicción de alta presión: si el dashboard fue generado, fecha y tamaño.",
            "parameters": {"type": "object", "properties": {}, "required": []}}},
    ]

    SYSTEM_PROMPT = """Sos el Agente de Producción de la Plataforma DINA.
Tu especialidad son los controles de producción, merma de petróleo/gas,
pérdidas históricas (downtimes) y el estado de los módulos de alertas.

REGLAS:
- Siempre usás tools para obtener datos reales.
- Para merma activa: get_controles_merma(solo_en_merma=true).
- Para pérdidas totales: get_kpis_perdidas.
- Para el estado de los dashboards de alertas: get_alertas_llenado / get_alertas_presion.
  NOTA: estos dashboards son HTML generados externamente. La tool solo informa si existen y su fecha.
- Respondés en español, de forma técnica y concisa.

CONCEPTOS:
- pct_merma_neta: caída % de producción neta respecto al período anterior.
- oilShortfall: petróleo no producido por downtime (m³)."""

    def _ejecutar_tool(self, nombre: str, args: dict) -> Any:

        if nombre == "get_controles_merma":
            try:
                df = read_csv_gcs("controles/merma_por_pozo.csv")
                if df is None:
                    return {"error": "Archivo de merma no encontrado en GCS (controles/merma_por_pozo.csv)."}
                if args.get("solo_en_merma") and "EN_MERMA_NETA" in df.columns:
                    df = df[df["EN_MERMA_NETA"] == True]
                bateria = args.get("bateria", "").strip()
                if bateria and "BATERIA" in df.columns:
                    df = df[df["BATERIA"].str.upper() == bateria.upper()]
                return {"total": len(df), "mostrando": min(50, len(df)), "pozos": clean_records(df, 50)}
            except Exception as e:
                return {"error": f"get_controles_merma: {e}"}

        elif nombre == "get_controles_historico":
            try:
                df = read_csv_gcs("controles/historico_CRUDO.csv")
                if df is None:
                    return {"error": "Archivo histórico no encontrado en GCS (controles/historico_CRUDO.csv)."}
                if "Fecha y Hora" in df.columns:
                    df["Fecha y Hora"] = pd.to_datetime(df["Fecha y Hora"], errors="coerce")
                pozo    = args.get("pozo", "").strip()
                bateria = args.get("bateria", "").strip()
                if pozo    and "Pozo"    in df.columns: df = df[df["Pozo"].str.upper()    == pozo.upper()]
                if bateria and "BATERIA" in df.columns: df = df[df["BATERIA"].str.upper() == bateria.upper()]
                if args.get("fecha_desde") and "Fecha y Hora" in df.columns:
                    df = df[df["Fecha y Hora"] >= pd.to_datetime(args["fecha_desde"], errors="coerce")]
                if args.get("fecha_hasta") and "Fecha y Hora" in df.columns:
                    df = df[df["Fecha y Hora"] <= pd.to_datetime(args["fecha_hasta"], errors="coerce")]
                for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
                    df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M")
                return {"total": len(df), "mostrando": min(100, len(df)), "registros": clean_records(df, 100)}
            except Exception as e:
                return {"error": f"get_controles_historico: {e}"}

        elif nombre == "get_kpis_controles":
            try:
                df_hist  = read_csv_gcs("controles/historico_CRUDO.csv")
                df_merma = read_csv_gcs("controles/merma_por_pozo.csv")
                result: dict = {}
                if df_hist is not None:
                    result["total_controles"] = len(df_hist)
                    if "Pozo" in df_hist.columns:
                        result["pozos_unicos"] = int(df_hist["Pozo"].nunique())
                    if "Fecha y Hora" in df_hist.columns:
                        fechas = pd.to_datetime(df_hist["Fecha y Hora"], errors="coerce").dropna()
                        if not fechas.empty:
                            result["fecha_min"] = str(fechas.min().date())
                            result["fecha_max"] = str(fechas.max().date())
                if df_merma is not None:
                    result["pozos_analizados_merma"] = len(df_merma)
                    if "EN_MERMA_NETA" in df_merma.columns:
                        result["en_merma_neta"] = int(df_merma["EN_MERMA_NETA"].sum())
                    if "DIAS_SIN_CONTROL" in df_merma.columns:
                        dias = pd.to_numeric(df_merma["DIAS_SIN_CONTROL"], errors="coerce").dropna()
                        if not dias.empty:
                            result["dias_prom_sin_control"] = round(float(dias.mean()), 1)
                            result["dias_max_sin_control"]  = int(dias.max())
                if not result:
                    return {"error": "No hay datos de controles disponibles en GCS."}
                return result
            except Exception as e:
                return {"error": f"get_kpis_controles: {e}"}

        elif nombre == "get_downtimes_perdidas":
            try:
                df = read_csv_gcs("merma/wellDowntimes_CRUDO.csv")
                if df is None:
                    return {"error": "Archivo de downtimes no encontrado en GCS (merma/wellDowntimes_CRUDO.csv)."}
                for col in ["FECHA DESDE", "FECHA HASTA"]:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                pozo = args.get("pozo", "").strip()
                if pozo and "POZO" in df.columns:
                    df = df[df["POZO"].str.upper() == pozo.upper()]
                if args.get("fecha_desde") and "FECHA DESDE" in df.columns:
                    df = df[df["FECHA DESDE"] >= pd.to_datetime(args["fecha_desde"], errors="coerce")]
                if args.get("fecha_hasta") and "FECHA DESDE" in df.columns:
                    df = df[df["FECHA DESDE"] <= pd.to_datetime(args["fecha_hasta"], errors="coerce")]
                for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
                    df[col] = df[col].dt.strftime("%Y-%m-%d")
                cols_utiles = [c for c in ["POZO", "RUBRO", "FECHA DESDE", "FECHA HASTA",
                                            "oilShortfall", "waterShortfall", "liquidShortfall",
                                            "gasShortfall", "potentialOil", "potentialLiquid"] if c in df.columns]
                df = df[cols_utiles] if cols_utiles else df
                return {"total": len(df), "mostrando": min(50, len(df)), "registros": clean_records(df, 50)}
            except Exception as e:
                return {"error": f"get_downtimes_perdidas: {e}"}

        elif nombre == "get_kpis_perdidas":
            try:
                df = read_csv_gcs("merma/wellDowntimes_CRUDO.csv")
                if df is None:
                    return {"error": "Archivo de downtimes no encontrado en GCS."}
                for col in ["FECHA DESDE", "FECHA HASTA"]:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                if args.get("fecha_desde") and "FECHA DESDE" in df.columns:
                    df = df[df["FECHA DESDE"] >= pd.to_datetime(args["fecha_desde"], errors="coerce")]
                if args.get("fecha_hasta") and "FECHA DESDE" in df.columns:
                    df = df[df["FECHA DESDE"] <= pd.to_datetime(args["fecha_hasta"], errors="coerce")]
                result: dict = {"total_registros": len(df)}
                for col in ["oilShortfall", "gasShortfall", "liquidShortfall",
                            "waterShortfall", "potentialOil", "potentialLiquid"]:
                    if col in df.columns:
                        result[f"total_{col}_m3"] = round(float(pd.to_numeric(df[col], errors="coerce").sum()), 1)
                if "POZO" in df.columns:
                    result["pozos_afectados"] = int(df["POZO"].nunique())
                if "RUBRO" in df.columns:
                    result["rubros"] = df["RUBRO"].dropna().unique().tolist()
                if "FECHA DESDE" in df.columns:
                    fechas = df["FECHA DESDE"].dropna()
                    if not fechas.empty:
                        result["fecha_min"] = str(fechas.min().date())
                        result["fecha_max"] = str(fechas.max().date())
                return result
            except Exception as e:
                return {"error": f"get_kpis_perdidas: {e}"}

        elif nombre == "get_alertas_llenado":
            # El dashboard es un HTML generado externamente — solo reportamos si existe y su metadata
            try:
                from core.gcs import get_gcs_client, GCS_BUCKET, GCS_PREFIX
                client = get_gcs_client()
                if not client or not GCS_BUCKET:
                    return {"error": "GCS no configurado."}
                blob_name = f"{GCS_PREFIX}/merma/alertas_llenado.html" if GCS_PREFIX else "merma/alertas_llenado.html"
                blob = client.bucket(GCS_BUCKET).blob(blob_name)
                if not blob.exists():
                    return {
                        "dashboard_existe": False,
                        "mensaje": "El dashboard de alertas de llenado aún no fue generado. "
                                   "Ejecutá alertas_llenado_script.py para crearlo.",
                        "blob": blob_name,
                    }
                blob.reload()
                return {
                    "dashboard_existe": True,
                    "updated_at": blob.updated.isoformat() if blob.updated else None,
                    "size_kb": round((blob.size or 0) / 1024, 1),
                    "blob": blob_name,
                    "nota": "El dashboard HTML está disponible en /api/alertas-llenado/dashboard",
                }
            except Exception as e:
                return {"error": f"get_alertas_llenado: {e}"}

        elif nombre == "get_alertas_presion":
            # Ídem — solo HTML, no JSON estructurado
            try:
                from core.gcs import get_gcs_client, GCS_BUCKET, GCS_PREFIX
                client = get_gcs_client()
                if not client or not GCS_BUCKET:
                    return {"error": "GCS no configurado."}
                blob_name = f"{GCS_PREFIX}/merma/alertas_presion.html" if GCS_PREFIX else "merma/alertas_presion.html"
                blob = client.bucket(GCS_BUCKET).blob(blob_name)
                if not blob.exists():
                    return {
                        "dashboard_existe": False,
                        "mensaje": "El dashboard de alertas de presión aún no fue generado. "
                                   "Ejecutá presion_final_completo.py para crearlo.",
                        "blob": blob_name,
                    }
                blob.reload()
                return {
                    "dashboard_existe": True,
                    "updated_at": blob.updated.isoformat() if blob.updated else None,
                    "size_kb": round((blob.size or 0) / 1024, 1),
                    "blob": blob_name,
                    "nota": "El dashboard HTML está disponible en /api/alertas-presion/dashboard",
                }
            except Exception as e:
                return {"error": f"get_alertas_presion: {e}"}

        return {"error": f"Tool desconocida en AgenteProduccion: {nombre}"}
