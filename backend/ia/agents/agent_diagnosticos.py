# =============================================================
# backend/ia/agents/agent_diagnosticos.py
# Agente: diagnósticos IA, semáforo AIB, problemáticas, validaciones.
# =============================================================
from __future__ import annotations
from typing import Any
import pandas as pd
from .base_agent import BaseAgent, buscar_pozo_fuzzy, clean_records


class AgenteDiagnosticos(BaseAgent):

    NOMBRE = "diagnosticos"

    TOOLS = [
        {"type": "function", "function": {
            "name": "get_kpis_diagnosticos",
            "description": "KPIs globales de diagnósticos IA: total pozos diagnosticados, cuántos son críticos, alta severidad, sin problemáticas.",
            "parameters": {"type": "object", "properties": {}, "required": []}}},

        {"type": "function", "function": {
            "name": "get_pozos_criticos",
            "description": "Pozos con severidad CRÍTICA o ALTA: problemáticas activas, batería, llenado, sumergencia, recomendación.",
            "parameters": {"type": "object", "properties": {
                "severidad": {"type": "string", "enum": ["CRÍTICA", "ALTA", "MEDIA", "BAJA"],
                              "description": "Filtrar por nivel. Sin valor devuelve CRÍTICA y ALTA."},
            }, "required": []}}},

        {"type": "function", "function": {
            "name": "get_diagnostico_pozo",
            "description": "Diagnóstico IA completo de un pozo: problemáticas, severidades, sumergencia, llenado, caudal, balance y recomendación.",
            "parameters": {"type": "object", "properties": {
                "pozo": {"type": "string", "description": "Nombre del pozo."},
            }, "required": ["pozo"]}}},

        {"type": "function", "function": {
            "name": "buscar_por_problematica",
            "description": "Todos los pozos con una problemática específica activa: golpeo de fondo, gas en bomba, llenado bajo, sumergencia crítica, etc.",
            "parameters": {"type": "object", "properties": {
                "problema": {"type": "string", "description": "Fragmento del nombre de la problemática."},
            }, "required": ["problema"]}}},

        {"type": "function", "function": {
            "name": "get_semaforo_aib",
            "description": "Estado del semáforo AIB: pozos en estado normal, alerta o crítico según sumergencia y llenado.",
            "parameters": {"type": "object", "properties": {
                "bateria": {"type": "string", "description": "Filtrar por batería (opcional)."},
            }, "required": []}}},

        {"type": "function", "function": {
            "name": "get_resumen_validaciones",
            "description": "Resumen global de validaciones de sumergencia: total mediciones, validadas, pendientes.",
            "parameters": {"type": "object", "properties": {}, "required": []}}},

        {"type": "function", "function": {
            "name": "get_validaciones_pozo",
            "description": "Validaciones de sumergencia de un pozo: mediciones validadas, pendientes, comentarios.",
            "parameters": {"type": "object", "properties": {
                "pozo": {"type": "string", "description": "Nombre del pozo."},
            }, "required": ["pozo"]}}},

        {"type": "function", "function": {
            "name": "get_detalle_pozo",
            "description": "Detalle completo de un pozo: última medición DIN/NIV, estado semáforo, coordenadas, batería.",
            "parameters": {"type": "object", "properties": {
                "pozo": {"type": "string", "description": "Nombre del pozo."},
            }, "required": ["pozo"]}}},

        {"type": "function", "function": {
            "name": "get_calidad_datos",
            "description": "Calidad de datos del campo: pozos con sumergencia negativa, PB anómalo, porcentaje de datos válidos.",
            "parameters": {"type": "object", "properties": {}, "required": []}}},
    ]

    SYSTEM_PROMPT = """Sos el Agente de Diagnósticos IA de la Plataforma DINA.
Tu especialidad son los diagnósticos generados por inteligencia artificial,
las problemáticas activas, severidades, el semáforo AIB y validaciones de sumergencia.

REGLAS:
- Siempre usás tools para obtener datos reales. Nunca inventás diagnósticos.
- Para pozos críticos: get_pozos_criticos.
- Para un pozo específico: get_diagnostico_pozo.
- Para buscar por tipo de problema: buscar_por_problematica.
- Respondés en español, de forma técnica y concisa.

SEVERIDADES: CRÍTICA > ALTA > MEDIA > BAJA
PROBLEMÁTICAS FRECUENTES: golpeo de fondo, gas en bomba, llenado bajo,
sumergencia crítica, fuga válvula descarga/succión, desbalance de contrapesos."""

    def _ejecutar_tool(self, nombre: str, args: dict) -> Any:

        if nombre == "get_kpis_diagnosticos":
            try:
                from api.diagnosticos import _load_din_niv_ok, _get_bat_map
                from ia.diagnostico import build_global_table, get_kpis_global_table
                from core.gcs import load_all_diags_from_gcs
                from core.parsers import normalize_no_exact
                din_ok, _ = _load_din_niv_ok()
                if din_ok is None or din_ok.empty:
                    return {"error": "No hay índice DIN disponible."}
                pozos = sorted(din_ok["NO_key"].dropna().unique().tolist())
                diags = load_all_diags_from_gcs(pozos)
                if not diags:
                    return {"error": "No hay diagnósticos generados aún."}
                df = build_global_table(diags, _get_bat_map(), normalize_no_exact)
                return get_kpis_global_table(df)
            except Exception as e:
                return {"error": f"get_kpis_diagnosticos: {e}"}

        elif nombre == "get_pozos_criticos":
            try:
                from api.diagnosticos import _load_din_niv_ok, _get_bat_map
                from ia.diagnostico import build_global_table
                from core.gcs import load_all_diags_from_gcs
                from core.parsers import normalize_no_exact
                sev = args.get("severidad")
                sevs = [sev] if sev else ["CRÍTICA", "ALTA"]
                din_ok, _ = _load_din_niv_ok()
                if din_ok is None or din_ok.empty:
                    return {"error": "No hay índice DIN disponible."}
                pozos = sorted(din_ok["NO_key"].dropna().unique().tolist())
                diags = load_all_diags_from_gcs(pozos)
                if not diags:
                    return {"error": "No hay diagnósticos generados aún."}
                df = build_global_table(diags, _get_bat_map(), normalize_no_exact)
                df_fil = df[df["Sev. máx"].isin(sevs)].sort_values(["Sev. máx", "Batería", "Pozo"])
                return {
                    "total": len(df_fil),
                    "pozos": [{"pozo": r["Pozo"], "bateria": r["Batería"],
                                "severidad": r["Sev. máx"], "fecha_din": r["Fecha DIN"],
                                "llenado": r["Llenado %"], "sumergencia": r["Sumergencia"],
                                "problematicas": r["Problemáticas"],
                                "recomendacion": r["Recomendación"]}
                               for _, r in df_fil.iterrows()],
                }
            except Exception as e:
                return {"error": f"get_pozos_criticos: {e}"}

        elif nombre == "get_diagnostico_pozo":
            try:
                from core.gcs import load_diag_from_gcs
                pozo_raw = args.get("pozo", "").strip()
                no_key, err = buscar_pozo_fuzzy(pozo_raw)
                if err: return {"error": err}
                diag = load_diag_from_gcs(no_key)
                if not diag:
                    return {"error": f"No se encontró diagnóstico para '{pozo_raw}'. Quizás no fue generado aún."}
                if "error" in diag:
                    return {"error": diag["error"]}
                meta = diag.get("_meta", {})
                return {
                    "pozo": no_key,
                    "recomendacion": diag.get("recomendacion", ""),
                    "confianza": diag.get("confianza", "N/D"),
                    "generado_utc": meta.get("generado_utc", "?")[:19],
                    "mediciones": [
                        {"fecha": m.get("fecha"),
                         "llenado_pct": m.get("llenado_pct"),
                         "sumergencia_m": m.get("sumergencia_m"),
                         "caudal_bruto": m.get("caudal_bruto"),
                         "pct_balance": m.get("pct_balance"),
                         "problematicas": [
                             {"nombre": p.get("nombre"), "severidad": p.get("severidad"),
                              "estado": p.get("estado"), "detalle": p.get("detalle", "")}
                             for p in m.get("problemáticas", [])
                         ]}
                        for m in diag.get("mediciones", [])
                    ],
                }
            except Exception as e:
                return {"error": f"get_diagnostico_pozo: {e}"}

        elif nombre == "buscar_por_problematica":
            try:
                from api.diagnosticos import _load_din_niv_ok, _get_bat_map
                from ia.diagnostico import build_global_table
                from core.gcs import load_all_diags_from_gcs
                from core.parsers import normalize_no_exact
                problema = args.get("problema", "").strip().lower()
                if not problema: return {"error": "Parámetro 'problema' requerido."}
                din_ok, _ = _load_din_niv_ok()
                if din_ok is None or din_ok.empty:
                    return {"error": "No hay índice DIN disponible."}
                pozos = sorted(din_ok["NO_key"].dropna().unique().tolist())
                diags = load_all_diags_from_gcs(pozos)
                df = build_global_table(diags, _get_bat_map(), normalize_no_exact)
                if df.empty: return {"pozos": [], "total": 0}
                # _prob_lista puede no existir en todas las versiones de build_global_table
                if "_prob_lista" in df.columns:
                    mask = df["_prob_lista"].apply(
                        lambda l: isinstance(l, list) and any(problema in str(p).lower() for p in l)
                    )
                else:
                    # Fallback: buscar en la columna string "Problemáticas"
                    mask = df["Problemáticas"].str.lower().str.contains(problema, na=False)
                df_fil = df[mask][["Pozo", "Batería", "Fecha DIN", "Sev. máx", "Problemáticas"]].copy()
                return {"problema_buscado": problema,
                        "pozos": df_fil.to_dict(orient="records"),
                        "total": len(df_fil)}
            except Exception as e:
                return {"error": f"buscar_por_problematica: {e}"}

        elif nombre == "get_semaforo_aib":
            try:
                from core.gcs import load_snapshot
                bateria = args.get("bateria", "").strip()
                snap = load_snapshot()
                if snap is None or snap.empty:
                    return {"error": "No hay snapshot disponible."}
                if "SE" in snap.columns:
                    snap = snap[snap["SE"].str.upper() == "AIB"]
                if bateria and "Bateria" in snap.columns:
                    snap = snap[snap["Bateria"].str.upper() == bateria.upper()]
                def clasificar(row):
                    s = row.get("Sumergencia")
                    l = row.get("Bba Llenado")
                    if s is None and l is None: return "sin_datos"
                    if (s is not None and s < 200) or (l is not None and l < 50): return "critico"
                    if (s is not None and s < 250) or (l is not None and l < 70): return "alerta"
                    return "normal"
                snap["_estado"] = snap.apply(clasificar, axis=1)
                counts = snap["_estado"].value_counts().to_dict()
                return {"total_aib": len(snap),
                        "normal": counts.get("normal", 0),
                        "alerta": counts.get("alerta", 0),
                        "critico": counts.get("critico", 0),
                        "sin_datos": counts.get("sin_datos", 0)}
            except Exception as e:
                return {"error": f"get_semaforo_aib: {e}"}

        elif nombre == "get_resumen_validaciones":
            try:
                from api.validaciones import _load_snap_map
                from core.gcs import load_all_validaciones
                # _load_snap_map retorna un DataFrame (no un dict)
                snap_df = _load_snap_map()
                if snap_df is None or snap_df.empty:
                    return {"error": "No hay datos de validaciones."}
                pozos_lista = snap_df["NO_key"].dropna().unique().tolist() if "NO_key" in snap_df.columns else []
                if not pozos_lista:
                    return {"error": "No se encontró columna NO_key en snap_map."}
                vals = load_all_validaciones(pozos_lista)
                total_med = validadas = 0
                for _, data in vals.items():
                    for _, estado in data.get("mediciones", {}).items():
                        total_med += 1
                        if estado.get("validada"): validadas += 1
                return {"total_pozos": len(pozos_lista),
                        "total_mediciones": total_med,
                        "validadas": validadas,
                        "pendientes": total_med - validadas}
            except Exception as e:
                return {"error": f"get_resumen_validaciones: {e}"}

        elif nombre == "get_validaciones_pozo":
            try:
                from core.gcs import load_validaciones
                pozo_raw = args.get("pozo", "").strip()
                no_key, err = buscar_pozo_fuzzy(pozo_raw)
                if err: return {"error": err}
                val_data = load_validaciones(no_key)
                mediciones = val_data.get("mediciones", {})
                validadas = sum(1 for v in mediciones.values() if v.get("validada"))
                detalle = [
                    {"fecha": fecha, "validada": v.get("validada", False),
                     "comentario": v.get("comentario", ""), "usuario": v.get("usuario", "")}
                    for fecha, v in sorted(mediciones.items(), reverse=True)
                ]
                return {"pozo": no_key, "total_mediciones": len(mediciones),
                        "validadas": validadas, "pendientes": len(mediciones) - validadas,
                        "detalle": detalle[:20]}
            except Exception as e:
                return {"error": f"get_validaciones_pozo: {e}"}

        elif nombre == "get_detalle_pozo":
            try:
                import math as _math
                from api.mapa import _load_indexes_ok, _build_snap_con_coords
                pozo_raw = args.get("pozo", "").strip()
                no_key, err = buscar_pozo_fuzzy(pozo_raw)
                if err: return {"error": err}
                din_ok, niv_ok, _ = _load_indexes_ok()
                snap = _build_snap_con_coords(din_ok, niv_ok)
                fila = snap[snap["NO_key"] == no_key]
                if fila.empty:
                    return {"error": f"Pozo '{no_key}' no encontrado."}
                row = fila.iloc[0].where(fila.iloc[0].notna(), other=None).to_dict()
                row = {k: (None if isinstance(v, float) and _math.isnan(v) else v) for k, v in row.items()}
                return {"pozo": no_key, "datos": row}
            except Exception as e:
                return {"error": f"get_detalle_pozo: {e}"}

        elif nombre == "get_calidad_datos":
            try:
                from core.semaforo import get_calidad_resumen, get_sumergencia_negativa, get_pb_anomalo
                from core.gcs import load_snapshot
                snap = load_snapshot()
                if snap is None or snap.empty:
                    return {"error": "No hay snapshot disponible."}
                resumen = get_calidad_resumen(snap)
                neg = get_sumergencia_negativa(snap)
                pb_anom, pb_mean, pb_std = get_pb_anomalo(snap)
                return {
                    "calidad_resumen": resumen,
                    "sumergencia_negativa": {
                        "total": len(neg),
                        "pozos": neg["NO_key"].tolist()[:10] if "NO_key" in neg.columns else [],
                    },
                    "pb_anomalo": {
                        "total": len(pb_anom),
                        "pb_media": pb_mean,
                        "pb_std": pb_std,
                        "pozos": pb_anom["NO_key"].tolist()[:10] if "NO_key" in pb_anom.columns else [],
                    },
                }
            except Exception as e:
                return {"error": f"get_calidad_datos: {e}"}

        return {"error": f"Tool desconocida en AgenteDiagnosticos: {nombre}"}
