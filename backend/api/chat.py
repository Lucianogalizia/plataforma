# ==========================================================
# backend/api/chat.py  — v3 (completo, todas las fuentes)
# ==========================================================
from __future__ import annotations
import json, io, math
from typing import Any
import pandas as pd
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

class ChatRequest(BaseModel):
    mensaje: str
    historial: list[dict] = []

class ChatResponse(BaseModel):
    respuesta: str
    tools_usadas: list[str] = []

# ==========================================================
# TOOLS — todas las fuentes de la app
# ==========================================================
TOOLS = [

    # ── DIAGNÓSTICOS IA ────────────────────────────────────
    {"type":"function","function":{
        "name":"get_kpis_diagnosticos",
        "description":"KPIs globales de diagnósticos IA: total pozos diagnosticados, cuántos son críticos, alta severidad, sin problemáticas.",
        "parameters":{"type":"object","properties":{},"required":[]}}},

    {"type":"function","function":{
        "name":"get_pozos_criticos",
        "description":"Lista pozos con severidad CRÍTICA o ALTA: problemáticas activas, batería, llenado, sumergencia, recomendación. Usar para: '¿qué pozos son críticos?', 'problemas graves'.",
        "parameters":{"type":"object","properties":{
            "severidad":{"type":"string","enum":["CRÍTICA","ALTA","MEDIA","BAJA"],"description":"Filtrar por nivel. Sin valor devuelve CRÍTICA y ALTA."}},"required":[]}}},

    {"type":"function","function":{
        "name":"get_diagnostico_pozo",
        "description":"Diagnóstico IA completo de un pozo: problemáticas, severidades, sumergencia, llenado, caudal, balance y recomendación.",
        "parameters":{"type":"object","properties":{
            "pozo":{"type":"string","description":"Nombre del pozo. Ej: BB-106, Pe-123. Se busca automáticamente aunque no sea exacto."}},"required":["pozo"]}}},

    {"type":"function","function":{
        "name":"buscar_por_problematica",
        "description":"Todos los pozos con una problemática activa específica. Ej: golpeo de fondo, gas en bomba, llenado bajo, sumergencia crítica, fuga válvula.",
        "parameters":{"type":"object","properties":{
            "problema":{"type":"string","description":"Fragmento del nombre de la problemática."}},"required":["problema"]}}},

    # ── SNAPSHOT / ESTADO ACTUAL ───────────────────────────
    {"type":"function","function":{
        "name":"get_snapshot_pozos",
        "description":"Estado actual de todos los pozos: sumergencia, llenado de bomba, caudal, balance, estructura, días desde última medición. Usar para preguntas generales del campo o comparar pozos.",
        "parameters":{"type":"object","properties":{
            "bateria":{"type":"string","description":"Filtrar por batería."},
            "con_sumergencia_baja":{"type":"boolean","description":"Si true, solo pozos con sumergencia < 50m."}},"required":[]}}},

    {"type":"function","function":{
        "name":"get_tendencias",
        "description":"Tendencias de una variable por pozo a lo largo del tiempo (pendiente por mes). Útil para: '¿qué pozos están empeorando?', '¿cuáles mejoran?'. Variables: Sumergencia, %Balance, %Estructura, Caudal bruto efec.",
        "parameters":{"type":"object","properties":{
            "variable":{"type":"string","description":"Variable a analizar. Default: Sumergencia."},
            "solo_positiva":{"type":"boolean","description":"Si true, solo tendencia creciente. Default true."}},"required":[]}}},

    {"type":"function","function":{
        "name":"get_pozos_por_mes",
        "description":"Cantidad de pozos medidos por mes. Muestra cobertura histórica de mediciones DIN/NIV.",
        "parameters":{"type":"object","properties":{},"required":[]}}},

    # ── NIVELES NIV ────────────────────────────────────────
    {"type":"function","function":{
        "name":"get_historico_niv",
        "description":"Histórico de niveles de fluido (NIV) de un pozo: serie temporal de NC, NM, ND, PB y sumergencia.",
        "parameters":{"type":"object","properties":{
            "pozo":{"type":"string","description":"Nombre del pozo."}},"required":["pozo"]}}},

    # ── VALIDACIONES ───────────────────────────────────────
    {"type":"function","function":{
        "name":"get_resumen_validaciones",
        "description":"Resumen del sistema de validaciones de sumergencias: total mediciones, cuántas validadas, pendientes.",
        "parameters":{"type":"object","properties":{},"required":[]}}},

    # ── SEMÁFORO AIB ───────────────────────────────────────
    {"type":"function","function":{
        "name":"get_semaforo_aib",
        "description":"Estado del semáforo AIB de pozos: cuántos están en estado normal, alerta o crítico según sumergencia y llenado.",
        "parameters":{"type":"object","properties":{
            "bateria":{"type":"string","description":"Filtrar por batería (opcional)."}},"required":[]}}},

    # ── CONTROLES DE PRODUCCIÓN ────────────────────────────
    {"type":"function","function":{
        "name":"get_controles_merma",
        "description":"Pozos con merma (caída) de producción: % pérdida neta y bruta, fecha último control, días sin control.",
        "parameters":{"type":"object","properties":{
            "solo_en_merma":{"type":"boolean","description":"Si true, solo pozos con merma activa."},
            "bateria":{"type":"string","description":"Filtrar por batería."}},"required":[]}}},

    {"type":"function","function":{
        "name":"get_controles_historico",
        "description":"Controles de producción históricos: producción de petróleo, gas y líquido por pozo y fecha.",
        "parameters":{"type":"object","properties":{
            "pozo":{"type":"string","description":"Nombre del pozo."},
            "bateria":{"type":"string","description":"Nombre de batería."},
            "fecha_desde":{"type":"string","description":"Fecha desde YYYY-MM-DD."},
            "fecha_hasta":{"type":"string","description":"Fecha hasta YYYY-MM-DD."}},"required":[]}}},

    # ── ACCIONES DE OPTIMIZACIÓN ───────────────────────────
    {"type":"function","function":{
        "name":"get_kpis_acciones",
        "description":"KPIs de acciones de optimización: total registradas, en proceso, finalizadas.",
        "parameters":{"type":"object","properties":{},"required":[]}}},

    {"type":"function","function":{
        "name":"get_acciones",
        "description":"Lista de acciones de optimización registradas. Filtrable por estado, batería o nombre de pozo.",
        "parameters":{"type":"object","properties":{
            "nombre_pozo":{"type":"string","description":"Nombre del pozo."},
            "bateria":{"type":"string","description":"Nombre de batería."},
            "estado":{"type":"string","description":"Estado de la acción."}},"required":[]}}},

    # ── RRHH ───────────────────────────────────────────────
    {"type":"function","function":{
        "name":"get_rrhh_personal",
        "description":"Lista del personal registrado en el sistema de RRHH: nombre, legajo, función, líder.",
        "parameters":{"type":"object","properties":{},"required":[]}}},

    {"type":"function","function":{
        "name":"get_rrhh_pendientes",
        "description":"Partes de guardia pendientes de aprobación para un líder.",
        "parameters":{"type":"object","properties":{
            "leader_legajo":{"type":"string","description":"Legajo del líder."}},"required":["leader_legajo"]}}},
]


# ==========================================================
# Helpers internos
# ==========================================================

def _buscar_pozo_fuzzy(pozo_raw: str) -> tuple[str | None, str | None]:
    """
    Busca un pozo por nombre aproximado en el índice DIN.
    Retorna (no_key, error_msg). Si hay match único, devuelve (no_key, None).
    Si hay múltiples, devuelve (None, mensaje con candidatos).
    """
    try:
        from api.diagnosticos import _load_din_niv_ok
        from core.parsers import normalize_no_exact
        no_key_exact = normalize_no_exact(pozo_raw)
        din_ok, _ = _load_din_niv_ok()
        if din_ok is None or din_ok.empty:
            return no_key_exact, None  # sin índice, intentar igual con el exacto
        todos = din_ok["NO_key"].dropna().unique().tolist()
        busqueda = pozo_raw.upper().replace(" ", "")
        candidatos = [p for p in todos if busqueda in p.upper().replace(" ", "")]
        if len(candidatos) == 1:
            return candidatos[0], None
        elif len(candidatos) > 1:
            return None, f"'{pozo_raw}' coincide con varios pozos: {candidatos[:8]}. Indicá el nombre completo."
        return no_key_exact, None  # ninguno → intentar con el exacto
    except Exception:
        from core.parsers import normalize_no_exact
        return normalize_no_exact(pozo_raw), None


def _read_csv_gcs(blob_path: str) -> pd.DataFrame | None:
    """Lee un CSV desde GCS. Devuelve None si no existe o hay error."""
    try:
        from core.gcs import get_gcs_client, GCS_BUCKET, GCS_PREFIX
        client = get_gcs_client()
        if not client or not GCS_BUCKET:
            return None
        full = f"{GCS_PREFIX}/{blob_path}" if GCS_PREFIX else blob_path
        blob = client.bucket(GCS_BUCKET).blob(full)
        if not blob.exists():
            return None
        return pd.read_csv(io.BytesIO(blob.download_as_bytes()), low_memory=False)
    except Exception:
        return None


def _clean_records(df: pd.DataFrame, limit: int = 50) -> list[dict]:
    """Convierte DataFrame a lista de dicts limpia (sin NaN/Inf)."""
    df = df.where(df.notna(), other=None)
    return [
        {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in r.items()}
        for r in df.head(limit).to_dict(orient="records")
    ]


# ==========================================================
# Implementación de tools
# ==========================================================

def _ejecutar_tool(nombre: str, args: dict) -> Any:

    # ── get_kpis_diagnosticos ──────────────────────────────
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

    # ── get_pozos_criticos ─────────────────────────────────
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
            if df.empty:
                return {"pozos": [], "total": 0}
            df_fil = df[df["Sev. máx"].isin(sevs)].sort_values(["Sev. máx", "Batería", "Pozo"])
            return {
                "pozos": [{"pozo": r["Pozo"], "bateria": r["Batería"], "severidad": r["Sev. máx"],
                           "fecha_din": r["Fecha DIN"], "llenado": r["Llenado %"],
                           "sumergencia": r["Sumergencia"], "problematicas": r["Problemáticas"],
                           "recomendacion": r["Recomendación"]} for _, r in df_fil.iterrows()],
                "total": len(df_fil)
            }
        except Exception as e:
            return {"error": f"get_pozos_criticos: {e}"}

    # ── get_diagnostico_pozo ───────────────────────────────
    elif nombre == "get_diagnostico_pozo":
        try:
            from core.gcs import load_diag_from_gcs
            pozo_raw = args.get("pozo", "").strip()
            if not pozo_raw:
                return {"error": "Parámetro 'pozo' requerido."}
            no_key, err = _buscar_pozo_fuzzy(pozo_raw)
            if err:
                return {"error": err}
            diag = load_diag_from_gcs(no_key)
            if not diag:
                return {"error": f"No se encontró diagnóstico para '{pozo_raw}'. Puede no tener DIN o diagnóstico generado."}
            if "error" in diag:
                return {"error": diag["error"]}
            meta = diag.get("_meta", {})
            return {
                "pozo": no_key,
                "recomendacion": diag.get("recomendacion", ""),
                "confianza": diag.get("confianza", "N/D"),
                "generado_utc": meta.get("generado_utc", "?")[:19],
                "mediciones": [
                    {"fecha": m.get("fecha"), "llenado_pct": m.get("llenado_pct"),
                     "sumergencia_m": m.get("sumergencia_m"), "caudal_bruto": m.get("caudal_bruto"),
                     "pct_balance": m.get("pct_balance"),
                     "problematicas": [{"nombre": p.get("nombre"), "severidad": p.get("severidad"),
                                        "estado": p.get("estado"), "detalle": p.get("detalle", "")}
                                       for p in m.get("problemáticas", [])]}
                    for m in diag.get("mediciones", [])
                ],
            }
        except Exception as e:
            return {"error": f"get_diagnostico_pozo: {e}"}

    # ── buscar_por_problematica ────────────────────────────
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
                return {"error": "No hay índice DIN disponible."}
            pozos = sorted(din_ok["NO_key"].dropna().unique().tolist())
            diags = load_all_diags_from_gcs(pozos)
            df = build_global_table(diags, _get_bat_map(), normalize_no_exact)
            if df.empty:
                return {"pozos": [], "total": 0}
            mask = df["_prob_lista"].apply(lambda l: isinstance(l, list) and any(problema in str(p).lower() for p in l))
            df_fil = df[mask][["Pozo", "Batería", "Fecha DIN", "Sev. máx", "Problemáticas"]].copy()
            return {"problema_buscado": problema, "pozos": df_fil.to_dict(orient="records"), "total": len(df_fil)}
        except Exception as e:
            return {"error": f"buscar_por_problematica: {e}"}

    # ── get_snapshot_pozos ─────────────────────────────────
    elif nombre == "get_snapshot_pozos":
        try:
            from core.gcs import load_snapshot
            bateria = args.get("bateria", "").strip()
            bajo = args.get("con_sumergencia_baja", False)
            snap = load_snapshot()
            if snap is None or snap.empty:
                return {"error": "No hay snapshot disponible."}
            if bateria and "Bateria" in snap.columns:
                snap = snap[snap["Bateria"].str.upper() == bateria.upper()]
            if bajo and "Sumergencia" in snap.columns:
                snap = snap[snap["Sumergencia"].notna() & (snap["Sumergencia"] < 50)]
            cols = [c for c in ["NO_key", "Bateria", "ORIGEN", "DT_plot", "Sumergencia",
                                 "Sumergencia_base", "Bba Llenado", "Caudal bruto efec",
                                 "%Balance", "%Estructura", "Dias_desde_ultima"] if c in snap.columns]
            total = len(snap)
            return {"total_pozos": total, "mostrando": min(50, total),
                    "nota": "Primeros 50 resultados." if total > 50 else "",
                    "pozos": _clean_records(snap[cols], 50)}
        except Exception as e:
            return {"error": f"get_snapshot_pozos: {e}"}

    # ── get_tendencias ─────────────────────────────────────
    elif nombre == "get_tendencias":
        try:
            from api.din import _load_indexes_with_keys
            from core.consolidado import build_global_consolidated
            import numpy as np
            variable = args.get("variable", "Sumergencia")
            solo_pos = args.get("solo_positiva", True)
            din_ok, niv_ok, col_map = _load_indexes_with_keys()
            df_all = build_global_consolidated(
                din_ok, niv_ok,
                col_map["din_no_col"], col_map["din_fe_col"], col_map["din_ho_col"],
                col_map["niv_no_col"], col_map["niv_fe_col"],
            )
            if df_all.empty or variable not in df_all.columns:
                return {"error": f"Variable '{variable}' no disponible. Opciones: Sumergencia, %Balance, %Estructura, Caudal bruto efec"}
            df_all["_dt"] = pd.to_datetime(df_all.get("din_datetime") or df_all.get("DT_plot"), errors="coerce")
            df_all["_mes"] = df_all["_dt"].dt.to_period("M").apply(lambda p: p.start_time if pd.notna(p) else pd.NaT)
            df_all[variable] = pd.to_numeric(df_all[variable], errors="coerce")
            resultados = []
            for pozo, grp in df_all.groupby("NO_key"):
                grp_v = grp[["_mes", variable]].dropna()
                if len(grp_v) < 3:
                    continue
                grp_v = grp_v.sort_values("_mes")
                x = (grp_v["_mes"] - grp_v["_mes"].min()).dt.days.values
                y = grp_v[variable].values
                if len(x) < 2:
                    continue
                try:
                    m, _ = np.polyfit(x, y, 1)
                    pendiente_mes = round(float(m) * 30, 2)
                except Exception:
                    continue
                if solo_pos and pendiente_mes <= 0:
                    continue
                resultados.append({"pozo": pozo, "pendiente_por_mes": pendiente_mes,
                                    "valor_inicial": round(float(y[0]), 1),
                                    "valor_final": round(float(y[-1]), 1),
                                    "n_puntos": len(grp_v)})
            resultados.sort(key=lambda r: abs(r["pendiente_por_mes"]), reverse=True)
            return {"variable": variable, "pozos": resultados[:30], "total": len(resultados)}
        except Exception as e:
            return {"error": f"get_tendencias: {e}"}

    # ── get_pozos_por_mes ──────────────────────────────────
    elif nombre == "get_pozos_por_mes":
        try:
            from api.din import _load_indexes_with_keys
            from core.consolidado import build_global_consolidated
            din_ok, niv_ok, col_map = _load_indexes_with_keys()
            df_all = build_global_consolidated(
                din_ok, niv_ok,
                col_map["din_no_col"], col_map["din_fe_col"], col_map["din_ho_col"],
                col_map["niv_no_col"], col_map["niv_fe_col"],
            )
            if df_all.empty:
                return {"error": "No hay datos consolidados."}
            fe_col = next((c for c in ["din_datetime", "DT_plot"] if c in df_all.columns), None)
            if not fe_col:
                return {"error": "No hay columna de fecha disponible."}
            df_all["_mes"] = pd.to_datetime(df_all[fe_col], errors="coerce").dt.to_period("M").astype(str)
            serie = df_all.groupby("_mes")["NO_key"].nunique().reset_index()
            serie.columns = ["Mes", "Pozos_medidos"]
            serie = serie.sort_values("Mes")
            ultimo = serie.iloc[-1] if not serie.empty else {}
            return {
                "ultimo_mes": str(ultimo.get("Mes", "?")),
                "ultimo_valor": int(ultimo.get("Pozos_medidos", 0)),
                "serie": serie.to_dict(orient="records"),
            }
        except Exception as e:
            return {"error": f"get_pozos_por_mes: {e}"}

    # ── get_historico_niv ──────────────────────────────────
    elif nombre == "get_historico_niv":
        try:
            from core.gcs import load_niv_index
            from core.parsers import normalize_no_exact, find_col, safe_to_float
            pozo_raw = args.get("pozo", "").strip()
            if not pozo_raw:
                return {"error": "Parámetro 'pozo' requerido."}
            no_key, err = _buscar_pozo_fuzzy(pozo_raw)
            if err:
                return {"error": err}
            df_niv = load_niv_index()
            if df_niv.empty:
                return {"error": "No hay índice NIV disponible."}
            no_col = find_col(df_niv, ["pozo", "NO"])
            if not no_col:
                return {"error": "No se encontró columna de pozo en índice NIV."}
            df_p = df_niv[df_niv[no_col].apply(normalize_no_exact) == no_key].copy()
            if df_p.empty:
                return {"error": f"No hay datos NIV para '{no_key}'."}
            fe_col = find_col(df_p, ["niv_datetime", "FE", "fecha"])
            registros = []
            for _, row in df_p.head(20).iterrows():
                r = {"fecha": str(row.get(fe_col, "?")) if fe_col else "?",
                     "PB": safe_to_float(row.get("PB")), "NC": safe_to_float(row.get("NC")),
                     "NM": safe_to_float(row.get("NM")), "ND": safe_to_float(row.get("ND"))}
                pb, nc = r["PB"], r["NC"]
                r["sumergencia_m"] = round(pb - nc, 1) if pb and nc else None
                registros.append(r)
            return {"pozo": no_key, "total": len(df_p), "mostrando": len(registros), "serie": registros}
        except Exception as e:
            return {"error": f"get_historico_niv: {e}"}

    # ── get_resumen_validaciones ───────────────────────────
    elif nombre == "get_resumen_validaciones":
        try:
            from api.validaciones import _load_snap_map
            from core.gcs import load_all_validaciones
            snap_map = _load_snap_map()
            if not snap_map:
                return {"error": "No hay datos de validaciones disponibles."}
            pozos = list(snap_map.keys())
            vals = load_all_validaciones(pozos)
            total_med = validadas = 0
            for no_key, data in vals.items():
                for fecha_key, estado in data.get("mediciones", {}).items():
                    total_med += 1
                    if estado.get("validada"):
                        validadas += 1
            return {"total_pozos": len(pozos), "total_mediciones": total_med,
                    "validadas": validadas, "pendientes": total_med - validadas}
        except Exception as e:
            return {"error": f"get_resumen_validaciones: {e}"}

    # ── get_semaforo_aib ───────────────────────────────────
    elif nombre == "get_semaforo_aib":
        try:
            from core.gcs import load_snapshot
            from core.parsers import normalize_no_exact
            bateria = args.get("bateria", "").strip()
            snap = load_snapshot()
            if snap is None or snap.empty:
                return {"error": "No hay snapshot disponible."}
            if "SE" in snap.columns:
                snap = snap[snap["SE"].str.upper() == "AIB"]
            if bateria and "Bateria" in snap.columns:
                snap = snap[snap["Bateria"].str.upper() == bateria.upper()]
            # Calcular semáforo básico
            def clasificar(row):
                s = row.get("Sumergencia")
                l = row.get("Bba Llenado")
                if s is None and l is None:
                    return "sin_datos"
                if (s is not None and s < 200) or (l is not None and l < 50):
                    return "critico"
                if (s is not None and s < 250) or (l is not None and l < 70):
                    return "alerta"
                return "normal"
            snap["_estado"] = snap.apply(clasificar, axis=1)
            counts = snap["_estado"].value_counts().to_dict()
            return {"total_aib": len(snap), "normal": counts.get("normal", 0),
                    "alerta": counts.get("alerta", 0), "critico": counts.get("critico", 0),
                    "sin_datos": counts.get("sin_datos", 0)}
        except Exception as e:
            return {"error": f"get_semaforo_aib: {e}"}

    # ── get_controles_merma ────────────────────────────────
    elif nombre == "get_controles_merma":
        try:
            df = _read_csv_gcs("controles/merma_por_pozo.csv")
            if df is None:
                return {"error": "Archivo de merma no encontrado. Ejecutá fetch_controles primero."}
            if args.get("solo_en_merma") and "EN_MERMA_NETA" in df.columns:
                df = df[df["EN_MERMA_NETA"] == True]
            bateria = args.get("bateria", "").strip()
            if bateria and "BATERIA" in df.columns:
                df = df[df["BATERIA"].str.upper() == bateria.upper()]
            return {"total": len(df), "mostrando": min(50, len(df)), "pozos": _clean_records(df, 50)}
        except Exception as e:
            return {"error": f"get_controles_merma: {e}"}

    # ── get_controles_historico ────────────────────────────
    elif nombre == "get_controles_historico":
        try:
            df = _read_csv_gcs("controles/historico_CRUDO.csv")
            if df is None:
                return {"error": "Archivo histórico no encontrado. Ejecutá fetch_controles primero."}
            if "Fecha y Hora" in df.columns:
                df["Fecha y Hora"] = pd.to_datetime(df["Fecha y Hora"], errors="coerce")
            pozo = args.get("pozo", "").strip()
            bateria = args.get("bateria", "").strip()
            if pozo and "Pozo" in df.columns:
                df = df[df["Pozo"].str.upper() == pozo.upper()]
            if bateria and "BATERIA" in df.columns:
                df = df[df["BATERIA"].str.upper() == bateria.upper()]
            if args.get("fecha_desde") and "Fecha y Hora" in df.columns:
                df = df[df["Fecha y Hora"] >= pd.to_datetime(args["fecha_desde"], errors="coerce")]
            if args.get("fecha_hasta") and "Fecha y Hora" in df.columns:
                df = df[df["Fecha y Hora"] <= pd.to_datetime(args["fecha_hasta"], errors="coerce")]
            for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M")
            return {"total": len(df), "mostrando": min(100, len(df)), "registros": _clean_records(df, 100)}
        except Exception as e:
            return {"error": f"get_controles_historico: {e}"}

    # ── get_kpis_acciones ──────────────────────────────────
    elif nombre == "get_kpis_acciones":
        try:
            from core.acciones import get_kpis_acciones
            return get_kpis_acciones()
        except Exception as e:
            return {"error": f"get_kpis_acciones: {e}"}

    # ── get_acciones ───────────────────────────────────────
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

    # ── get_rrhh_personal ──────────────────────────────────
    elif nombre == "get_rrhh_personal":
        try:
            from core.rrhh_db import list_personal
            personal = list_personal()
            return {"total": len(personal), "personal": personal[:50]}
        except Exception as e:
            return {"error": f"get_rrhh_personal: {e}"}

    # ── get_rrhh_pendientes ────────────────────────────────
    elif nombre == "get_rrhh_pendientes":
        try:
            from core.rrhh_db import list_pendientes_lider
            leader = args.get("leader_legajo", "").strip()
            if not leader:
                return {"error": "Parámetro 'leader_legajo' requerido."}
            pendientes = list_pendientes_lider(leader)
            return {"leader_legajo": leader, "total": len(pendientes), "pendientes": pendientes}
        except Exception as e:
            return {"error": f"get_rrhh_pendientes: {e}"}

    return {"error": f"Tool desconocida: {nombre}"}


# ==========================================================
# System prompt
# ==========================================================

SYSTEM_PROMPT = """Sos el asistente técnico de la Plataforma DINA,
sistema de análisis dinamométrico e inteligencia operativa de pozos petroleros.

REGLAS:
1. Usás las tools para obtener datos reales ANTES de responder.
2. Si una tool devuelve {"error": "..."}, informás el error al usuario.
3. NUNCA inventás valores, pozos, fechas ni datos.
4. Respondés en español, de forma concisa y técnica.
5. Podés combinar múltiples tools en una misma respuesta si hace falta.
6. Para preguntas de un pozo específico → get_diagnostico_pozo + get_historico_niv.
7. Para estado general del campo → get_snapshot_pozos + get_kpis_diagnosticos.
8. Para producción → get_controles_merma + get_controles_historico.
9. Para acciones de optimización → get_kpis_acciones + get_acciones.
10. Para RRHH → get_rrhh_personal o get_rrhh_pendientes.

Contexto técnico para interpretar datos:
- sumergencia: metros de fluido sobre la bomba (más es mejor)
- llenado / Bba Llenado: % de llenado de la cámara de la bomba (>70% = ok)
- pct_merma_neta: caída % de producción neta respecto al período anterior
- dias_sin_control: días sin medición de producción
- %Balance: balance mecánico de la unidad AIB (ideal ~100%)
- %Estructura: carga sobre la estructura (no superar 100%)"""


# ==========================================================
# Debug endpoint
# ==========================================================

@router.get("/debug")
async def chat_debug():
    """Verifica acceso a datos de todas las fuentes."""
    resultados = {}
    for tool, targs in [
        ("get_kpis_diagnosticos", {}),
        ("get_snapshot_pozos",    {}),
        ("get_kpis_acciones",     {}),
        ("get_controles_merma",   {"solo_en_merma": True}),
        ("get_resumen_validaciones", {}),
        ("get_semaforo_aib",      {}),
        ("get_rrhh_personal",     {}),
    ]:
        r = _ejecutar_tool(tool, targs)
        # Truncar listas para que sea legible
        if isinstance(r, dict):
            for k in ["pozos", "registros", "acciones", "personal", "serie"]:
                if k in r and isinstance(r[k], list):
                    r[k] = r[k][:2]
        resultados[tool] = r
    return resultados


# ==========================================================
# Chat endpoint
# ==========================================================

@router.post("", response_model=ChatResponse)
async def chat(req: ChatRequest):
    from ia.diagnostico import get_openai_key
    from openai import OpenAI

    api_key = get_openai_key()
    if not api_key:
        return ChatResponse(respuesta="⚠️ No hay clave OpenAI configurada.", tools_usadas=[])

    client = OpenAI(api_key=api_key)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for msg in req.historial[-10:]:
        if msg.get("role") in ("user", "assistant") and msg.get("content"):
            messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": req.mensaje})

    tools_usadas: list[str] = []
    response = client.chat.completions.create(
        model="gpt-5.2", messages=messages, tools=TOOLS,
        tool_choice="auto", max_tokens=1500, temperature=0,
    )
    msg_resp = response.choices[0].message

    if msg_resp.tool_calls:
        messages.append(msg_resp)
        for tc in msg_resp.tool_calls:
            nombre_tool = tc.function.name
            tools_usadas.append(nombre_tool)
            try:
                targs = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                targs = {}
            resultado = _ejecutar_tool(nombre_tool, targs)
            messages.append({
                "role": "tool", "tool_call_id": tc.id,
                "content": json.dumps(resultado, ensure_ascii=False, default=str),
            })
        response2 = client.chat.completions.create(
            model="gpt-4o-mini", messages=messages, max_tokens=1500, temperature=0,
        )
        respuesta_final = response2.choices[0].message.content or ""
    else:
        respuesta_final = msg_resp.content or ""

    return ChatResponse(respuesta=respuesta_final.strip(), tools_usadas=tools_usadas)

# ── PARCHE: agregar tools faltantes ──────────────────────────────────────────
# Este bloque se agrega al final para no romper el archivo existente.
# Se sobreescribe TOOLS y se extiende _ejecutar_tool via monkey-patch.

_TOOLS_EXTRA = [
    {"type":"function","function":{
        "name":"get_historico_sumergencia",
        "description":"Serie temporal de sumergencia de un pozo: evolución en el tiempo con fecha, valor, PB, nivel usado y origen (DIN/NIV). Usar para: 'cómo evolucionó la sumergencia de X', 'tendencia histórica'.",
        "parameters":{"type":"object","properties":{
            "pozo":{"type":"string","description":"Nombre del pozo."}},"required":["pozo"]}}},

    {"type":"function","function":{
        "name":"get_stats_campo",
        "description":"Estadísticas generales del campo: distribución de sumergencia (media, mediana, min, max, percentiles), semáforo AIB completo, KPIs de cobertura.",
        "parameters":{"type":"object","properties":{},"required":[]}}},

    {"type":"function","function":{
        "name":"get_downtimes_perdidas",
        "description":"Histórico de pérdidas de producción (wellDowntimes): shortfall de petróleo, gas, líquido por pozo, fecha y rubro. Módulo 'Histórico de Pérdidas'. Diferente a merma por pozo.",
        "parameters":{"type":"object","properties":{
            "pozo":{"type":"string","description":"Nombre del pozo (opcional)."},
            "fecha_desde":{"type":"string","description":"Fecha desde YYYY-MM-DD."},
            "fecha_hasta":{"type":"string","description":"Fecha hasta YYYY-MM-DD."}},"required":[]}}},

    {"type":"function","function":{
        "name":"get_validaciones_pozo",
        "description":"Estado de validaciones de sumergencia de un pozo específico: qué mediciones están validadas, pendientes, con comentarios.",
        "parameters":{"type":"object","properties":{
            "pozo":{"type":"string","description":"Nombre del pozo."}},"required":["pozo"]}}},

    {"type":"function","function":{
        "name":"get_cobertura_din",
        "description":"Cobertura de mediciones DIN en un período: cuántos pozos tienen medición, cuáles no. Útil para: '¿qué pozos no se midieron este mes?'.",
        "parameters":{"type":"object","properties":{
            "fecha_desde":{"type":"string","description":"Fecha desde YYYY-MM-DD."},
            "fecha_hasta":{"type":"string","description":"Fecha hasta YYYY-MM-DD."}},"required":["fecha_desde","fecha_hasta"]}}},

    {"type":"function","function":{
        "name":"get_detalle_pozo",
        "description":"Detalle completo de un pozo: última medición DIN/NIV, histórico reciente de sumergencia, estado semáforo, coordenadas, batería.",
        "parameters":{"type":"object","properties":{
            "pozo":{"type":"string","description":"Nombre del pozo."}},"required":["pozo"]}}},

    {"type":"function","function":{
        "name":"get_rrhh_consolidado",
        "description":"Consolidado de partes de guardia de un líder: horas, guardias, feriados, estado de aprobación por empleado.",
        "parameters":{"type":"object","properties":{
            "leader_legajo":{"type":"string","description":"Legajo del líder."},
            "periodo":{"type":"string","description":"Período en formato YYYY-MM."}},"required":["leader_legajo","periodo"]}}},
]

# Extender la lista TOOLS global
TOOLS.extend(_TOOLS_EXTRA)

# Guardar referencia a la función original
_ejecutar_tool_original = _ejecutar_tool

def _ejecutar_tool(nombre: str, args: dict) -> Any:  # type: ignore[no-redef]
    # Primero intentar con las tools originales
    result = _ejecutar_tool_original(nombre, args)
    if not (isinstance(result, dict) and result.get("error") == f"Tool desconocida: {nombre}"):
        return result

    # ── get_historico_sumergencia ──────────────────────────
    if nombre == "get_historico_sumergencia":
        try:
            from api.din import _load_indexes_with_keys
            from core.consolidado import build_pozo_consolidado
            from core.parsers import normalize_no_exact, safe_to_float
            pozo_raw = args.get("pozo", "").strip()
            if not pozo_raw:
                return {"error": "Parámetro 'pozo' requerido."}
            no_key, err = _buscar_pozo_fuzzy(pozo_raw)
            if err:
                return {"error": err}
            din_ok, niv_ok, col_map = _load_indexes_with_keys()
            dfp = build_pozo_consolidado(
                din_ok, niv_ok, no_key,
                col_map["din_no_col"], col_map["din_fe_col"], col_map["din_ho_col"],
                col_map["niv_no_col"], col_map["niv_fe_col"], col_map["niv_ho_col"],
            )
            if dfp.empty or "Sumergencia" not in dfp.columns:
                return {"error": f"No hay datos de sumergencia para '{no_key}'."}
            dfp["Sumergencia"] = pd.to_numeric(dfp["Sumergencia"], errors="coerce")
            dfp = dfp.dropna(subset=["DT_plot", "Sumergencia"]).sort_values("DT_plot")
            serie = []
            for _, row in dfp.iterrows():
                dt = row.get("DT_plot")
                pb = safe_to_float(row.get("PB"))
                base = row.get("Sumergencia_base")
                nivel = safe_to_float(row.get(base) if base else None)
                serie.append({
                    "dt": dt.isoformat() if hasattr(dt, "isoformat") else str(dt),
                    "sumergencia": round(float(row["Sumergencia"]), 1),
                    "pb": pb, "base": base, "nivel_usado": nivel,
                    "origen": row.get("ORIGEN", ""),
                })
            return {"pozo": no_key, "total_puntos": len(serie), "serie": serie[-30:],
                    "nota": "Últimos 30 puntos." if len(serie) > 30 else ""}
        except Exception as e:
            return {"error": f"get_historico_sumergencia: {e}"}

    # ── get_stats_campo ────────────────────────────────────
    elif nombre == "get_stats_campo":
        try:
            from api.mapa import _load_indexes_ok, _build_snap_con_coords
            din_ok, niv_ok, _ = _load_indexes_ok()
            snap = _build_snap_con_coords(din_ok, niv_ok)
            if snap.empty:
                return {"error": "No hay snapshot disponible."}
            total = len(snap)
            # Sumergencia stats
            s_col = snap["Sumergencia"] if "Sumergencia" in snap.columns else pd.Series(dtype=float)
            s_num = pd.to_numeric(s_col, errors="coerce").dropna()
            sumer_stats = {}
            if not s_num.empty:
                sumer_stats = {
                    "media": round(float(s_num.mean()), 1),
                    "mediana": round(float(s_num.median()), 1),
                    "min": round(float(s_num.min()), 1),
                    "max": round(float(s_num.max()), 1),
                    "p25": round(float(s_num.quantile(0.25)), 1),
                    "p75": round(float(s_num.quantile(0.75)), 1),
                    "con_sumergencia": int(len(s_num)),
                    "sin_sumergencia": int(total - len(s_num)),
                }
            # Llenado stats
            l_col = snap.get("Bba Llenado", pd.Series(dtype=float))
            l_num = pd.to_numeric(l_col, errors="coerce").dropna()
            llen_stats = {}
            if not l_num.empty:
                llen_stats = {
                    "media": round(float(l_num.mean()), 1),
                    "bajo_50pct": int((l_num < 50).sum()),
                    "entre_50_70": int(((l_num >= 50) & (l_num < 70)).sum()),
                    "ok_70plus": int((l_num >= 70).sum()),
                }
            # Dias sin medicion
            d_col = snap.get("Dias_desde_ultima", pd.Series(dtype=float))
            d_num = pd.to_numeric(d_col, errors="coerce").dropna()
            dias_stats = {}
            if not d_num.empty:
                dias_stats = {
                    "media_dias": round(float(d_num.mean()), 1),
                    "mas_de_90_dias": int((d_num > 90).sum()),
                    "mas_de_30_dias": int((d_num > 30).sum()),
                }
            return {
                "total_pozos": total,
                "sumergencia": sumer_stats,
                "llenado_bomba": llen_stats,
                "dias_sin_medicion": dias_stats,
            }
        except Exception as e:
            return {"error": f"get_stats_campo: {e}"}

    # ── get_downtimes_perdidas ─────────────────────────────
    elif nombre == "get_downtimes_perdidas":
        try:
            df = _read_csv_gcs("merma/wellDowntimes_CRUDO.csv")
            if df is None:
                return {"error": "Archivo de downtimes no encontrado. Ejecutá fetch_downtimes primero."}
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
            # Columnas relevantes
            cols_utiles = [c for c in ["POZO", "RUBRO", "FECHA DESDE", "FECHA HASTA",
                                        "oilShortfall", "waterShortfall", "liquidShortfall",
                                        "gasShortfall", "potentialOil", "potentialLiquid"] if c in df.columns]
            df = df[cols_utiles] if cols_utiles else df
            return {"total": len(df), "mostrando": min(50, len(df)), "registros": _clean_records(df, 50)}
        except Exception as e:
            return {"error": f"get_downtimes_perdidas: {e}"}

    # ── get_validaciones_pozo ──────────────────────────────
    elif nombre == "get_validaciones_pozo":
        try:
            from core.gcs import load_validaciones
            from core.parsers import normalize_no_exact
            pozo_raw = args.get("pozo", "").strip()
            if not pozo_raw:
                return {"error": "Parámetro 'pozo' requerido."}
            no_key, err = _buscar_pozo_fuzzy(pozo_raw)
            if err:
                return {"error": err}
            val_data = load_validaciones(no_key)
            mediciones = val_data.get("mediciones", {})
            validadas = sum(1 for v in mediciones.values() if v.get("validada"))
            pendientes = len(mediciones) - validadas
            detalle = [
                {"fecha": fecha, "validada": v.get("validada", False),
                 "comentario": v.get("comentario", ""), "usuario": v.get("usuario", "")}
                for fecha, v in sorted(mediciones.items(), reverse=True)
            ]
            return {"pozo": no_key, "total_mediciones": len(mediciones),
                    "validadas": validadas, "pendientes": pendientes,
                    "detalle": detalle[:20]}
        except Exception as e:
            return {"error": f"get_validaciones_pozo: {e}"}

    # ── get_cobertura_din ──────────────────────────────────
    elif nombre == "get_cobertura_din":
        try:
            from api.din import _load_indexes_with_keys
            from core.consolidado import build_global_consolidated
            from core.gcs import load_coords_repo
            from core.parsers import normalize_no_exact
            fecha_desde = args.get("fecha_desde", "")
            fecha_hasta = args.get("fecha_hasta", "")
            if not fecha_desde or not fecha_hasta:
                return {"error": "Se requieren fecha_desde y fecha_hasta (YYYY-MM-DD)."}
            din_ok, niv_ok, col_map = _load_indexes_with_keys()
            df_all = build_global_consolidated(
                din_ok, niv_ok,
                col_map["din_no_col"], col_map["din_fe_col"], col_map["din_ho_col"],
                col_map["niv_no_col"], col_map["niv_fe_col"],
            )
            coords = load_coords_repo()
            todos_pozos = set()
            if not coords.empty and "nombre_corto" in coords.columns:
                todos_pozos = set(coords["nombre_corto"].apply(normalize_no_exact).dropna().unique())
            fe_col = next((c for c in ["din_datetime", "DT_plot"] if c in df_all.columns), None)
            if not fe_col:
                return {"error": "No hay columna de fecha."}
            df_all["_dt"] = pd.to_datetime(df_all[fe_col], errors="coerce")
            mask = (df_all["_dt"] >= pd.to_datetime(fecha_desde)) & (df_all["_dt"] <= pd.to_datetime(fecha_hasta))
            df_rango = df_all[mask]
            con_din = set(df_rango["NO_key"].dropna().apply(normalize_no_exact).unique())
            sin_din = sorted(todos_pozos - con_din) if todos_pozos else []
            return {
                "periodo": f"{fecha_desde} → {fecha_hasta}",
                "total_pozos": len(todos_pozos),
                "con_medicion": len(con_din),
                "sin_medicion": len(sin_din),
                "lista_sin_medicion": sin_din[:30],
                "nota": f"Primeros 30 de {len(sin_din)} sin medición." if len(sin_din) > 30 else "",
            }
        except Exception as e:
            return {"error": f"get_cobertura_din: {e}"}

    # ── get_detalle_pozo ───────────────────────────────────
    elif nombre == "get_detalle_pozo":
        try:
            from api.mapa import _load_indexes_ok, _build_snap_con_coords
            from core.parsers import normalize_no_exact
            pozo_raw = args.get("pozo", "").strip()
            if not pozo_raw:
                return {"error": "Parámetro 'pozo' requerido."}
            no_key, err = _buscar_pozo_fuzzy(pozo_raw)
            if err:
                return {"error": err}
            din_ok, niv_ok, _ = _load_indexes_ok()
            snap = _build_snap_con_coords(din_ok, niv_ok)
            fila = snap[snap["NO_key"] == no_key]
            if fila.empty:
                return {"error": f"Pozo '{no_key}' no encontrado en snapshot."}
            row = fila.iloc[0].where(fila.iloc[0].notna(), other=None).to_dict()
            # Limpiar NaN
            row = {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
            return {"pozo": no_key, "datos": row}
        except Exception as e:
            return {"error": f"get_detalle_pozo: {e}"}

    # ── get_rrhh_consolidado ───────────────────────────────
    elif nombre == "get_rrhh_consolidado":
        try:
            from core.rrhh_db import get_consolidado, period_display, period_bounds, current_period_id
            leader = args.get("leader_legajo", "").strip()
            periodo = args.get("periodo", "").strip()
            if not leader:
                return {"error": "Parámetro 'leader_legajo' requerido."}
            if not periodo:
                periodo = current_period_id()
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

    return {"error": f"Tool desconocida: {nombre}"}

# ==========================================================
# PARCHE v7 — 8 tools faltantes tras audit completo
# ==========================================================

_TOOLS_V7 = [
    {"type":"function","function":{
        "name":"get_info_sistema",
        "description":"Estado general del sistema DINA: versión, conexión GCS, conexión OpenAI, cantidad de archivos DIN y NIV indexados. Usar para: '¿está todo bien?', '¿cuántos pozos están indexados?', 'estado del sistema'.",
        "parameters":{"type":"object","properties":{},"required":[]}}},

    {"type":"function","function":{
        "name":"get_mediciones_din_pozo",
        "description":"Mediciones detalladas DIN+NIV de un pozo: todos los campos mecánicos (carrera AIB, contrapeso actual e ideal, %Balance, %Estructura, GPM, Bba Prof, Bba Llenado, Caudal bruto, torque, RPM motor, etc). Usar para análisis mecánico profundo de un pozo.",
        "parameters":{"type":"object","properties":{
            "pozo":{"type":"string","description":"Nombre del pozo."}},"required":["pozo"]}}},

    {"type":"function","function":{
        "name":"get_lista_baterias",
        "description":"Lista completa de baterías disponibles en el sistema con cantidad de pozos por batería.",
        "parameters":{"type":"object","properties":{},"required":[]}}},

    {"type":"function","function":{
        "name":"get_kpis_controles",
        "description":"KPIs del módulo Controles de Producción: total de registros, pozos únicos, días promedio sin control, pozos en merma neta.",
        "parameters":{"type":"object","properties":{},"required":[]}}},

    {"type":"function","function":{
        "name":"get_kpis_perdidas",
        "description":"KPIs del módulo Histórico de Pérdidas (downtimes): total oilShortfall, gasShortfall, liquidShortfall acumulados. Usar para: '¿cuánto petróleo perdimos?', resumen de pérdidas.",
        "parameters":{"type":"object","properties":{
            "fecha_desde":{"type":"string","description":"Fecha desde YYYY-MM-DD (opcional)."},
            "fecha_hasta":{"type":"string","description":"Fecha hasta YYYY-MM-DD (opcional)."}},"required":[]}}},

    {"type":"function","function":{
        "name":"get_rrhh_periodos",
        "description":"Períodos disponibles en el sistema de RRHH (los últimos 8 períodos: fechas de inicio y fin de cada guardia mensual).",
        "parameters":{"type":"object","properties":{},"required":[]}}},

    {"type":"function","function":{
        "name":"get_rrhh_bitacora",
        "description":"Historial completo de partes de guardia de un empleado: todos los períodos, estado (BORRADOR/ENVIADO/APROBADO/RECHAZADO), fechas de envío y aprobación.",
        "parameters":{"type":"object","properties":{
            "legajo":{"type":"string","description":"Legajo del empleado."}},"required":["legajo"]}}},

    {"type":"function","function":{
        "name":"get_rrhh_parte",
        "description":"Detalle completo del parte de guardia de un empleado en un período: día a día con guardias (G), feriados (F), días libres (D), horas extra (HE), horas viaje (HV) y comentarios.",
        "parameters":{"type":"object","properties":{
            "legajo":{"type":"string","description":"Legajo del empleado."},
            "periodo":{"type":"string","description":"Período YYYY-MM. Si no se indica, usa el período actual."}},"required":["legajo"]}}},
]

TOOLS.extend(_TOOLS_V7)

_ejecutar_tool_v6 = _ejecutar_tool

def _ejecutar_tool(nombre: str, args: dict) -> Any:  # type: ignore[no-redef]
    result = _ejecutar_tool_v6(nombre, args)
    if not (isinstance(result, dict) and result.get("error") == f"Tool desconocida: {nombre}"):
        return result

    # ── get_info_sistema ───────────────────────────────────
    if nombre == "get_info_sistema":
        try:
            from core.gcs import get_gcs_client, GCS_BUCKET, GCS_PREFIX, load_din_index, load_niv_index
            import importlib.metadata, os
            gcs_ok = False
            try:
                client = get_gcs_client()
                if client and GCS_BUCKET:
                    client.bucket(GCS_BUCKET).exists()
                    gcs_ok = True
            except Exception:
                pass
            openai_ok = False
            try:
                from ia.diagnostico import get_openai_key
                openai_ok = bool(get_openai_key())
            except Exception:
                pass
            din_count = niv_count = 0
            try:
                df_din = load_din_index()
                din_count = len(df_din) if df_din is not None else 0
            except Exception:
                pass
            try:
                df_niv = load_niv_index()
                niv_count = len(df_niv) if df_niv is not None else 0
            except Exception:
                pass
            return {
                "gcs_ok": gcs_ok,
                "openai_ok": openai_ok,
                "din_registros_indexados": din_count,
                "niv_registros_indexados": niv_count,
                "gcs_bucket": GCS_BUCKET or "no configurado",
                "gcs_prefix": GCS_PREFIX or "(raíz)",
            }
        except Exception as e:
            return {"error": f"get_info_sistema: {e}"}

    # ── get_mediciones_din_pozo ────────────────────────────
    elif nombre == "get_mediciones_din_pozo":
        try:
            from api.din import _load_indexes_with_keys
            from core.consolidado import build_pozo_consolidado
            from core.parsers import normalize_no_exact, safe_to_float, parse_din_extras, EXTRA_FIELDS, resolve_existing_path
            pozo_raw = args.get("pozo", "").strip()
            if not pozo_raw:
                return {"error": "Parámetro 'pozo' requerido."}
            no_key, err = _buscar_pozo_fuzzy(pozo_raw)
            if err:
                return {"error": err}
            din_ok, niv_ok, col_map = _load_indexes_with_keys()
            dfp = build_pozo_consolidado(
                din_ok, niv_ok, no_key,
                col_map["din_no_col"], col_map["din_fe_col"], col_map["din_ho_col"],
                col_map["niv_no_col"], col_map["niv_fe_col"], col_map["niv_ho_col"],
            )
            if dfp.empty:
                return {"error": f"No hay mediciones para '{no_key}'."}
            # Enriquecer con extras DIN
            extra_rows = []
            if "path" in dfp.columns:
                for _, row in dfp.iterrows():
                    if row.get("ORIGEN") == "DIN":
                        try:
                            p_res = resolve_existing_path(row.get("path"))
                            extra_rows.append(parse_din_extras(str(p_res)) if p_res else {k: None for k in EXTRA_FIELDS})
                        except Exception:
                            extra_rows.append({k: None for k in EXTRA_FIELDS})
                    else:
                        extra_rows.append({k: None for k in EXTRA_FIELDS})
            else:
                extra_rows = [{k: None for k in EXTRA_FIELDS} for _ in range(len(dfp))]
            df_extra = pd.DataFrame(extra_rows)
            for c in df_extra.columns:
                if c in dfp.columns:
                    dfp = dfp.drop(columns=[c])
            dfp = pd.concat([dfp.reset_index(drop=True), df_extra.reset_index(drop=True)], axis=1)
            # Serializar fechas
            for col in dfp.select_dtypes(include=["datetime64[ns]"]).columns:
                dfp[col] = dfp[col].dt.strftime("%Y-%m-%d %H:%M")
            cols_utiles = [c for c in [
                "ORIGEN", "DT_plot", "Sumergencia", "PB", "NM", "NC", "ND",
                "Bba Llenado", "Caudal bruto efec", "%Balance", "%Estructura",
                "AIB Carrera", "Contrapeso actual", "Contrapeso ideal",
                "Distancia contrapesos (cm)", "AIBEB_Torque max contrapeso",
                "Bba Prof", "Bba Diam Pistón", "GPM",
                "Polea Motor", "Potencia Motor", "RPM Motor",
            ] if c in dfp.columns]
            return {
                "pozo": no_key,
                "total": len(dfp),
                "mostrando": min(10, len(dfp)),
                "nota": "Últimas 10 mediciones." if len(dfp) > 10 else "",
                "mediciones": _clean_records(dfp[cols_utiles].sort_values("DT_plot", ascending=False) if "DT_plot" in dfp.columns else dfp[cols_utiles], 10),
            }
        except Exception as e:
            return {"error": f"get_mediciones_din_pozo: {e}"}

    # ── get_lista_baterias ─────────────────────────────────
    elif nombre == "get_lista_baterias":
        try:
            from core.gcs import load_snapshot
            snap = load_snapshot()
            if snap is None or snap.empty:
                return {"error": "No hay snapshot disponible."}
            bat_col = next((c for c in ["Bateria", "nivel_5", "Batería"] if c in snap.columns), None)
            if not bat_col:
                return {"error": "No se encontró columna de batería en snapshot."}
            counts = snap[bat_col].dropna().value_counts().reset_index()
            counts.columns = ["bateria", "pozos"]
            counts = counts.sort_values("bateria")
            return {
                "total_baterias": len(counts),
                "baterias": counts.to_dict(orient="records"),
            }
        except Exception as e:
            return {"error": f"get_lista_baterias: {e}"}

    # ── get_kpis_controles ─────────────────────────────────
    elif nombre == "get_kpis_controles":
        try:
            df_hist = _read_csv_gcs("controles/historico_CRUDO.csv")
            df_merma = _read_csv_gcs("controles/merma_por_pozo.csv")
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
                        result["dias_max_sin_control"] = int(dias.max())
            if not result:
                return {"error": "No hay datos de controles disponibles."}
            return result
        except Exception as e:
            return {"error": f"get_kpis_controles: {e}"}

    # ── get_kpis_perdidas ──────────────────────────────────
    elif nombre == "get_kpis_perdidas":
        try:
            df = _read_csv_gcs("merma/wellDowntimes_CRUDO.csv")
            if df is None:
                return {"error": "Archivo de downtimes no encontrado."}
            for col in ["FECHA DESDE", "FECHA HASTA"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
            if args.get("fecha_desde") and "FECHA DESDE" in df.columns:
                df = df[df["FECHA DESDE"] >= pd.to_datetime(args["fecha_desde"], errors="coerce")]
            if args.get("fecha_hasta") and "FECHA DESDE" in df.columns:
                df = df[df["FECHA DESDE"] <= pd.to_datetime(args["fecha_hasta"], errors="coerce")]
            result: dict = {"total_registros": len(df)}
            for col in ["oilShortfall", "gasShortfall", "liquidShortfall", "waterShortfall",
                        "potentialOil", "potentialLiquid"]:
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

    # ── get_rrhh_periodos ──────────────────────────────────
    elif nombre == "get_rrhh_periodos":
        try:
            from core.rrhh_db import recent_periods, current_period_id
            return {
                "periodo_actual": current_period_id(),
                "periodos": recent_periods(8),
            }
        except Exception as e:
            return {"error": f"get_rrhh_periodos: {e}"}

    # ── get_rrhh_bitacora ──────────────────────────────────
    elif nombre == "get_rrhh_bitacora":
        try:
            from core.rrhh_db import list_bitacora
            legajo = args.get("legajo", "").strip()
            if not legajo:
                return {"error": "Parámetro 'legajo' requerido."}
            partes = list_bitacora(legajo)
            return {
                "legajo": legajo,
                "total_partes": len(partes),
                "partes": partes,
            }
        except Exception as e:
            return {"error": f"get_rrhh_bitacora: {e}"}

    # ── get_rrhh_parte ─────────────────────────────────────
    elif nombre == "get_rrhh_parte":
        try:
            from api.rrhh import _build_parte_response
            from core.rrhh_db import current_period_id
            legajo = args.get("legajo", "").strip()
            if not legajo:
                return {"error": "Parámetro 'legajo' requerido."}
            periodo = args.get("periodo", "").strip() or current_period_id()
            return _build_parte_response(legajo, periodo)
        except Exception as e:
            return {"error": f"get_rrhh_parte: {e}"}

    return {"error": f"Tool desconocida: {nombre}"}
