# =============================================================
# backend/ia/agents/agent_pozos.py
# Agente: mediciones DIN/NIV, snapshot, sumergencia, tendencias.
# =============================================================
from __future__ import annotations
from typing import Any
import pandas as pd
from .base_agent import BaseAgent, buscar_pozo_fuzzy, clean_records


class AgentePozos(BaseAgent):

    NOMBRE = "pozos"

    TOOLS = [
        {"type": "function", "function": {
            "name": "get_snapshot_pozos",
            "description": "Estado actual de pozos: sumergencia, llenado de bomba, caudal, balance, estructura, días desde última medición. Filtrable por pozo, batería o sumergencia baja.",
            "parameters": {"type": "object", "properties": {
                "pozo":                {"type": "string",  "description": "Nombre del pozo (búsqueda aproximada)."},
                "bateria":             {"type": "string",  "description": "Filtrar por batería."},
                "con_sumergencia_baja": {"type": "boolean", "description": "Si true, solo pozos con sumergencia < 50m."},
            }, "required": []}}},

        {"type": "function", "function": {
            "name": "get_historico_sumergencia",
            "description": "Serie temporal de sumergencia de un pozo: evolución histórica con fecha, valor, PB y origen (DIN/NIV).",
            "parameters": {"type": "object", "properties": {
                "pozo": {"type": "string", "description": "Nombre del pozo."},
            }, "required": ["pozo"]}}},

        {"type": "function", "function": {
            "name": "get_mediciones_din_pozo",
            "description": "Mediciones mecánicas DIN+NIV de un pozo: carrera AIB, contrapesos, %Balance, %Estructura, GPM, Bba Prof, torque, RPM motor.",
            "parameters": {"type": "object", "properties": {
                "pozo": {"type": "string", "description": "Nombre del pozo."},
            }, "required": ["pozo"]}}},

        {"type": "function", "function": {
            "name": "get_historico_niv",
            "description": "Histórico de niveles NIV de un pozo: serie temporal de NC, NM, ND, PB y sumergencia calculada.",
            "parameters": {"type": "object", "properties": {
                "pozo": {"type": "string", "description": "Nombre del pozo."},
            }, "required": ["pozo"]}}},

        {"type": "function", "function": {
            "name": "get_tendencias",
            "description": "Tendencias de una variable por pozo (pendiente mensual). Pozos que mejoran o empeoran en sumergencia, balance, estructura o caudal.",
            "parameters": {"type": "object", "properties": {
                "variable":      {"type": "string",  "description": "Sumergencia, %Balance, %Estructura, Caudal bruto efec. Default: Sumergencia."},
                "solo_positiva": {"type": "boolean", "description": "Si true, solo tendencia creciente. Default true."},
            }, "required": []}}},

        {"type": "function", "function": {
            "name": "get_stats_campo",
            "description": "Estadísticas generales del campo: distribución de sumergencia, llenado de bomba, días sin medición.",
            "parameters": {"type": "object", "properties": {}, "required": []}}},

        {"type": "function", "function": {
            "name": "get_pozos_por_mes",
            "description": "Cantidad de pozos medidos por mes. Cobertura histórica de mediciones DIN/NIV.",
            "parameters": {"type": "object", "properties": {}, "required": []}}},

        {"type": "function", "function": {
            "name": "get_cobertura_din",
            "description": "Cobertura de mediciones DIN en un período: cuántos pozos se midieron y cuáles no.",
            "parameters": {"type": "object", "properties": {
                "fecha_desde": {"type": "string", "description": "Fecha desde YYYY-MM-DD."},
                "fecha_hasta": {"type": "string", "description": "Fecha hasta YYYY-MM-DD."},
            }, "required": ["fecha_desde", "fecha_hasta"]}}},

        {"type": "function", "function": {
            "name": "get_lista_baterias",
            "description": "Lista completa de baterías con cantidad de pozos por batería.",
            "parameters": {"type": "object", "properties": {}, "required": []}}},

        {"type": "function", "function": {
            "name": "get_info_sistema",
            "description": "Estado del sistema DINA: conexión GCS, OpenAI, registros DIN y NIV indexados.",
            "parameters": {"type": "object", "properties": {}, "required": []}}},
    ]

    SYSTEM_PROMPT = """Sos el Agente de Pozos de la Plataforma DINA.
Tu especialidad son las mediciones DIN/NIV, el estado actual del campo,
sumergencia, llenado de bomba, caudal, balance mecánico y tendencias históricas.

REGLAS:
- Siempre usás tools para obtener datos reales. Nunca inventás valores.
- Para un pozo específico: get_snapshot_pozos(pozo=nombre), luego get_historico_sumergencia si falta sumergencia.
- Para el campo en general: get_stats_campo + get_lista_baterias.
- Respondés en español, de forma técnica y concisa.

CONTEXTO TÉCNICO:
- sumergencia > 200m = buena; < 100m = crítica.
- Bba Llenado > 70% = ok; < 50% = problema.
- %Balance ideal ≈ 100%."""

    def _ejecutar_tool(self, nombre: str, args: dict) -> Any:

        if nombre == "get_snapshot_pozos":
            try:
                from core.gcs import load_snapshot
                pozo_raw = args.get("pozo", "").strip()
                bateria  = args.get("bateria", "").strip()
                bajo     = args.get("con_sumergencia_baja", False)
                snap = load_snapshot()
                if snap is None or snap.empty:
                    return {"error": "No hay snapshot disponible."}
                if pozo_raw and "NO_key" in snap.columns:
                    busqueda = pozo_raw.upper().replace(" ", "")
                    mask = snap["NO_key"].str.upper().str.replace(" ", "", regex=False).str.contains(busqueda, regex=False)
                    snap = snap[mask]
                    if snap.empty:
                        return {"error": f"No se encontró '{pozo_raw}' en el snapshot."}
                if bateria and "Bateria" in snap.columns:
                    snap = snap[snap["Bateria"].str.upper() == bateria.upper()]
                if bajo and "Sumergencia" in snap.columns:
                    snap = snap[snap["Sumergencia"].notna() & (snap["Sumergencia"] < 50)]
                cols = [c for c in ["NO_key", "Bateria", "ORIGEN", "DT_plot", "Sumergencia",
                                     "Sumergencia_base", "Bba Llenado", "Caudal bruto efec",
                                     "%Balance", "%Estructura", "Dias_desde_ultima"] if c in snap.columns]
                records = clean_records(snap[cols], 50)
                # Enriquecer sumergencia nula para pozo específico
                if pozo_raw and records:
                    for rec in records:
                        if rec.get("Sumergencia") is None:
                            try:
                                from api.din import _load_indexes_with_keys
                                from core.consolidado import build_pozo_consolidado
                                no_key = rec.get("NO_key", "")
                                din_ok2, niv_ok2, col_map2 = _load_indexes_with_keys()
                                dfp2 = build_pozo_consolidado(
                                    din_ok2, niv_ok2, no_key,
                                    col_map2["din_no_col"], col_map2["din_fe_col"], col_map2["din_ho_col"],
                                    col_map2["niv_no_col"], col_map2["niv_fe_col"], col_map2["niv_ho_col"],
                                )
                                if not dfp2.empty and "Sumergencia" in dfp2.columns:
                                    dfp2["Sumergencia"] = pd.to_numeric(dfp2["Sumergencia"], errors="coerce")
                                    last_rows = dfp2.dropna(subset=["Sumergencia"]).sort_values("DT_plot")
                                    if not last_rows.empty:
                                        rec["Sumergencia"] = round(float(last_rows.iloc[-1]["Sumergencia"]), 1)
                            except Exception:
                                pass
                return {"total_pozos": len(snap), "mostrando": min(50, len(snap)), "pozos": records}
            except Exception as e:
                return {"error": f"get_snapshot_pozos: {e}"}

        elif nombre == "get_historico_sumergencia":
            try:
                from api.din import _load_indexes_with_keys
                from core.consolidado import build_pozo_consolidado
                from core.parsers import safe_to_float
                pozo_raw = args.get("pozo", "").strip()
                no_key, err = buscar_pozo_fuzzy(pozo_raw)
                if err: return {"error": err}
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
                    serie.append({
                        "dt": dt.isoformat() if hasattr(dt, "isoformat") else str(dt),
                        "sumergencia": round(float(row["Sumergencia"]), 1),
                        "pb": safe_to_float(row.get("PB")),
                        "origen": row.get("ORIGEN", ""),
                    })
                return {"pozo": no_key, "total_puntos": len(serie), "serie": serie[-30:],
                        "nota": "Últimos 30 puntos." if len(serie) > 30 else ""}
            except Exception as e:
                return {"error": f"get_historico_sumergencia: {e}"}

        elif nombre == "get_mediciones_din_pozo":
            try:
                from api.din import _load_indexes_with_keys
                from core.consolidado import build_pozo_consolidado
                # resolve_existing_path está en core.gcs (no en core.parsers)
                from core.gcs import resolve_existing_path
                from core.parsers import parse_din_extras, EXTRA_FIELDS
                pozo_raw = args.get("pozo", "").strip()
                no_key, err = buscar_pozo_fuzzy(pozo_raw)
                if err: return {"error": err}
                din_ok, niv_ok, col_map = _load_indexes_with_keys()
                dfp = build_pozo_consolidado(
                    din_ok, niv_ok, no_key,
                    col_map["din_no_col"], col_map["din_fe_col"], col_map["din_ho_col"],
                    col_map["niv_no_col"], col_map["niv_fe_col"], col_map["niv_ho_col"],
                )
                if dfp.empty:
                    return {"error": f"No hay mediciones para '{no_key}'."}
                for col in dfp.select_dtypes(include=["datetime64[ns]"]).columns:
                    dfp[col] = dfp[col].dt.strftime("%Y-%m-%d %H:%M")
                cols_utiles = [c for c in [
                    "ORIGEN", "DT_plot", "Sumergencia", "PB", "NM", "NC", "ND",
                    "Bba Llenado", "Caudal bruto efec", "%Balance", "%Estructura",
                    "AIB Carrera", "Contrapeso actual", "Contrapeso ideal",
                    "Bba Prof", "Bba Diam Pistón", "GPM",
                    "Polea Motor", "Potencia Motor", "RPM Motor",
                ] if c in dfp.columns]
                df_sorted = dfp.sort_values("DT_plot", ascending=False) if "DT_plot" in dfp.columns else dfp
                return {
                    "pozo": no_key, "total": len(dfp), "mostrando": 10,
                    "mediciones": clean_records(df_sorted[cols_utiles], 10),
                }
            except Exception as e:
                return {"error": f"get_mediciones_din_pozo: {e}"}

        elif nombre == "get_historico_niv":
            try:
                from core.gcs import load_niv_index
                from core.parsers import normalize_no_exact, find_col, safe_to_float
                pozo_raw = args.get("pozo", "").strip()
                no_key, err = buscar_pozo_fuzzy(pozo_raw)
                if err: return {"error": err}
                df_niv = load_niv_index()
                if df_niv is None or df_niv.empty:
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
                    pb = safe_to_float(row.get("PB"))
                    nc = safe_to_float(row.get("NC"))
                    registros.append({
                        "fecha": str(row.get(fe_col, "?")) if fe_col else "?",
                        "PB": pb, "NC": nc,
                        "NM": safe_to_float(row.get("NM")),
                        "ND": safe_to_float(row.get("ND")),
                        "sumergencia_m": round(pb - nc, 1) if pb and nc else None,
                    })
                return {"pozo": no_key, "total": len(df_p), "serie": registros}
            except Exception as e:
                return {"error": f"get_historico_niv: {e}"}

        elif nombre == "get_tendencias":
            try:
                from api.din import _load_indexes_with_keys
                from core.consolidado import build_global_consolidated
                import numpy as np
                variable = args.get("variable", "Sumergencia")
                solo_pos = args.get("solo_positiva", True)
                din_ok, niv_ok, col_map = _load_indexes_with_keys()
                # Llamada idéntica al chat.py original (sin niv_ho_col — verificado)
                df_all = build_global_consolidated(
                    din_ok, niv_ok,
                    col_map["din_no_col"], col_map["din_fe_col"], col_map["din_ho_col"],
                    col_map["niv_no_col"], col_map["niv_fe_col"],
                )
                if df_all.empty or variable not in df_all.columns:
                    return {"error": f"Variable '{variable}' no disponible. Opciones: Sumergencia, %Balance, %Estructura, Caudal bruto efec"}
                # Lógica idéntica al chat.py original: usar _mes (inicio de período mensual)
                fe_col = next((c for c in ["din_datetime", "DT_plot"] if c in df_all.columns), None)
                if not fe_col:
                    return {"error": "No hay columna de fecha en el consolidado."}
                df_all["_dt"] = pd.to_datetime(df_all.get("din_datetime") or df_all.get("DT_plot"), errors="coerce")
                df_all["_mes"] = df_all["_dt"].dt.to_period("M").apply(
                    lambda p: p.start_time if pd.notna(p) else pd.NaT
                )
                df_all[variable] = pd.to_numeric(df_all[variable], errors="coerce")
                resultados = []
                for pozo, grp in df_all.groupby("NO_key"):
                    grp_v = grp[["_mes", variable]].dropna()
                    if len(grp_v) < 3: continue
                    grp_v = grp_v.sort_values("_mes")
                    x = (grp_v["_mes"] - grp_v["_mes"].min()).dt.days.values
                    y = grp_v[variable].values
                    if len(x) < 2: continue
                    try:
                        m, _ = np.polyfit(x, y, 1)
                        pendiente_mes = round(float(m) * 30, 2)
                    except Exception:
                        continue
                    if solo_pos and pendiente_mes <= 0: continue
                    resultados.append({"pozo": pozo, "pendiente_por_mes": pendiente_mes,
                                       "valor_inicial": round(float(y[0]), 1),
                                       "valor_final": round(float(y[-1]), 1),
                                       "n_puntos": len(grp_v)})
                resultados.sort(key=lambda r: abs(r["pendiente_por_mes"]), reverse=True)
                return {"variable": variable, "pozos": resultados[:30], "total": len(resultados)}
            except Exception as e:
                return {"error": f"get_tendencias: {e}"}

        elif nombre == "get_stats_campo":
            try:
                from api.mapa import _load_indexes_ok, _build_snap_con_coords
                din_ok, niv_ok, _ = _load_indexes_ok()
                snap = _build_snap_con_coords(din_ok, niv_ok)
                if snap.empty:
                    return {"error": "No hay snapshot disponible."}
                total = len(snap)
                s_num = pd.to_numeric(snap.get("Sumergencia", pd.Series(dtype=float)), errors="coerce").dropna()
                l_num = pd.to_numeric(snap.get("Bba Llenado", pd.Series(dtype=float)), errors="coerce").dropna()
                d_num = pd.to_numeric(snap.get("Dias_desde_ultima", pd.Series(dtype=float)), errors="coerce").dropna()
                return {
                    "total_pozos": total,
                    "sumergencia": {
                        "media": round(float(s_num.mean()), 1), "mediana": round(float(s_num.median()), 1),
                        "min": round(float(s_num.min()), 1), "max": round(float(s_num.max()), 1),
                        "p25": round(float(s_num.quantile(0.25)), 1), "p75": round(float(s_num.quantile(0.75)), 1),
                        "con_sumergencia": int(len(s_num)), "sin_sumergencia": int(total - len(s_num)),
                    } if not s_num.empty else {},
                    "llenado_bomba": {
                        "media": round(float(l_num.mean()), 1),
                        "bajo_50pct": int((l_num < 50).sum()),
                        "entre_50_70": int(((l_num >= 50) & (l_num < 70)).sum()),
                        "ok_70plus": int((l_num >= 70).sum()),
                    } if not l_num.empty else {},
                    "dias_sin_medicion": {
                        "media_dias": round(float(d_num.mean()), 1),
                        "mas_de_90_dias": int((d_num > 90).sum()),
                        "mas_de_30_dias": int((d_num > 30).sum()),
                    } if not d_num.empty else {},
                }
            except Exception as e:
                return {"error": f"get_stats_campo: {e}"}

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
                fe_col = next((c for c in ["din_datetime", "DT_plot"] if c in df_all.columns), None)
                if not fe_col: return {"error": "No hay columna de fecha."}
                df_all["_mes"] = pd.to_datetime(df_all[fe_col], errors="coerce").dt.to_period("M").astype(str)
                serie = df_all.groupby("_mes")["NO_key"].nunique().reset_index()
                serie.columns = ["Mes", "Pozos_medidos"]
                serie = serie.sort_values("Mes")
                ultimo = serie.iloc[-1] if not serie.empty else {}
                return {"ultimo_mes": str(ultimo.get("Mes", "?")),
                        "ultimo_valor": int(ultimo.get("Pozos_medidos", 0)),
                        "serie": serie.to_dict(orient="records")}
            except Exception as e:
                return {"error": f"get_pozos_por_mes: {e}"}

        elif nombre == "get_cobertura_din":
            try:
                from api.din import _load_indexes_with_keys
                from core.consolidado import build_global_consolidated
                from core.gcs import load_coords_repo
                from core.parsers import normalize_no_exact
                fecha_desde = args.get("fecha_desde", "")
                fecha_hasta = args.get("fecha_hasta", "")
                if not fecha_desde or not fecha_hasta:
                    return {"error": "Se requieren fecha_desde y fecha_hasta."}
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
                if not fe_col: return {"error": "No hay columna de fecha."}
                df_all["_dt"] = pd.to_datetime(df_all[fe_col], errors="coerce")
                mask = (df_all["_dt"] >= pd.to_datetime(fecha_desde)) & (df_all["_dt"] <= pd.to_datetime(fecha_hasta))
                con_din = set(df_all[mask]["NO_key"].dropna().apply(normalize_no_exact).unique())
                sin_din = sorted(todos_pozos - con_din) if todos_pozos else []
                return {"periodo": f"{fecha_desde} → {fecha_hasta}", "total_pozos": len(todos_pozos),
                        "con_medicion": len(con_din), "sin_medicion": len(sin_din),
                        "lista_sin_medicion": sin_din[:30]}
            except Exception as e:
                return {"error": f"get_cobertura_din: {e}"}

        elif nombre == "get_lista_baterias":
            try:
                from core.gcs import load_snapshot
                snap = load_snapshot()
                if snap is None or snap.empty:
                    return {"error": "No hay snapshot disponible."}
                bat_col = next((c for c in ["Bateria", "nivel_5", "Batería"] if c in snap.columns), None)
                if not bat_col: return {"error": "No se encontró columna de batería."}
                counts = snap[bat_col].dropna().value_counts().reset_index()
                counts.columns = ["bateria", "pozos"]
                return {"total_baterias": len(counts),
                        "baterias": counts.sort_values("bateria").to_dict(orient="records")}
            except Exception as e:
                return {"error": f"get_lista_baterias: {e}"}

        elif nombre == "get_info_sistema":
            try:
                from core.gcs import get_gcs_client, GCS_BUCKET, GCS_PREFIX, load_din_index, load_niv_index
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
                return {"gcs_ok": gcs_ok, "openai_ok": openai_ok,
                        "din_registros_indexados": din_count,
                        "niv_registros_indexados": niv_count,
                        "gcs_bucket": GCS_BUCKET or "no configurado",
                        "gcs_prefix": GCS_PREFIX or "(raíz)"}
            except Exception as e:
                return {"error": f"get_info_sistema: {e}"}

        return {"error": f"Tool desconocida en AgentePozos: {nombre}"}
