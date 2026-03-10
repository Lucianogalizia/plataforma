# ==========================================================
# backend/api/chat.py  — v2 (auditado)
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

TOOLS = [
    {"type":"function","function":{"name":"get_kpis_diagnosticos","description":"KPIs globales de diagnósticos IA: total pozos, críticos, alta severidad, sin problemáticas.","parameters":{"type":"object","properties":{},"required":[]}}},
    {"type":"function","function":{"name":"get_pozos_criticos","description":"Lista pozos CRÍTICA/ALTA con problemáticas activas, batería, llenado, sumergencia y recomendación. Usar para '¿qué pozos son críticos?'.","parameters":{"type":"object","properties":{"severidad":{"type":"string","enum":["CRÍTICA","ALTA","MEDIA","BAJA"],"description":"Nivel de severidad. Sin valor devuelve CRÍTICA y ALTA."}},"required":[]}}},
    {"type":"function","function":{"name":"get_diagnostico_pozo","description":"Diagnóstico completo de un pozo: problemáticas, severidades, sumergencia, llenado, caudal, balance y recomendación.","parameters":{"type":"object","properties":{"pozo":{"type":"string","description":"Nombre del pozo. Ej: Pe-123, ai-45."}},"required":["pozo"]}}},
    {"type":"function","function":{"name":"buscar_por_problematica","description":"Busca pozos con una problemática activa específica. Usar para: golpeo de fondo, gas en bomba, llenado bajo, sumergencia crítica.","parameters":{"type":"object","properties":{"problema":{"type":"string","description":"Fragmento del nombre de la problemática."}},"required":["problema"]}}},
    {"type":"function","function":{"name":"get_snapshot_pozos","description":"Estado actual de todos los pozos: sumergencia, llenado, caudal, balance, días desde última medición. Para preguntas generales del campo.","parameters":{"type":"object","properties":{"bateria":{"type":"string","description":"Filtrar por batería (opcional)."},"con_sumergencia_baja":{"type":"boolean","description":"Si true, solo pozos con sumergencia < 50m."}},"required":[]}}},
    {"type":"function","function":{"name":"get_kpis_acciones","description":"KPIs de acciones de optimización: total, en proceso, finalizadas.","parameters":{"type":"object","properties":{},"required":[]}}},
    {"type":"function","function":{"name":"get_acciones","description":"Lista acciones de optimización. Filtrable por estado, batería o pozo.","parameters":{"type":"object","properties":{"nombre_pozo":{"type":"string","description":"Nombre del pozo."},"bateria":{"type":"string","description":"Nombre de batería."},"estado":{"type":"string","description":"Estado de la acción."}},"required":[]}}},
    {"type":"function","function":{"name":"get_controles_merma","description":"Pozos con merma de producción: % pérdida neta y bruta, último control, días sin control.","parameters":{"type":"object","properties":{"solo_en_merma":{"type":"boolean","description":"Si true, solo pozos con merma activa."},"bateria":{"type":"string","description":"Filtrar por batería."}},"required":[]}}},
    {"type":"function","function":{"name":"get_controles_historico","description":"Controles de producción históricos: petróleo, gas y líquido por pozo y fecha.","parameters":{"type":"object","properties":{"pozo":{"type":"string","description":"Nombre del pozo."},"bateria":{"type":"string","description":"Nombre de batería."},"fecha_desde":{"type":"string","description":"Fecha desde YYYY-MM-DD."},"fecha_hasta":{"type":"string","description":"Fecha hasta YYYY-MM-DD."}},"required":[]}}},
]


def _ejecutar_tool(nombre: str, args: dict) -> Any:

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
            if df.empty:
                return {"pozos": [], "total": 0}
            df_fil = df[df["Sev. máx"].isin(sevs)].sort_values(["Sev. máx", "Batería", "Pozo"])
            return {"pozos": [{"pozo": r["Pozo"], "bateria": r["Batería"], "severidad": r["Sev. máx"], "fecha_din": r["Fecha DIN"], "llenado": r["Llenado %"], "sumergencia": r["Sumergencia"], "problematicas": r["Problemáticas"], "recomendacion": r["Recomendación"]} for _, r in df_fil.iterrows()], "total": len(df_fil)}
        except Exception as e:
            return {"error": f"get_pozos_criticos: {e}"}

    elif nombre == "get_diagnostico_pozo":
        try:
            from core.gcs import load_diag_from_gcs
            from core.parsers import normalize_no_exact
            pozo_raw = args.get("pozo", "").strip()
            if not pozo_raw:
                return {"error": "Parámetro 'pozo' requerido."}
            diag = load_diag_from_gcs(normalize_no_exact(pozo_raw))
            if not diag:
                return {"error": f"No se encontró diagnóstico para '{pozo_raw}'."}
            if "error" in diag:
                return {"error": diag["error"]}
            meta = diag.get("_meta", {})
            return {
                "pozo": normalize_no_exact(pozo_raw),
                "recomendacion": diag.get("recomendacion", ""),
                "confianza": diag.get("confianza", "N/D"),
                "generado_utc": meta.get("generado_utc", "?")[:19],
                "mediciones": [{"fecha": m.get("fecha"), "llenado_pct": m.get("llenado_pct"), "sumergencia_m": m.get("sumergencia_m"), "caudal_bruto": m.get("caudal_bruto"), "pct_balance": m.get("pct_balance"), "problematicas": [{"nombre": p.get("nombre"), "severidad": p.get("severidad"), "estado": p.get("estado"), "detalle": p.get("detalle", "")} for p in m.get("problemáticas", [])]} for m in diag.get("mediciones", [])],
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
            cols = [c for c in ["NO_key", "Bateria", "ORIGEN", "DT_plot", "Sumergencia", "Sumergencia_base", "Bba Llenado", "Caudal bruto efec", "%Balance", "%Estructura", "Dias_desde_ultima"] if c in snap.columns]
            snap_fil = snap[cols].where(snap[cols].notna(), other=None)
            total = len(snap_fil)
            return {"total_pozos": total, "mostrando": min(50, total), "nota": "Primeros 50 resultados." if total > 50 else "", "pozos": snap_fil.head(50).to_dict(orient="records")}
        except Exception as e:
            return {"error": f"get_snapshot_pozos: {e}"}

    elif nombre == "get_kpis_acciones":
        try:
            from core.acciones import get_kpis_acciones
            return get_kpis_acciones()
        except Exception as e:
            return {"error": f"get_kpis_acciones: {e}"}

    elif nombre == "get_acciones":
        try:
            from core.acciones import get_acciones_filtradas
            acciones = get_acciones_filtradas(nombre_pozo=args.get("nombre_pozo"), bateria=args.get("bateria"), estado=args.get("estado"))
            return {"acciones": acciones[:30], "total": len(acciones), "nota": "Máximo 30 resultados." if len(acciones) > 30 else ""}
        except Exception as e:
            return {"error": f"get_acciones: {e}"}

    elif nombre == "get_controles_merma":
        try:
            from core.gcs import get_gcs_client, GCS_BUCKET, GCS_PREFIX
            client = get_gcs_client()
            if not client or not GCS_BUCKET:
                return {"error": "GCS no configurado."}
            blob_name = f"{GCS_PREFIX}/controles/merma_por_pozo.csv" if GCS_PREFIX else "controles/merma_por_pozo.csv"
            blob = client.bucket(GCS_BUCKET).blob(blob_name)
            if not blob.exists():
                return {"error": "Archivo de merma no encontrado. Ejecutá fetch_controles primero."}
            df = pd.read_csv(io.BytesIO(blob.download_as_bytes()), low_memory=False)
            if args.get("solo_en_merma") and "EN_MERMA_NETA" in df.columns:
                df = df[df["EN_MERMA_NETA"] == True]
            bateria = args.get("bateria", "").strip()
            if bateria and "BATERIA" in df.columns:
                df = df[df["BATERIA"].str.upper() == bateria.upper()]
            df = df.where(df.notna(), other=None)
            records = [{k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in r.items()} for r in df.head(50).to_dict(orient="records")]
            return {"total": len(df), "mostrando": len(records), "pozos": records}
        except Exception as e:
            return {"error": f"get_controles_merma: {e}"}

    elif nombre == "get_controles_historico":
        try:
            from core.gcs import get_gcs_client, GCS_BUCKET, GCS_PREFIX
            client = get_gcs_client()
            if not client or not GCS_BUCKET:
                return {"error": "GCS no configurado."}
            blob_name = f"{GCS_PREFIX}/controles/historico_CRUDO.csv" if GCS_PREFIX else "controles/historico_CRUDO.csv"
            blob = client.bucket(GCS_BUCKET).blob(blob_name)
            if not blob.exists():
                return {"error": "Archivo histórico no encontrado. Ejecutá fetch_controles primero."}
            df = pd.read_csv(io.BytesIO(blob.download_as_bytes()), low_memory=False)
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
            df = df.where(df.notna(), other=None)
            records = [{k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in r.items()} for r in df.head(100).to_dict(orient="records")]
            return {"total": len(df), "mostrando": len(records), "registros": records}
        except Exception as e:
            return {"error": f"get_controles_historico: {e}"}

    return {"error": f"Tool desconocida: {nombre}"}


SYSTEM_PROMPT = """Sos el asistente técnico de la Plataforma DINA,
sistema de análisis dinamométrico de pozos petroleros.

REGLAS:
1. Usás las tools disponibles para obtener datos reales antes de responder.
2. Si una tool devuelve {"error": "..."}, informás el error textualmente.
3. NUNCA inventás valores, pozos, fechas ni problemáticas.
4. Respondés en español, de forma concisa y técnica.
5. Para pozos específicos → get_diagnostico_pozo.
6. Para estado general del campo → get_snapshot_pozos o get_kpis_diagnosticos.
7. Para producción y pérdidas → get_controles_merma o get_controles_historico.
8. Interpretás los datos con contexto técnico petrolero:
   pct_merma_neta = caída % de producción neta,
   dias_sin_control = días sin medición de producción,
   sumergencia = metros de fluido sobre la bomba,
   llenado = % de llenado de la cámara de la bomba."""


@router.get("/debug")
async def chat_debug():
    """Verifica acceso a datos de todas las fuentes."""
    resultados = {}
    for tool, args in [
        ("get_kpis_diagnosticos", {}),
        ("get_pozos_criticos",    {}),
        ("get_snapshot_pozos",    {}),
        ("get_kpis_acciones",     {}),
        ("get_controles_merma",   {"solo_en_merma": True}),
    ]:
        r = _ejecutar_tool(tool, args)
        if isinstance(r, dict):
            if "pozos" in r:    r["pozos"]    = r["pozos"][:2]
            if "registros" in r: r["registros"] = r["registros"][:2]
        resultados[tool] = r
    return resultados


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
    response = client.chat.completions.create(model="gpt-4o-mini", messages=messages, tools=TOOLS, tool_choice="auto", max_tokens=1500, temperature=0)
    msg_resp = response.choices[0].message

    if msg_resp.tool_calls:
        messages.append(msg_resp)
        for tc in msg_resp.tool_calls:
            nombre_tool = tc.function.name
            tools_usadas.append(nombre_tool)
            try:
                args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                args = {}
            resultado = _ejecutar_tool(nombre_tool, args)
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": json.dumps(resultado, ensure_ascii=False, default=str)})
        response2 = client.chat.completions.create(model="gpt-4o-mini", messages=messages, max_tokens=1500, temperature=0)
        respuesta_final = response2.choices[0].message.content or ""
    else:
        respuesta_final = msg_resp.content or ""

    return ChatResponse(respuesta=respuesta_final.strip(), tools_usadas=tools_usadas)
