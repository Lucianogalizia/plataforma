# ==========================================================
# backend/ia/diagnostico.py
#
# Lógica completa de diagnóstico IA extraída de
# diagnostico_tab.py
#
# Incluye:
#   - Obtención de API key de OpenAI (GCP Secret Manager o env)
#   - Llamada a OpenAI (gpt-5.2-chat-latest)
#   - Generación de diagnóstico para un pozo
#   - Verificación de necesidad de regeneración
#   - Generación en lote con progress tracking
#   - Construcción de tabla global (una fila por medición)
#   - Normalización de estados en respuesta IA
# ==========================================================

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from core.gcs import (
    GCS_BUCKET,
    GCS_PREFIX,
    gcs_download_to_temp,
    is_gs_path,
    load_diag_from_gcs,
    save_diag_to_gcs,
    load_all_diags_from_gcs,
)
from core.parsers import (
    parse_din_full,
    extract_variables_from_parsed,
    describe_cs_shape,
    safe_to_float,
    normalize_no_exact,
)
from ia.prompts import (
    DIAG_SCHEMA_VERSION,
    SEVERIDAD_ORDEN,
    SEVERIDAD_EMOJI,
    ESTADO_EMOJI,
    build_prompt,
)


# ==========================================================
# API Key de OpenAI
# ==========================================================

def get_openai_key() -> str | None:
    """
    Obtiene la API key de OpenAI en orden de prioridad:
        1. GCP Secret Manager (producción en Cloud Run)
        2. Variable de entorno OPENAI_API_KEY (desarrollo local)

    Returns:
        API key como string, o None si no se encontró.
    """
    import os

    # 1. GCP Secret Manager
    try:
        from google.cloud import secretmanager
        project_id  = (
            os.environ.get("GOOGLE_CLOUD_PROJECT")
            or os.environ.get("GCLOUD_PROJECT")
        )
        secret_name = os.environ.get("OPENAI_SECRET_NAME", "OPENAI_API_KEY")
        if project_id:
            client   = secretmanager.SecretManagerServiceClient()
            name     = (
                f"projects/{project_id}/secrets/{secret_name}/versions/latest"
            )
            response = client.access_secret_version(request={"name": name})
            key      = response.payload.data.decode("UTF-8").strip()
            if key:
                return key
    except Exception:
        pass

    # 2. Variable de entorno
    return os.environ.get("OPENAI_API_KEY", "").strip() or None


# ==========================================================
# Llamada a OpenAI
# ==========================================================

def call_openai(prompt: str, api_key: str) -> dict:
    """
    Envía el prompt a OpenAI y parsea la respuesta JSON.

    Modelo: gpt-5.2-chat-latest
    max_completion_tokens: 2500

    Args:
        prompt:  string completo del prompt
        api_key: API key de OpenAI

    Returns:
        dict con el diagnóstico parseado desde el JSON de respuesta.

    Raises:
        json.JSONDecodeError: si la respuesta no es JSON válido
        Exception:            cualquier error de la API
    """
    from openai import OpenAI

    client = OpenAI(api_key=api_key)

    response = client.chat.completions.create(
        model="gpt-5.2-chat-latest",
        messages=[{"role": "user", "content": prompt}],
        max_completion_tokens=2500,
    )

    raw = response.choices[0].message.content.strip()

    # Limpiar fences de markdown si el modelo las agrega
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.lower().startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    if raw.endswith("```"):
        raw = raw[:-3].strip()

    return json.loads(raw)


# ==========================================================
# Generación de diagnóstico para un pozo
# ==========================================================

def generar_diagnostico(
    no_key:          str,
    din_ok:          pd.DataFrame,
    resolve_path_fn,
    api_key:          str,
    niv_ok:          pd.DataFrame | None = None,
) -> dict:
    """
    Genera el diagnóstico IA completo para un pozo.
    """
    # --- Filtrar DINs del pozo ---
    # SEGURIDAD: Validamos la existencia de la columna antes de filtrar
    if din_ok is None or "NO_key" not in din_ok.columns:
        return {"error": "Estructura de datos inválida: columna 'NO_key' no encontrada en el índice DIN."}

    din_p = din_ok[din_ok["NO_key"] == no_key].copy()
    if din_p.empty or "path" not in din_p.columns:
        return {"error": "Sin archivos DIN disponibles para este pozo."}

    sort_cols = [c for c in ["din_datetime", "mtime"] if c in din_p.columns]
    if sort_cols:
        din_p = din_p.sort_values(sort_cols, na_position="last")

    din_p = din_p.dropna(subset=["path"]).drop_duplicates(subset=["path"]).tail(3)

    # --- Parsear cada DIN ---
    mediciones = []

    for _, row in din_p.iterrows():
        path_str = row.get("path")
        if not path_str:
            continue

        p_res = resolve_path_fn(str(path_str))
        if not p_res:
            continue

        local_path = p_res
        if is_gs_path(str(p_res)):
            try:
                local_path = gcs_download_to_temp(p_res)
            except Exception:
                continue

        try:
            parsed = parse_din_full(local_path)
        except Exception:
            continue

        vars_    = extract_variables_from_parsed(parsed)
        cs_shape = describe_cs_shape(parsed.get("cs_points", []))

        fecha = (
            row.get("din_datetime")
            or row.get("mtime")
            or vars_.get("FE")
            or "Desconocida"
        )
        if hasattr(fecha, "strftime"):
            fecha = fecha.strftime("%Y-%m-%d %H:%M")

        # --- Completar sumergencia desde NIV si falta ---
        if vars_.get("Sumergencia_m") is None and niv_ok is not None and not niv_ok.empty:
            if "NO_key" in niv_ok.columns:
                niv_p = niv_ok[niv_ok["NO_key"] == no_key].copy()
                if not niv_p.empty:
                    try:
                        fecha_din_dt = pd.to_datetime(str(fecha), errors="coerce")
                        sort_niv = [c for c in ["niv_datetime", "mtime"] if c in niv_p.columns]

                        if sort_niv and not pd.isna(fecha_din_dt):
                            niv_p["_dt"] = pd.to_datetime(
                                niv_p[sort_niv[0]], errors="coerce"
                            )
                            niv_p = niv_p.dropna(subset=["_dt"])

                            if not niv_p.empty:
                                niv_p["_diff"] = (niv_p["_dt"] - fecha_din_dt).abs()
                                niv_p = niv_p.sort_values("_diff")
                                mejor_niv = niv_p.iloc[0]
                                diff_dias = (
                                    mejor_niv["_diff"].days
                                    if hasattr(mejor_niv["_diff"], "days")
                                    else 999
                                )

                                if diff_dias <= 90:
                                    def sf(v):
                                        try:
                                            return float(str(v).replace(",", "."))
                                        except Exception:
                                            return None

                                    pb_niv = sf(mejor_niv.get("PB"))
                                    nc_niv = sf(mejor_niv.get("NC"))
                                    nm_niv = sf(mejor_niv.get("NM"))
                                    nd_niv = sf(mejor_niv.get("ND"))

                                    pb = vars_.get("Prof_bomba_m") or pb_niv
                                    if pb is not None:
                                        for nivel_val, nivel_nom in [
                                            (nc_niv, "NC"),
                                            (nm_niv, "NM"),
                                            (nd_niv, "ND"),
                                        ]:
                                            if nivel_val is not None:
                                                vars_["Sumergencia_m"]    = round(pb - nivel_val, 1)
                                                vars_["Base_sumergencia"] = nivel_nom
                                                vars_["Prof_bomba_m"]     = (
                                                    vars_.get("Prof_bomba_m") or pb
                                                )
                                                if vars_.get("NC_m") is None:
                                                    vars_["NC_m"] = nc_niv
                                                if vars_.get("NM_m") is None:
                                                    vars_["NM_m"] = nm_niv
                                                if vars_.get("ND_m") is None:
                                                    vars_["ND_m"] = nd_niv
                                                if vars_.get("PB_m") is None:
                                                    vars_["PB_m"] = pb_niv
                                                break
                    except Exception:
                        pass

        mediciones.append({
            "fecha":    str(fecha),
            "path":     str(p_res),
            "vars":     vars_,
            "cs_shape": cs_shape,
        })

    if not mediciones:
        return {"error": "No se pudieron parsear archivos DIN para este pozo."}

    # --- Llamar a OpenAI ---
    prompt = build_prompt(no_key, mediciones)
    try:
        diag = call_openai(prompt, api_key)
    except Exception as e:
        return {"error": f"Error llamando a OpenAI: {e}"}

    # --- Normalizar estados ---
    for med in diag.get("mediciones", []):
        for p in med.get("problemáticas", []):
            estado  = str(p.get("estado", "")).strip().upper()
            p["estado"] = "RESUELTA" if estado == "RESUELTA" else "ACTIVA"

    # --- Metadata ---
    diag["_meta"] = {
        "generado_utc":           datetime.now(timezone.utc).isoformat(),
        "paths_analizados":       [m["path"] for m in mediciones],
        "fecha_din_mas_reciente": mediciones[-1]["fecha"] if mediciones else None,
        "n_mediciones":           len(mediciones),
        "schema_version":         DIAG_SCHEMA_VERSION,
    }

    # --- Guardar en GCS ---
    if GCS_BUCKET:
        save_diag_to_gcs(no_key, diag)

    return diag

# ==========================================================
# Verificación de necesidad de regeneración
# ==========================================================

def necesita_regenerar(
    diag:   dict | None,
    din_ok: pd.DataFrame,
    no_key: str,
) -> bool:
    """
    Determina si el diagnóstico cacheado necesita regenerarse.
    """
    # Si no hay diagnóstico o tiene error, hay que generarlo
    if not diag or "error" in diag:
        return True

    meta = diag.get("_meta", {})
    
    # Versión de esquema desactualizada
    if meta.get("schema_version", 0) < DIAG_SCHEMA_VERSION:
        return True

    # Fecha de generación inválida
    fecha_diag_str = meta.get("generado_utc")
    if not fecha_diag_str:
        return True

    try:
        fecha_diag = pd.to_datetime(fecha_diag_str, utc=True)
    except Exception:
        return True

    # --- SEGURO LÍNEA 171 (KeyError: 'NO_key') ---
    if din_ok is None or "NO_key" not in din_ok.columns:
        return True # Si no podemos validar, mejor regenerar

    # Comparar con el DIN más reciente
    din_p = din_ok[din_ok["NO_key"] == no_key].copy()
    if din_p.empty:
        return False

    sort_cols = [c for c in ["din_datetime", "mtime"] if c in din_p.columns]
    if not sort_cols:
        return False

    try:
        latest_din = pd.to_datetime(
            din_p[sort_cols[0]], errors="coerce", utc=True
        ).max()
        if pd.isna(latest_din):
            return False
        return latest_din > fecha_diag
    except:
        return True


# ==========================================================
# Generación en lote
# ==========================================================

def generar_todos(
    pozos:           list[str],
    din_ok:          pd.DataFrame,
    resolve_path_fn,
    api_key:         str,
    solo_pendientes: bool = True,
    niv_ok:          pd.DataFrame | None = None,
    progress_cb=None,
) -> dict:
    """
    Genera diagnósticos para todos los pozos de la lista.

    Args:
        pozos:           lista de NO_key a procesar
        din_ok:          DataFrame de índice DIN
        resolve_path_fn: función resolve_existing_path
        api_key:         API key de OpenAI
        solo_pendientes: si True, saltea los pozos ya actualizados
        niv_ok:          DataFrame de índice NIV (opcional)
        progress_cb:     callback opcional para reportar progreso.
                         Firma: progress_cb(idx, total, no_key, resultado)
                         donde resultado es "ok", "error" o "salteado"

    Returns:
        {
            "ok":        list[str],           pozos generados correctamente
            "error":     list[(str, str)],    (no_key, mensaje_error)
            "salteados": list[str],           pozos con caché válido
        }
    """
    resumen = {"ok": [], "error": [], "salteados": []}

    # --- Determinar qué procesar ---
    pozos_a_procesar = []
    for no_key in pozos:
        if solo_pendientes:
            cache = load_diag_from_gcs(no_key) if GCS_BUCKET else None
            if not necesita_regenerar(cache, din_ok, no_key):
                resumen["salteados"].append(no_key)
                if progress_cb:
                    progress_cb(
                        len(resumen["salteados"]) - 1,
                        len(pozos),
                        no_key,
                        "salteado",
                    )
                continue
        pozos_a_procesar.append(no_key)

    total    = len(pozos_a_procesar)
    t_inicio = time.time()

    for idx, no_key in enumerate(pozos_a_procesar):
        try:
            diag = generar_diagnostico(
                no_key=no_key,
                din_ok=din_ok,
                resolve_path_fn=resolve_path_fn,
                api_key=api_key,
                niv_ok=niv_ok,
            )

            if "error" in diag:
                resumen["error"].append((no_key, diag["error"]))
                resultado = "error"
            else:
                resumen["ok"].append(no_key)
                resultado = "ok"

        except Exception as e:
            resumen["error"].append((no_key, str(e)))
            resultado = "error"

        if progress_cb:
            elapsed   = time.time() - t_inicio
            velocidad = elapsed / (idx + 0.001)
            restantes = total - idx - 1
            eta_seg   = int(velocidad * restantes)
            progress_cb(idx, total, no_key, resultado, eta_seg)

    return resumen


# ==========================================================
# Construcción de tabla global (una fila por medición)
# ==========================================================

# ==========================================================
# Construcción de tabla global (VERSIÓN FINAL PROTEGIDA)
# ==========================================================

def build_global_table(
    diags:          dict[str, dict],
    bat_map:         dict[str, str],
    normalize_no_fn,
) -> pd.DataFrame:
    """
    Construye la tabla global. 
    Asegura compatibilidad entre nombres reales de GCS y nombres normalizados de la tabla.
    """
    rows = []

    for real_no_key, diag in diags.items():
        try:
            if not isinstance(diag, dict):
                continue
            
            # --- CLAVE: Intentamos normalizar el nombre para buscarlo en el bat_map ---
            try:
                norm_key = normalize_no_fn(str(real_no_key))
            except:
                norm_key = str(real_no_key)

            bateria       = bat_map.get(norm_key, "N/D")
            meta          = diag.get("_meta", {})
            fecha_gen     = str(meta.get("generado_utc", "?"))[:19].replace("T", " ")
            confianza     = diag.get("confianza", "?")
            recomendacion = diag.get("recomendacion", "")

            mediciones_list = diag.get("mediciones", [])

            if not mediciones_list:
                probs_viejas    = diag.get("problematicas", [])
                mediciones_list = [{
                    "fecha":             meta.get("fecha_din_mas_reciente", "?"),
                    "label":             "Única medición",
                    "llenado_pct":       None,
                    "sumergencia_m":     None,
                    "sumergencia_nivel": "N/D",
                    "caudal_bruto":      None,
                    "pct_balance":       None,
                    "problemáticas":     probs_viejas,
                }]

            for med in mediciones_list:
                fecha     = med.get("fecha",             "?")
                label     = med.get("label",             "")
                llenado   = med.get("llenado_pct")
                sumer     = med.get("sumergencia_m")
                sumer_niv = med.get("sumergencia_nivel", "N/D")
                caudal    = med.get("caudal_bruto")
                balance   = med.get("pct_balance")
                probs     = med.get("problemáticas",     [])

                try:
                    probs_sorted = sorted(
                        probs,
                        key=lambda x: (
                            0 if x.get("estado") == "ACTIVA" else 1,
                            SEVERIDAD_ORDEN.get(x.get("severidad", "BAJA"), 9),
                        ),
                    )
                except:
                    probs_sorted = probs

                if probs_sorted:
                    lineas = []
                    for p in probs_sorted:
                        sev     = p.get("severidad", "BAJA")
                        estado  = p.get("estado",    "ACTIVA")
                        emoji_s = SEVERIDAD_EMOJI.get(sev,    "⚪")
                        emoji_e = ESTADO_EMOJI.get(estado,    "")
                        lineas.append(f"{emoji_e}{emoji_s} {p.get('nombre','?')} [{sev}]")
                    prob_texto = "\n".join(lineas)
                    prob_lista = [p.get("nombre", "?") for p in probs_sorted]
                    activas = [p for p in probs_sorted if p.get("estado") == "ACTIVA"]
                    
                    try:
                        sev_max = min(activas, key=lambda x: SEVERIDAD_ORDEN.get(x.get("severidad", "BAJA"), 9)).get("severidad", "BAJA") if activas else "RESUELTA"
                    except:
                        sev_max = "BAJA"

                    n_activas   = len(activas)
                    n_resueltas = len(probs_sorted) - n_activas
                else:
                    prob_texto, prob_lista, sev_max, n_activas, n_resueltas = "✅ Sin problemáticas", [], "NINGUNA", 0, 0

                rows.append({
                    "Pozo":          str(real_no_key), # Mostramos el nombre real para que coincida con la carpeta
                    "Batería":        str(bateria),
                    "Fecha DIN":     str(fecha),
                    "Medición":      str(label),
                    "Llenado %":     f"{llenado}%" if llenado is not None else "N/D",
                    "Sumergencia":   f"{sumer} m ({sumer_niv})" if sumer is not None else "N/D",
                    "Caudal m³/d":   caudal if caudal is not None else "N/D",
                    "%Balance":      f"{balance}%" if balance is not None else "N/D",
                    "Sev. máx":      str(sev_max),
                    "Act.":          int(n_activas),
                    "Res.":          int(n_resueltas),
                    "Problemáticas": str(prob_texto),
                    "_prob_lista":   prob_lista,
                    "Recomendación": str(recomendacion),
                    "Confianza":     str(confianza),
                    "Generado":      str(fecha_gen),
                })
        except Exception as e:
            print(f"Error procesando pozo {real_no_key}: {e}")
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # LIMPIEZA CRÍTICA: Convertir NaNs a None para evitar Error 500 en la respuesta JSON
    df = df.replace([float('inf'), float('-inf')], None)
    df = df.where(pd.notnull(df), None)

    sev_ord_ext = {"CRÍTICA": 0, "ALTA": 1, "MEDIA": 2, "BAJA": 3, "RESUELTA": 4, "NINGUNA": 5}
    if "Sev. máx" in df.columns:
        df["_sev_ord"] = df["Sev. máx"].map(sev_ord_ext).fillna(9)
        sort_cols = [c for c in ["_sev_ord", "Batería", "Pozo", "Fecha DIN"] if c in df.columns]
        df = df.sort_values(sort_cols).drop(columns=["_sev_ord"], errors="ignore")

    return df.reset_index(drop=True)


# ==========================================================
# ==========================================================
# KPIs de la tabla global (VERSIÓN PROTEGIDA)
# ==========================================================

def get_kpis_global_table(df: pd.DataFrame) -> dict:
    """
    Calcula los KPIs de la tabla global de diagnósticos.
    Blindado contra errores de columnas faltantes o datos corruptos.
    """
    # 1. Caso base: DataFrame nulo o vacío
    if df is None or df.empty:
        return {
            "pozos_diagnosticados": 0,
            "mediciones_totales":   0,
            "criticos":             0,
            "alta_severidad":       0,
            "sin_problematicas":    0,
        }

    # 2. Función interna para contar de forma segura
    # Evita que el programa explote si no encuentra la columna "Pozo" o "Sev. máx"
    def safe_count(condition):
        try:
            if "Pozo" in df.columns and "Sev. máx" in df.columns:
                return int(df[condition]["Pozo"].nunique())
            return 0
        except Exception:
            return 0

    # 3. Retorno con validaciones de existencia de columnas
    return {
        "pozos_diagnosticados": int(df["Pozo"].nunique()) if "Pozo" in df.columns else 0,
        "mediciones_totales":   len(df),
        "criticos":             safe_count(df["Sev. máx"] == "CRÍTICA"),
        "alta_severidad":       safe_count(df["Sev. máx"] == "ALTA"),
        "sin_problematicas":    safe_count(df["Sev. máx"] == "NINGUNA"),
    }


# ==========================================================
# Bat map (mapa pozo → batería)
# ==========================================================

def build_bat_map(coords_df: pd.DataFrame, normalize_no_fn) -> dict[str, str]:
    """
    Construye el mapa { no_key_normalizado: bateria } desde el repo.
    Blindado contra errores de normalización.
    """
    bat_map: dict[str, str] = {}

    if coords_df is None or coords_df.empty:
        return bat_map

    if "nombre_corto" not in coords_df.columns or "nivel_5" not in coords_df.columns:
        return bat_map

    for _, row in coords_df.iterrows():
        try:
            # Intentamos normalizar; si falla (por caracteres raros), usamos el nombre original
            nombre_raw = str(row["nombre_corto"])
            try:
                k = normalize_no_fn(nombre_raw)
            except:
                k = nombre_raw
            bat_map[k] = str(row["nivel_5"])
        except:
            continue

    return bat_map


# ==========================================================
# Estado de caché de diagnósticos
# ==========================================================

def get_estado_cache(
    pozos:  list[str],
    din_ok: pd.DataFrame,
) -> dict:
    """
    Analiza el estado del caché de diagnósticos en GCS.
    Blindado para que el endpoint /estado-cache no devuelva 500.
    """
    if not GCS_BUCKET:
        return {
            "total":      len(pozos),
            "listos":     0,
            "pendientes": len(pozos),
            "diags":      {},
        }

    try:
        # Cargamos todos los diagnósticos
        diags_cache = load_all_diags_from_gcs(pozos)
        
        # Calculamos pendientes con un try-except por cada pozo
        pendientes = 0
        for pk in pozos:
            try:
                if necesita_regenerar(diags_cache.get(pk), din_ok, pk):
                    pendientes += 1
            except:
                # Si un pozo falla en la validación, lo contamos como pendiente
                pendientes += 1

        return {
            "total":      len(pozos),
            "listos":     len(diags_cache),
            "pendientes": pendientes,
            "diags":      diags_cache,
        }
    except Exception as e:
        # Si todo falla, devolvemos un estado vacío en lugar de un Error 500
        print(f"Error crítico en get_estado_cache: {e}")
        return {
            "total":      len(pozos),
            "listos":     0,
            "pendientes": len(pozos),
            "diags":      {},
            "error_log":  str(e)
        }
