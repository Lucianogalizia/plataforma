# ==========================================================
# backend/core/parsers.py
#
# Todo el parseo de archivos .din y .niv extraído de:
#   - app.py
#   - diagnostico_tab.py
#
# Incluye:
#   - Lectura de texto con encoding fallback
#   - Parseo de secciones [SECCION] y pares clave=valor
#   - Extracción de puntos CS (carta de superficie)
#   - Extracción de campos extra (AIB, BOMBA, MOTOR, etc.)
#   - Parseo completo del .din (para diagnóstico IA)
#   - Extracción de variables para el prompt IA
#   - Descripción geométrica de la carta (shape analysis)
#   - Normalización de columnas y helpers de búsqueda
# ==========================================================

import re
from pathlib import Path

import pandas as pd

from core.gcs import gcs_download_to_temp, is_gs_path

# ---------- Regex globales ----------
SECTION_RE   = re.compile(r"^\s*\[(.+?)\]\s*$")
KV_RE        = re.compile(r"^\s*([^=]+?)\s*=\s*(.*?)\s*$")
POINT_KEY_RE = re.compile(r"^(X|Y)\s*(\d+)$", re.IGNORECASE)

# ---------- Campos extra a extraer de cada .din ----------
# Formato: "Nombre columna": ("SECCION", "CLAVE")
EXTRA_FIELDS = {
    "Tipo AIB":                    ("AIB",        "MA"),
    "AIB Carrera":                 ("AIB",        "CS"),
    "Sentido giro":                ("AIB",        "SG"),
    "Tipo Contrapesos":            ("CONTRAPESO",  "TP"),
    "Distancia contrapesos (cm)":  ("CONTRAPESO",  "DE"),
    "Contrapeso actual":           ("RARE",        "CA"),
    "Contrapeso ideal":            ("RARE",        "CM"),
    "AIBEB_Torque max contrapeso": ("RAEB",        "TM"),
    "%Estructura":                 ("RARE",        "SE"),
    "%Balance":                    ("RARR",        "PC"),
    "Bba Diam Pistón":             ("BOMBA",       "DP"),
    "Bba Prof":                    ("BOMBA",       "PB"),
    "Bba Llenado":                 ("BOMBA",       "CA"),
    "GPM":                         ("AIB",         "GM"),
    "Caudal bruto efec":           ("RBO",         "CF"),
    "Polea Motor":                 ("MOTOR",       "DP"),
    "Potencia Motor":              ("MOTOR",       "PN"),
    "RPM Motor":                   ("MOTOR",       "RM"),
}


# ==========================================================
# Helpers de texto y tipos
# ==========================================================

def read_text_best_effort(path: Path) -> str:
    """
    Lee un archivo de texto probando encodings en orden:
    utf-8 → latin-1 → cp1252.
    Como último recurso usa latin-1 con errors='ignore'.
    """
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return path.read_text(encoding=enc, errors="strict")
        except Exception:
            pass
    return path.read_text(encoding="latin-1", errors="ignore")


def safe_to_float(v) -> float | None:
    """
    Convierte un valor a float de forma segura.
    Maneja comas como separador decimal y expresiones con '='.
    Devuelve None si no se puede convertir.
    """
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if not s:
        return None
    if "=" in s:
        s = s.split("=")[-1].strip()
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def normalize_no_exact(x) -> str:
    """
    Normaliza el número de pozo (campo NO=) para comparaciones.
    Elimina espacios, guiones especiales y pasa a mayúsculas.
    Devuelve string vacío si el valor es nulo o inválido.
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    if not s:
        return ""
    if s.upper() in ("<NA>", "NAN", "NONE"):
        return ""
    s = s.replace("–", "-").replace("—", "-").replace("−", "-")
    s = re.sub(r"\s+", "", s)
    s = s.casefold().upper()
    return s


def normalize_fe_date(x):
    """
    Normaliza una fecha (campo FE=) a objeto date de Python.
    Acepta strings, datetime y Timestamps.
    Devuelve None si no se puede parsear.
    """
    from datetime import datetime
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, (datetime, pd.Timestamp)):
        return pd.to_datetime(x).date()
    s = str(x).strip()
    if not s:
        return None
    dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
    return dt.date() if not pd.isna(dt) else None


def normalize_ho_str(x) -> str:
    """
    Normaliza una hora (campo HO=) al formato HH:MM.
    Devuelve string vacío si no se puede parsear.
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    if not s:
        return ""
    try:
        t = pd.to_datetime(s, errors="coerce").time()
        if t:
            return f"{t.hour:02d}:{t.minute:02d}"
    except Exception:
        pass
    m = re.match(r"^(\d{1,2}):(\d{2})", s)
    if m:
        return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
    return s


def find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """
    Busca la primera columna del DataFrame que coincida
    (case-insensitive) con alguno de los candidatos.
    Devuelve None si no encuentra ninguna.
    """
    if df is None or df.empty:
        return None
    cols_upper = {c.upper(): c for c in df.columns}
    for cand in candidates:
        if cand.upper() in cols_upper:
            return cols_upper[cand.upper()]
    return None


def make_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra columnas duplicadas agregando sufijo _dup2, _dup3, etc.
    para evitar errores en operaciones posteriores.
    """
    cols  = list(df.columns)
    seen  = {}
    new_cols = []
    for c in cols:
        if c not in seen:
            seen[c] = 1
            new_cols.append(c)
        else:
            seen[c] += 1
            new_cols.append(f"{c}_dup{seen[c]}")
    df.columns = new_cols
    return df


# ==========================================================
# Parseo de puntos CS (Carta de Superficie)
# ==========================================================

def parse_din_surface_points(path_str: str) -> pd.DataFrame:
    """
    Parsea los puntos X/Y de la sección [CS] de un archivo .din.

    Acepta:
        - path local: C:\\...\\archivo.din
        - path GCS:   gs://bucket/data_store/din/.../archivo.din

    Returns:
        pd.DataFrame con columnas: i, X, Y
        DataFrame vacío si no hay sección [CS] o no hay puntos válidos.
    """
    # Si es GCS, descargar a /tmp primero
    if is_gs_path(path_str):
        path_str = gcs_download_to_temp(path_str)

    p   = Path(path_str)
    txt = read_text_best_effort(p)

    section = None
    xs: dict[int, float] = {}
    ys: dict[int, float] = {}
    in_cs = False

    for line in txt.splitlines():
        m = SECTION_RE.match(line)
        if m:
            section = m.group(1).strip().upper()
            in_cs   = (section == "CS")
            continue

        m = KV_RE.match(line)
        if not m or not section:
            continue

        k_raw = m.group(1).strip()
        v_raw = m.group(2).strip()

        if in_cs:
            mk = POINT_KEY_RE.match(k_raw)
            if mk:
                xy  = mk.group(1).upper()
                idx = int(mk.group(2))
                try:
                    val = float(v_raw.replace(",", "."))
                except Exception:
                    continue
                if xy == "X":
                    xs[idx] = val
                else:
                    ys[idx] = val

    idxs = sorted(set(xs.keys()) & set(ys.keys()))
    return pd.DataFrame({
        "i": idxs,
        "X": [xs[i] for i in idxs],
        "Y": [ys[i] for i in idxs],
    })


# ==========================================================
# Parseo de campos extra del .din
# ==========================================================

def parse_din_extras(path_str: str) -> dict:
    """
    Extrae los campos definidos en EXTRA_FIELDS de un archivo .din.
    Incluye fallback para %Balance desde [RARR] PC=.

    Acepta paths locales o gs://.

    Returns:
        dict { nombre_columna: valor } para todas las claves de EXTRA_FIELDS.
        Los valores pueden ser float, str o None.
    """
    if is_gs_path(path_str):
        path_str = gcs_download_to_temp(path_str)

    p   = Path(path_str)
    txt = read_text_best_effort(p)

    wanted = {
        (sec.upper(), key.upper()): col
        for col, (sec, key) in EXTRA_FIELDS.items()
    }
    out     = {col: None for col in EXTRA_FIELDS.keys()}
    section = None

    for line in txt.splitlines():
        m = SECTION_RE.match(line)
        if m:
            section = m.group(1).strip().upper()
            continue
        m = KV_RE.match(line)
        if not m or not section:
            continue
        k = m.group(1).strip().upper()
        v = m.group(2).strip()
        if (section, k) in wanted:
            col = wanted[(section, k)]
            fv  = safe_to_float(v)
            out[col] = fv if fv is not None else (v if v != "" else None)

    # Fallback %Balance desde [RARR] PC=
    if out.get("%Balance") is None:
        section = None
        for line in txt.splitlines():
            m = SECTION_RE.match(line)
            if m:
                section = m.group(1).strip().upper()
                continue
            m = KV_RE.match(line)
            if not m or not section:
                continue
            k = m.group(1).strip().upper()
            v = m.group(2).strip()
            if (section, k) == ("RARR", "PC"):
                fv = safe_to_float(v)
                out["%Balance"] = fv if fv is not None else (v if v != "" else None)
                break

    return out


def parse_extras_for_paths(paths: list[str]) -> pd.DataFrame:
    """
    Extrae campos extra para una lista de paths .din.
    Devuelve un DataFrame con una fila por path.

    Errores individuales se registran como filas con None en todos los campos.
    """
    rows = []
    for pth in paths:
        try:
            if pth:
                rows.append(parse_din_extras(str(pth)))
            else:
                rows.append({k: None for k in EXTRA_FIELDS.keys()})
        except Exception:
            rows.append({k: None for k in EXTRA_FIELDS.keys()})
    return pd.DataFrame(rows)


# ==========================================================
# Parseo completo del .din (para diagnóstico IA)
# ==========================================================

def parse_din_full(path_str: str) -> dict:
    """
    Parseo completo de un archivo .din.
    Extrae TODAS las secciones como dict y los puntos CS.

    Usado por el módulo de diagnóstico IA.

    Returns:
        {
            "sections": { "SECCION": { "CLAVE": "valor" } },
            "cs_points": [ {"X": float, "Y": float}, ... ]
        }
    """
    txt      = read_text_best_effort(Path(path_str))
    sections: dict[str, dict] = {}
    section  = None
    xs: dict[int, float] = {}
    ys: dict[int, float] = {}
    in_cs    = False

    for line in txt.splitlines():
        m = SECTION_RE.match(line)
        if m:
            section = m.group(1).strip().upper()
            in_cs   = (section == "CS")
            sections.setdefault(section, {})
            continue

        m = KV_RE.match(line)
        if not m or not section:
            continue

        k = m.group(1).strip()
        v = m.group(2).strip()

        if in_cs:
            mp = POINT_KEY_RE.match(k)
            if mp:
                xy  = mp.group(1).upper()
                idx = int(mp.group(2))
                try:
                    val = float(v.replace(",", "."))
                except Exception:
                    continue
                (xs if xy == "X" else ys)[idx] = val
                continue

        sections[section][k] = v

    idxs      = sorted(set(xs) & set(ys))
    cs_points = [{"X": xs[i], "Y": ys[i]} for i in idxs]

    return {"sections": sections, "cs_points": cs_points}


def extract_variables_from_parsed(parsed: dict) -> dict:
    """
    Extrae las variables relevantes de un .din ya parseado con parse_din_full().
    Calcula la Sumergencia si hay datos de nivel disponibles.

    Returns:
        dict con todas las variables nombradas para el prompt IA.
    """
    secs = parsed.get("sections", {})

    def g(sec: str, key: str):
        return secs.get(sec.upper(), {}).get(key)

    v = {
        "NO":                g("GEN", "NO"),
        "FE":                g("GEN", "FE"),
        "HO":                g("GEN", "HO"),
        "Tipo_AIB":          g("AIB", "MA"),
        "Carrera_pulg":      safe_to_float(g("AIB", "CS")),
        "Golpes_min":        safe_to_float(g("AIB", "GM")),
        "Sentido_giro":      g("AIB", "SG"),
        "Tipo_contrapeso":   g("CONTRAPESO", "TP"),
        "Dist_contrapeso":   safe_to_float(g("CONTRAPESO", "DE")),
        "Polea_motor":       safe_to_float(g("MOTOR", "DP")),
        "Potencia_motor":    safe_to_float(g("MOTOR", "PN")),
        "RPM_motor":         safe_to_float(g("MOTOR", "RM")),
        "Diam_piston_pulg":  safe_to_float(g("BOMBA", "DP")),
        "Prof_bomba_m":      safe_to_float(g("BOMBA", "PB")),
        "Llenado_pct":       safe_to_float(g("BOMBA", "CA")),
        "PE_m":              safe_to_float(g("NIV", "PE")),
        "PB_m":              safe_to_float(g("NIV", "PB")),
        "NM_m":              safe_to_float(g("NIV", "NM")),
        "NC_m":              safe_to_float(g("NIV", "NC")),
        "ND_m":              safe_to_float(g("NIV", "ND")),
        "Contrapeso_actual": safe_to_float(g("RARE", "CA")),
        "Contrapeso_ideal":  safe_to_float(g("RARE", "CM")),
        "Pct_estructura":    safe_to_float(g("RARE", "SE")),
        "Pct_balance":       safe_to_float(g("RARR", "PC")),
        "Caudal_bruto":      safe_to_float(g("RBO", "CF")),
        "Torque_max":        safe_to_float(g("RAEB", "TM")),
    }

    # Calcular Sumergencia = PB - primer nivel disponible (NC > NM > ND)
    pb = v.get("Prof_bomba_m")
    for nk in ["NC_m", "NM_m", "ND_m"]:
        nv = v.get(nk)
        if pb is not None and nv is not None:
            v["Sumergencia_m"]    = round(pb - nv, 1)
            v["Base_sumergencia"] = nk.replace("_m", "")
            break
    else:
        v["Sumergencia_m"]    = None
        v["Base_sumergencia"] = None

    return v


# ==========================================================
# Análisis geométrico de la carta CS (shape analysis)
# ==========================================================

def describe_cs_shape(cs_points: list[dict]) -> str:
    """
    Calcula métricas geométricas de la carta dinamométrica de superficie.

    Incluye:
        - Detección de carta degenerada (plana o con ruido excesivo)
        - Área por método de Shoelace
        - Fill ratio (compacidad geométrica)
        - Detección de RULO real (cruce de ramas ascendente/descendente)
        - Métricas de subida y bajada
        - Panza extendida (gas en bomba / interferencia)
        - Ratio carga_min / carga_max

    NOTA: fill_ratio NO es llenado de bomba. Es solo geometría de la carta.

    Returns:
        String con todas las métricas en formato clave=valor separado por |
        para ser incluido en el prompt IA.
    """
    if not cs_points:
        return "Sin datos de carta de superficie [CS]."

    xs = [p["X"] for p in cs_points]
    ys = [p["Y"] for p in cs_points]
    n  = len(cs_points)

    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)
    carrera      = round(x_max - x_min, 1)
    rango_carga  = round(y_max - y_min, 1)

    # --- Detección de carta degenerada ---
    rango_relativo = (rango_carga / y_max) if y_max > 0 else 0
    carta_plana    = rango_relativo < 0.03

    inversiones = sum(
        1 for i in range(1, n - 1)
        if (ys[i] - ys[i-1]) * (ys[i+1] - ys[i]) < 0
    )
    ruido_excesivo   = inversiones > n * 0.40
    carta_degenerada = carta_plana or ruido_excesivo

    if carta_degenerada:
        motivo = []
        if carta_plana:
            motivo.append(
                f"rango_carga={rango_carga} es solo "
                f"{round(rango_relativo*100,1)}% de carga_max={round(y_max,1)} "
                f"(umbral: <3%)"
            )
        if ruido_excesivo:
            motivo.append(
                f"inversiones_señal={inversiones} sobre {n} puntos "
                f"({round(inversiones/n*100,1)}% > umbral 40%)"
            )
        return (
            f"CARTA_DEGENERADA=True | motivo={' | '.join(motivo)} | "
            f"n_puntos={n} | carrera_efectiva={carrera} | "
            f"carga_max={round(y_max,1)} | carga_min={round(y_min,1)} | "
            f"rango_carga={rango_carga} | "
            f"inversiones_señal={inversiones} | "
            f"rango_relativo_pct={round(rango_relativo*100,1)}"
        )

    # --- Área por Shoelace ---
    area = 0.0
    for i in range(n):
        j     = (i + 1) % n
        area += xs[i] * ys[j]
        area -= xs[j] * ys[i]
    area = round(abs(area) / 2.0, 1)

    rect_area  = carrera * rango_carga
    fill_ratio = round(area / rect_area, 2) if rect_area > 0 else 0

    if fill_ratio > 0.60:
        forma_desc = "muy_compacta"
    elif fill_ratio > 0.45:
        forma_desc = "normal"
    elif fill_ratio > 0.30:
        forma_desc = "delgada"
    else:
        forma_desc = "muy_delgada"

    idx_max     = ys.index(max(ys))
    idx_min     = ys.index(min(ys))
    pos_max_pct = round((xs[idx_max] - x_min) / (carrera or 1) * 100, 1)
    pos_min_pct = round((xs[idx_min] - x_min) / (carrera or 1) * 100, 1)

    # --- Separar ramas ascendente y descendente ---
    idx_x_max       = xs.index(max(xs))
    idx_x_min_start = xs.index(min(xs))

    if idx_x_max > idx_x_min_start:
        rama_sub = cs_points[idx_x_min_start:idx_x_max + 1]
        rama_baj = cs_points[idx_x_max:] + cs_points[:idx_x_min_start + 1]
    else:
        rama_sub = cs_points[idx_x_max:idx_x_min_start + 1]
        rama_baj = cs_points[idx_x_min_start:] + cs_points[:idx_x_max + 1]

    # --- Métricas de subida ---
    n_sub = max(2, int(len(rama_sub) * 0.30))
    if len(rama_sub) >= 2:
        ys_sub        = [p["Y"] for p in rama_sub]
        subida_dy     = round(max(ys_sub[:n_sub]) - ys_sub[0], 1)
        subida_brusca = subida_dy > rango_carga * 0.70
    else:
        ys_sub        = []
        subida_dy     = None
        subida_brusca = False

    # --- Métricas de bajada ---
    if len(rama_baj) >= 2:
        ys_baj       = [p["Y"] for p in rama_baj]
        bajada_dy    = round(ys_baj[-1] - ys_baj[-max(2, int(len(rama_baj)*0.30))], 1)
        bajada_lenta = bajada_dy > -(rango_carga * 0.15)
    else:
        ys_baj       = []
        bajada_dy    = None
        bajada_lenta = False

    # --- Detección de RULO real (cruce de ramas) ---
    rulo_detectado = False
    rulo_amplitud  = 0.0
    rulo_pos_pct   = None
    rulo_en_subida = False

    if len(rama_sub) >= 3 and len(rama_baj) >= 3:
        xs_sub = [p["X"] for p in rama_sub]
        xs_baj = [p["X"] for p in rama_baj]
        x_overlap_min = max(min(xs_sub), min(xs_baj))
        x_overlap_max = min(max(xs_sub), max(xs_baj))

        if x_overlap_max > x_overlap_min:

            def interp_y(x_target, pts):
                pts_s = sorted(pts, key=lambda p: p["X"])
                for k in range(len(pts_s) - 1):
                    x0, x1 = pts_s[k]["X"], pts_s[k+1]["X"]
                    if x0 <= x_target <= x1 and x1 > x0:
                        t = (x_target - x0) / (x1 - x0)
                        return pts_s[k]["Y"] + t * (pts_s[k+1]["Y"] - pts_s[k]["Y"])
                return None

            cruces = []
            for pt in rama_sub:
                x = pt["X"]
                if x_overlap_min <= x <= x_overlap_max:
                    y_sub = pt["Y"]
                    y_baj = interp_y(x, rama_baj)
                    if y_baj is not None:
                        diferencia = y_sub - y_baj
                        if diferencia > rango_carga * 0.05:
                            cruces.append({"x": x, "diferencia": diferencia})

            if cruces:
                rulo_detectado = True
                mejor          = max(cruces, key=lambda c: c["diferencia"])
                rulo_amplitud  = round(mejor["diferencia"], 1)
                rulo_pos_pct   = round(
                    (mejor["x"] - x_min) / (carrera or 1) * 100, 1
                )

    # --- Ratio carga_min / carga_max ---
    ratio_carga_min_max = round(y_min / y_max, 3) if y_max > 0 else None

    # --- Panza extendida ---
    panza_extendida = False
    if len(ys_baj) >= 6:
        umbral_panza = rango_carga * 0.05
        ventana      = max(3, int(len(ys_baj) * 0.30))
        for k in range(len(ys_baj) - ventana):
            tramo = ys_baj[k:k + ventana]
            if max(tramo) - min(tramo) < umbral_panza:
                panza_extendida = True
                break

    return (
        f"n_puntos={n} | carrera_efectiva={carrera} | "
        f"carga_max={round(y_max,1)} | carga_min={round(y_min,1)} | "
        f"rango_carga={rango_carga} | "
        f"area={area} | fill_ratio={fill_ratio} | forma={forma_desc} | "
        f"NOTA_fill_ratio=geometria_carta_no_llenado_bomba | "
        f"ratio_carga_min_max={ratio_carga_min_max} | "
        f"panza_extendida={panza_extendida} | "
        f"pos_carga_max={pos_max_pct}%_carrera | "
        f"pos_carga_min={pos_min_pct}%_carrera | "
        f"subida_dy={subida_dy} | subida_brusca={subida_brusca} | "
        f"bajada_dy={bajada_dy} | "
        f"bajada_lenta_posible_fuga_fija={bajada_lenta} | "
        f"rulo_en_bajada={rulo_detectado} | "
        f"rulo_amplitud={rulo_amplitud} | "
        f"rulo_pos_en_carrera={rulo_pos_pct}% | "
        f"rulo_en_subida={rulo_en_subida}"
    )


# ==========================================================
# Keys de normalización (para joins din/niv)
# ==========================================================

def build_keys(
    df: pd.DataFrame,
    no_col: str,
    fe_col: str,
    ho_col: str | None,
) -> pd.DataFrame:
    """
    Agrega columnas NO_key, FE_key y HO_key al DataFrame
    para poder hacer joins entre índices DIN y NIV.

    Args:
        df:     DataFrame a procesar
        no_col: nombre de la columna con el número de pozo
        fe_col: nombre de la columna con la fecha
        ho_col: nombre de la columna con la hora (puede ser None)

    Returns:
        DataFrame con las 3 columnas key agregadas.
    """
    df = df.copy()
    df["NO_key"] = (
        df[no_col].apply(normalize_no_exact)
        if no_col in df.columns else ""
    )
    df["FE_key"] = (
        df[fe_col].apply(normalize_fe_date)
        if fe_col in df.columns else None
    )
    if ho_col and ho_col in df.columns:
        df["HO_key"] = df[ho_col].apply(normalize_ho_str)
    else:
        df["HO_key"] = ""
    return df
