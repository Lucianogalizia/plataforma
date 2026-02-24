# ==========================================================
# backend/core/consolidado.py
#
# Lógica de consolidación DIN + NIV extraída de app.py
#
# Incluye:
#   - Deduplicación de registros DIN y NIV
#   - Join DIN + NIV por claves NO/FE/HO
#   - Cálculo de Sumergencia (PB - nivel)
#   - Inferencia de DT_plot (datetime para graficar)
#   - Snapshot global (última medición por pozo)
#   - Snapshot para mapa (1 fila por pozo con coordenadas)
#   - Tendencia lineal por mes
#   - Label de display para selector de mediciones
# ==========================================================

import pandas as pd

from core.parsers import (
    safe_to_float,
    normalize_no_exact,
    make_unique_columns,
    find_col,
    build_keys,
)


# ==========================================================
# Helpers internos
# ==========================================================

def _infer_dt_plot(dfp: pd.DataFrame) -> pd.Series:
    """
    Infiere la columna datetime para graficar (DT_plot).
    Orden de prioridad: din_datetime → niv_datetime → FE_key + HO_key.

    Returns:
        pd.Series con dtype datetime64
    """
    dt = None

    if "din_datetime" in dfp.columns:
        dt = pd.to_datetime(dfp["din_datetime"], errors="coerce")

    if dt is None or dt.isna().all():
        if "niv_datetime" in dfp.columns:
            dt = pd.to_datetime(dfp["niv_datetime"], errors="coerce")

    if dt is None:
        dt = pd.Series([pd.NaT] * len(dfp))

    if dt.isna().all() and "FE_key" in dfp.columns:
        try:
            ho = dfp["HO_key"] if "HO_key" in dfp.columns else ""
            dt = pd.to_datetime(
                dfp["FE_key"].astype(str) + " " + ho.astype(str),
                errors="coerce",
                dayfirst=True,
            )
        except Exception:
            pass

    return dt


def _pick_dt_plot(df: pd.DataFrame, preferred_cols: list[str]) -> pd.Series:
    """
    Devuelve un Series datetime usando la mejor columna disponible.
    Fallback a FE_key + HO_key si ninguna columna preferida tiene datos.
    """
    for c in preferred_cols:
        if c in df.columns:
            s = pd.to_datetime(df[c], errors="coerce")
            s = s.reindex(df.index)
            if not s.isna().all():
                return s

    if "FE_key" in df.columns:
        try:
            ho = df["HO_key"] if "HO_key" in df.columns else ""
            s  = pd.to_datetime(
                df["FE_key"].astype(str) + " " + ho.astype(str),
                errors="coerce",
                dayfirst=True,
            )
            return s.reindex(df.index)
        except Exception:
            pass

    return pd.Series([pd.NaT] * len(df), index=df.index)


def compute_sumergencia_and_base(row: pd.Series) -> tuple:
    """
    Calcula la Sumergencia = PB - nivel, usando el primer nivel disponible
    en orden de prioridad: NC → NM → ND.

    Returns:
        (sumergencia: float | None, base_usada: str | None)
        Ejemplo: (185.3, "NC")
    """
    pb = safe_to_float(row.get("PB"))
    if pb is None:
        return None, None

    nc = safe_to_float(row.get("NC"))
    nm = safe_to_float(row.get("NM"))
    nd = safe_to_float(row.get("ND"))

    if nc is not None:
        return pb - nc, "NC"
    if nm is not None:
        return pb - nm, "NM"
    if nd is not None:
        return pb - nd, "ND"

    return None, None


def make_display_label(row: pd.Series) -> str:
    """
    Construye el label para el selector de mediciones DIN.
    Formato: "YYYY-MM-DD | HH:MM | ORIGEN"

    Returns:
        String con el label, o "SIN_FECHA" si no hay datos.
    """
    fe     = row.get("fecha", None)
    ho     = row.get("hora",  None)
    origen = row.get("ORIGEN", "")
    parts  = []
    if fe:     parts.append(str(fe))
    if ho:     parts.append(str(ho))
    if origen: parts.append(str(origen))
    return " | ".join(parts) if parts else "SIN_FECHA"


# ==========================================================
# Deduplicación
# ==========================================================

def dedup_niv(df_niv_k: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplica el índice NIV.
    Ordena por niv_datetime/mtime y conserva el último registro
    por combinación (NO_key, FE_key, HO_key).

    Returns:
        DataFrame deduplicado, vacío si la entrada es vacía.
    """
    if df_niv_k is None or df_niv_k.empty:
        return pd.DataFrame()

    out      = df_niv_k.copy()
    sort_niv = [c for c in ["niv_datetime", "mtime"] if c in out.columns]
    if sort_niv:
        out = out.sort_values(sort_niv, na_position="last")

    out = out.drop_duplicates(
        subset=["NO_key", "FE_key", "HO_key"], keep="last"
    ).reset_index(drop=True)

    return out


def dedup_din(df_din_k: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplica el índice DIN.
    Ordena por din_datetime/mtime y deduplica por path (preferido)
    o por (NO_key, FE_key, HO_key).

    Returns:
        DataFrame deduplicado, vacío si la entrada es vacía.
    """
    if df_din_k is None or df_din_k.empty:
        return pd.DataFrame()

    out      = df_din_k.copy()
    sort_din = [c for c in ["din_datetime", "mtime"] if c in out.columns]
    if sort_din:
        out = out.sort_values(sort_din, na_position="last")

    if "path" in out.columns:
        out = out.drop_duplicates(subset=["path"], keep="last")
    else:
        out = out.drop_duplicates(
            subset=["NO_key", "FE_key", "HO_key"], keep="last"
        )

    return out.reset_index(drop=True)


# ==========================================================
# Consolidado global DIN + NIV
# ==========================================================

def build_global_consolidated(
    din_ok: pd.DataFrame,
    niv_ok: pd.DataFrame,
    din_no_col: str | None,
    din_fe_col: str | None,
    din_ho_col: str | None,
    niv_no_col: str | None,
    niv_fe_col: str | None,
    niv_ho_col: str | None,
) -> pd.DataFrame:
    """
    Consolida los índices DIN y NIV en un único DataFrame.

    Estrategia:
        1. Deduplica DIN y NIV por separado.
        2. Hace LEFT JOIN de DIN con NIV por (NO_key, FE_key, HO_key).
        3. Agrega registros NIV que no tienen DIN correspondiente.
        4. Calcula Sumergencia y DT_plot para cada fila.

    Args:
        din_ok / niv_ok: DataFrames ya filtrados (sin errores) con NO_key/FE_key/HO_key
        din_no_col .. niv_ho_col: nombres de columnas originales de pozo/fecha/hora

    Returns:
        DataFrame consolidado con columnas:
            ORIGEN, pozo, fecha, hora, NO_key, FE_key, HO_key,
            CO, empresa, SE, NM, NC, ND, PE, PB, CM,
            Sumergencia, Sumergencia_base, DT_plot
            + todas las columnas originales de DIN y NIV
    """
    din_d = dedup_din(din_ok)   if din_ok is not None else pd.DataFrame()
    niv_d = dedup_niv(niv_ok)   if niv_ok is not None else pd.DataFrame()

    # --- JOIN DIN ← NIV ---
    din_join = din_d.copy()
    if not din_join.empty:
        din_join["ORIGEN"] = "DIN"
        if not niv_d.empty:
            din_join = din_join.merge(
                niv_d,
                on=["NO_key", "FE_key", "HO_key"],
                how="left",
                suffixes=("", "_niv"),
            )

    # --- NIV sin DIN correspondiente ---
    niv_only = pd.DataFrame()
    if not niv_d.empty:
        if din_d.empty:
            niv_only = niv_d.copy()
        else:
            key_din  = din_d[["NO_key", "FE_key", "HO_key"]].drop_duplicates()
            niv_only = niv_d.merge(
                key_din,
                on=["NO_key", "FE_key", "HO_key"],
                how="left",
                indicator=True,
            )
            niv_only = niv_only[
                niv_only["_merge"] == "left_only"
            ].drop(columns=["_merge"])

        if not niv_only.empty:
            niv_only = niv_only.copy()
            niv_only["ORIGEN"] = "NIV"

    # --- Concat ---
    dfp_all = pd.concat([din_join, niv_only], ignore_index=True, sort=False)
    dfp_all = make_unique_columns(dfp_all)

    if dfp_all.empty:
        return dfp_all

    # --- Columnas canónicas de pozo/fecha/hora ---
    if din_no_col and din_no_col in dfp_all.columns:
        dfp_all["pozo"] = dfp_all[din_no_col]
    elif niv_no_col and niv_no_col in dfp_all.columns:
        dfp_all["pozo"] = dfp_all[niv_no_col]
    else:
        dfp_all["pozo"] = dfp_all["NO_key"]

    if din_fe_col and din_fe_col in dfp_all.columns:
        dfp_all["fecha"] = dfp_all[din_fe_col]
    elif niv_fe_col and niv_fe_col in dfp_all.columns:
        dfp_all["fecha"] = dfp_all[niv_fe_col]
    else:
        dfp_all["fecha"] = dfp_all["FE_key"].astype("string")

    if din_ho_col and din_ho_col in dfp_all.columns:
        dfp_all["hora"] = dfp_all[din_ho_col]
    elif niv_ho_col and niv_ho_col in dfp_all.columns:
        dfp_all["hora"] = dfp_all[niv_ho_col]
    else:
        dfp_all["hora"] = dfp_all["HO_key"].astype("string")

    # --- Promover columnas _niv si la original falta ---
    for c in ["CO", "empresa", "SE", "NM", "NC", "ND", "PE", "PB", "CM", "niv_datetime"]:
        if c not in dfp_all.columns:
            alt = f"{c}_niv"
            if alt in dfp_all.columns:
                dfp_all[c] = dfp_all[alt]

    # --- Tipos numéricos ---
    for c in ["NM", "NC", "ND", "PE", "PB"]:
        if c in dfp_all.columns:
            dfp_all[c] = dfp_all[c].apply(safe_to_float)

    # --- Sumergencia ---
    tmp = dfp_all.apply(
        compute_sumergencia_and_base, axis=1, result_type="expand"
    )
    dfp_all["Sumergencia"]      = tmp[0]
    dfp_all["Sumergencia_base"] = tmp[1]

    # --- DT_plot ---
    dfp_all["DT_plot"] = _infer_dt_plot(dfp_all)

    return dfp_all


# ==========================================================
# Snapshot global (última medición por pozo)
# ==========================================================

def build_last_snapshot_for_map(
    din_ok: pd.DataFrame,
    niv_ok: pd.DataFrame,
) -> pd.DataFrame:
    """
    Construye un snapshot con 1 fila por pozo (NO_key),
    usando la última medición disponible entre DIN y NIV.

    No asume que existan PB/NM/NC/ND/PE en los índices:
    si faltan, las crea como None.

    Calcula Sumergencia si hay datos suficientes.

    Returns:
        DataFrame con columnas:
            NO_key, ORIGEN, DT_plot, PB, NM, NC, ND, PE,
            Sumergencia, Sumergencia_base
    """

    def _prep_one(
        df: pd.DataFrame,
        origen: str,
        dt_candidates: list[str],
    ) -> pd.DataFrame:
        if df is None or df.empty or "NO_key" not in df.columns:
            return pd.DataFrame()

        keep = [
            c for c in [
                "NO_key", "mtime", "din_datetime", "niv_datetime",
                "FE_key", "HO_key", "PB", "NM", "NC", "ND", "PE",
            ]
            if c in df.columns
        ]
        d = df[keep].copy()

        d["NO_key"] = d["NO_key"].astype(str).map(lambda x: x.strip())
        d = d[d["NO_key"] != ""]

        d["ORIGEN"]   = origen
        d["DT_plot"]  = _pick_dt_plot(d, dt_candidates)

        # Asegurar columnas numéricas aunque no existan en el índice
        for c in ["PB", "NM", "NC", "ND", "PE"]:
            if c not in d.columns:
                d[c] = None
            d[c] = pd.to_numeric(d[c], errors="coerce")

        d    = d.sort_values(["NO_key", "DT_plot"], na_position="last")
        last = d.groupby("NO_key", as_index=False).tail(1).copy()

        out_cols = ["NO_key", "ORIGEN", "DT_plot", "PB", "NM", "NC", "ND", "PE"]
        return last[out_cols]

    din_last = _prep_one(din_ok, "DIN", ["din_datetime", "mtime"])
    niv_last = _prep_one(niv_ok, "NIV", ["niv_datetime", "mtime"])

    both = pd.concat([din_last, niv_last], ignore_index=True, sort=False)
    if both.empty:
        return pd.DataFrame()

    both = both.sort_values(["NO_key", "DT_plot"], na_position="last")
    snap = both.groupby("NO_key", as_index=False).tail(1).copy()

    # Calcular Sumergencia
    tmp  = snap.apply(compute_sumergencia_and_base, axis=1, result_type="expand")
    snap["Sumergencia"]      = tmp[0]
    snap["Sumergencia_base"] = tmp[1]

    return snap.reset_index(drop=True)


# ==========================================================
# Consolidado para un pozo individual (Tab Mediciones)
# ==========================================================

def build_pozo_consolidado(
    din_ok: pd.DataFrame,
    niv_ok: pd.DataFrame,
    pozo_sel: str,
    din_no_col: str | None,
    din_fe_col: str | None,
    din_ho_col: str | None,
    niv_no_col: str | None,
    niv_fe_col: str | None,
    niv_ho_col: str | None,
) -> pd.DataFrame:
    """
    Construye el consolidado DIN+NIV para un único pozo seleccionado.
    Equivalente a la lógica del Tab Mediciones en app.py.

    Returns:
        DataFrame consolidado con todas las columnas del pozo,
        ordenado por fecha/hora ascendente.
    """
    din_p = (
        din_ok[din_ok["NO_key"] == pozo_sel].copy()
        if not din_ok.empty else pd.DataFrame()
    )
    niv_p = (
        niv_ok[niv_ok["NO_key"] == pozo_sel].copy()
        if not niv_ok.empty else pd.DataFrame()
    )

    # Deduplicar
    if not niv_p.empty:
        sort_niv = [c for c in ["niv_datetime", "mtime"] if c in niv_p.columns]
        if sort_niv:
            niv_p = niv_p.sort_values(sort_niv, na_position="last")
        niv_p = niv_p.drop_duplicates(
            subset=["NO_key", "FE_key", "HO_key"], keep="last"
        ).reset_index(drop=True)

    if not din_p.empty:
        sort_din = [c for c in ["din_datetime", "mtime"] if c in din_p.columns]
        if sort_din:
            din_p = din_p.sort_values(sort_din, na_position="last")
        if "path" in din_p.columns:
            din_p = din_p.drop_duplicates(subset=["path"], keep="last")
        else:
            din_p = din_p.drop_duplicates(
                subset=["NO_key", "FE_key", "HO_key"], keep="last"
            )
        din_p = din_p.reset_index(drop=True)

    # JOIN
    din_join = din_p.copy()
    if not din_join.empty:
        din_join["ORIGEN"] = "DIN"
        if not niv_p.empty:
            din_join = din_join.merge(
                niv_p,
                on=["NO_key", "FE_key", "HO_key"],
                how="left",
                suffixes=("", "_niv"),
            )

    niv_only = pd.DataFrame()
    if not niv_p.empty:
        if din_p.empty:
            niv_only = niv_p.copy()
        else:
            key_din  = din_p[["NO_key", "FE_key", "HO_key"]].drop_duplicates()
            niv_only = niv_p.merge(
                key_din,
                on=["NO_key", "FE_key", "HO_key"],
                how="left",
                indicator=True,
            )
            niv_only = niv_only[
                niv_only["_merge"] == "left_only"
            ].drop(columns=["_merge"])

        if not niv_only.empty:
            niv_only = niv_only.copy()
            niv_only["ORIGEN"] = "NIV"

    dfp = pd.concat([din_join, niv_only], ignore_index=True, sort=False)
    dfp = make_unique_columns(dfp)

    if dfp.empty:
        return dfp

    # Columnas canónicas
    if din_no_col and din_no_col in dfp.columns:
        dfp["pozo"] = dfp[din_no_col]
    elif niv_no_col and niv_no_col in dfp.columns:
        dfp["pozo"] = dfp[niv_no_col]
    else:
        dfp["pozo"] = dfp["NO_key"]

    if din_fe_col and din_fe_col in dfp.columns:
        dfp["fecha"] = dfp[din_fe_col]
    elif niv_fe_col and niv_fe_col in dfp.columns:
        dfp["fecha"] = dfp[niv_fe_col]
    else:
        dfp["fecha"] = dfp["FE_key"].astype("string")

    if din_ho_col and din_ho_col in dfp.columns:
        dfp["hora"] = dfp[din_ho_col]
    elif niv_ho_col and niv_ho_col in dfp.columns:
        dfp["hora"] = dfp[niv_ho_col]
    else:
        dfp["hora"] = dfp["HO_key"].astype("string")

    # Promover _niv
    for c in ["CO", "empresa", "SE", "NM", "NC", "ND", "PE", "PB", "CM", "niv_datetime"]:
        if c not in dfp.columns:
            alt = f"{c}_niv"
            if alt in dfp.columns:
                dfp[c] = dfp[alt]

    # Numéricos
    for c in ["NM", "NC", "ND", "PE", "PB"]:
        if c in dfp.columns:
            dfp[c] = dfp[c].apply(safe_to_float)

    # Sumergencia
    tmp = dfp.apply(compute_sumergencia_and_base, axis=1, result_type="expand")
    dfp["Sumergencia"]      = tmp[0]
    dfp["Sumergencia_base"] = tmp[1]

    # DT_plot
    dfp["DT_plot"] = _infer_dt_plot(dfp)

    # Ordenar
    sort_cols = [
        c for c in ["FE_key", "HO_key", "din_datetime", "niv_datetime"]
        if c in dfp.columns
    ]
    if sort_cols:
        dfp = dfp.sort_values(sort_cols, na_position="last")

    return dfp.reset_index(drop=True)


# ==========================================================
# Tendencia lineal por mes
# ==========================================================

def trend_linear_per_month(
    df_one: pd.DataFrame,
    ycol: str,
) -> tuple | None:
    """
    Calcula la tendencia lineal de una variable a lo largo del tiempo,
    expresada como pendiente por mes.

    Args:
        df_one: DataFrame de un único pozo con columnas DT_plot e ycol
        ycol:   nombre de la columna a analizar

    Returns:
        (slope_por_mes, valor_inicial, valor_final, n_puntos)
        o None si no hay suficientes datos.
    """
    if (
        df_one is None
        or df_one.empty
        or ycol not in df_one.columns
        or "DT_plot" not in df_one.columns
    ):
        return None

    d = df_one[["DT_plot", ycol]].dropna().copy()
    if d.empty:
        return None

    d["DT_plot"] = pd.to_datetime(d["DT_plot"], errors="coerce")
    d = d.dropna(subset=["DT_plot"]).sort_values("DT_plot")
    if d.shape[0] < 2:
        return None

    t0       = d["DT_plot"].iloc[0]
    x_days   = (d["DT_plot"] - t0).dt.total_seconds() / 86400.0
    x_months = x_days / 30.4375
    y        = pd.to_numeric(d[ycol], errors="coerce")
    good     = (~x_months.isna()) & (~y.isna())
    x        = x_months[good].to_numpy()
    yv       = y[good].to_numpy()

    if len(x) < 2:
        return None

    x_mean = x.mean()
    y_mean = yv.mean()
    denom  = ((x - x_mean) ** 2).sum()
    if denom == 0:
        return None

    b = ((x - x_mean) * (yv - y_mean)).sum() / denom

    return float(b), float(yv[0]), float(yv[-1]), int(len(x))


# ==========================================================
# Preparación de índices con keys (helper para API)
# ==========================================================

def prepare_indexes(
    df_din: pd.DataFrame,
    df_niv: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    A partir de los índices crudos, detecta columnas y agrega keys
    de normalización (NO_key, FE_key, HO_key).

    Returns:
        (df_din_k, df_niv_k, col_map)
        donde col_map = {
            "din_no_col", "din_fe_col", "din_ho_col",
            "niv_no_col", "niv_fe_col", "niv_ho_col"
        }
    """
    din_no_col = find_col(df_din, ["pozo", "NO"])
    din_fe_col = find_col(df_din, ["fecha", "FE"])
    din_ho_col = find_col(df_din, ["hora",  "HO"])

    niv_no_col = find_col(df_niv, ["pozo", "NO"])
    niv_fe_col = find_col(df_niv, ["fecha", "FE"])
    niv_ho_col = find_col(df_niv, ["hora",  "HO"])

    if not df_din.empty and din_no_col and din_fe_col:
        df_din_k = build_keys(df_din, din_no_col, din_fe_col, din_ho_col)
    else:
        df_din_k = pd.DataFrame()

    if not df_niv.empty and niv_no_col and niv_fe_col:
        df_niv_k = build_keys(df_niv, niv_no_col, niv_fe_col, niv_ho_col)
    else:
        df_niv_k = pd.DataFrame()

    col_map = {
        "din_no_col": din_no_col,
        "din_fe_col": din_fe_col,
        "din_ho_col": din_ho_col,
        "niv_no_col": niv_no_col,
        "niv_fe_col": niv_fe_col,
        "niv_ho_col": niv_ho_col,
    }

    return df_din_k, df_niv_k, col_map
