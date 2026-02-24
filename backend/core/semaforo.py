# ==========================================================
# backend/core/semaforo.py
#
# Lógica de semáforo y calidad de datos extraída de app.py
#
# Incluye:
#   - Semáforo AIB (NORMAL / ALERTA / CRÍTICO)
#   - Calidad del dato (sumergencia negativa, PB anómalo)
#   - Cobertura DIN vs NIV (pozos con/sin DIN en ventana)
#   - Pozos medidos por mes
#   - Filtrado por validación
# ==========================================================

import pandas as pd

from core.parsers import safe_to_float, normalize_no_exact


# ==========================================================
# Semáforo AIB
# ==========================================================

def compute_semaforo_aib(
    row: pd.Series,
    se_target: str  = "AIB",
    sum_media: float = 200.0,
    sum_alta:  float = 250.0,
    llen_ok:   float = 70.0,
    llen_bajo: float = 50.0,
) -> str:
    """
    Calcula el estado del semáforo AIB para una fila del snapshot.

    Lógica:
        - Si SE != "AIB"          → "NO APLICA"
        - Si faltan datos          → "SIN DATOS"
        - Si s < sum_media
          O llen >= llen_ok        → "🟢 NORMAL"
        - Si s > sum_alta
          Y llen < llen_bajo       → "🔴 CRÍTICO"
        - Resto                    → "🟡 ALERTA"

    Args:
        row:       fila del DataFrame snapshot
        se_target: tipo de SE a evaluar (default "AIB")
        sum_media: umbral de sumergencia media (m)
        sum_alta:  umbral de sumergencia alta (m)
        llen_ok:   porcentaje de llenado OK (≥ este valor → normal)
        llen_bajo: porcentaje de llenado bajo (< este valor → crítico)

    Returns:
        "🟢 NORMAL" | "🟡 ALERTA" | "🔴 CRÍTICO" | "SIN DATOS" | "NO APLICA"
    """
    se     = row.get("SE", None)
    se_str = (
        str(se).strip().upper()
        if se is not None and not (isinstance(se, float) and pd.isna(se))
        else ""
    )

    if se_str != se_target:
        return "NO APLICA"

    s    = safe_to_float(row.get("Sumergencia"))
    llen = safe_to_float(row.get("Bba Llenado"))

    if s is None or llen is None:
        return "SIN DATOS"

    if s < sum_media or llen >= llen_ok:
        return "🟢 NORMAL"

    if s > sum_alta and llen < llen_bajo:
        return "🔴 CRÍTICO"

    return "🟡 ALERTA"


def apply_semaforo_aib(
    snap: pd.DataFrame,
    se_target: str  = "AIB",
    sum_media: float = 200.0,
    sum_alta:  float = 250.0,
    llen_ok:   float = 70.0,
    llen_bajo: float = 50.0,
) -> pd.DataFrame:
    """
    Aplica el semáforo AIB a todo el snapshot y agrega la columna
    'Semaforo_AIB'.

    Returns:
        DataFrame con columna Semaforo_AIB agregada.
    """
    df = snap.copy()

    if df.empty:
        df["Semaforo_AIB"] = pd.Series(dtype="string")
        return df

    df["Semaforo_AIB"] = df.apply(
        lambda r: compute_semaforo_aib(
            r,
            se_target=se_target,
            sum_media=sum_media,
            sum_alta=sum_alta,
            llen_ok=llen_ok,
            llen_bajo=llen_bajo,
        ),
        axis=1,
    )

    return df


def get_semaforo_counts(df: pd.DataFrame) -> dict:
    """
    Cuenta pozos por estado del semáforo AIB.

    Returns:
        {
            "total_aib":   int,
            "normal":      int,
            "alerta":      int,
            "critico":     int,
            "sin_datos":   int,
            "no_aplica":   int,
        }
    """
    if "Semaforo_AIB" not in df.columns or df.empty:
        return {
            "total_aib": 0,
            "normal":    0,
            "alerta":    0,
            "critico":   0,
            "sin_datos": 0,
            "no_aplica": 0,
        }

    se_col = df.get("SE", pd.Series(dtype="string"))

    return {
        "total_aib": int(
            se_col.astype(str).str.strip().str.upper().eq("AIB").sum()
        ) if "SE" in df.columns else 0,
        "normal":    int((df["Semaforo_AIB"] == "🟢 NORMAL").sum()),
        "alerta":    int((df["Semaforo_AIB"] == "🟡 ALERTA").sum()),
        "critico":   int((df["Semaforo_AIB"] == "🔴 CRÍTICO").sum()),
        "sin_datos": int((df["Semaforo_AIB"] == "SIN DATOS").sum()),
        "no_aplica": int((df["Semaforo_AIB"] == "NO APLICA").sum()),
    }


# ==========================================================
# Calidad del dato
# ==========================================================

def get_sumergencia_negativa(snap: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra los pozos con Sumergencia < 0.
    Indica posible error en los datos de PB o niveles.

    Returns:
        DataFrame con los pozos problemáticos, columnas relevantes.
    """
    if snap.empty or "Sumergencia" not in snap.columns:
        return pd.DataFrame()

    bad = snap[
        snap["Sumergencia"].notna() & (snap["Sumergencia"] < 0)
    ].copy()

    cols = [
        c for c in [
            "NO_key", "ORIGEN", "DT_plot",
            "PB", "NM", "NC", "ND",
            "Sumergencia", "Sumergencia_base",
        ]
        if c in bad.columns
    ]

    return bad[cols].sort_values(["Sumergencia"], na_position="last")


def get_pb_anomalo(snap: pd.DataFrame) -> tuple[pd.DataFrame, float | None, float | None]:
    """
    Detecta pozos con PB fuera del rango IQR (outliers estadísticos).
    Requiere al menos 10 valores de PB no nulos para calcular IQR.

    Returns:
        (df_anomalos, pb_low, pb_high)
        df_anomalos: DataFrame con los pozos con PB anómalo
        pb_low / pb_high: umbrales usados (None si no hay suficientes datos)
    """
    if snap.empty or "PB" not in snap.columns:
        return pd.DataFrame(), None, None

    pb_nonnull = snap["PB"].dropna()

    if len(pb_nonnull) < 10:
        return pd.DataFrame(), None, None

    q1v     = pb_nonnull.quantile(0.25)
    q3v     = pb_nonnull.quantile(0.75)
    iqr     = q3v - q1v
    pb_low  = q1v - 1.5 * iqr
    pb_high = q3v + 1.5 * iqr

    bad = snap[
        snap["PB"].notna() & (
            (snap["PB"] < pb_low) | (snap["PB"] > pb_high)
        )
    ].copy()

    cols = [
        c for c in [
            "NO_key", "ORIGEN", "DT_plot",
            "PB", "NM", "NC", "ND", "Sumergencia",
        ]
        if c in bad.columns
    ]

    return (
        bad[cols].sort_values(["PB"], na_position="last"),
        float(pb_low),
        float(pb_high),
    )


def get_calidad_resumen(snap: pd.DataFrame) -> dict:
    """
    Resumen completo de calidad del dato para el snapshot.

    Returns:
        {
            "sum_negativa":   int,   # pozos con Sumergencia < 0
            "pb_anomalo":     int,   # pozos con PB fuera de IQR
            "pb_faltante":    int,   # pozos sin PB
            "pb_low":         float | None,
            "pb_high":        float | None,
        }
    """
    bad_sum = get_sumergencia_negativa(snap)
    bad_pb, pb_low, pb_high = get_pb_anomalo(snap)

    return {
        "sum_negativa": len(bad_sum),
        "pb_anomalo":   len(bad_pb),
        "pb_faltante":  int(snap["PB"].isna().sum()) if "PB" in snap.columns else 0,
        "pb_low":       pb_low,
        "pb_high":      pb_high,
    }


# ==========================================================
# Cobertura DIN vs NIV
# ==========================================================

def get_cobertura_din_niv(
    df_all: pd.DataFrame,
    cov_from: pd.Timestamp,
    cov_to:   pd.Timestamp,
    modo:     str = "historico",
) -> dict:
    """
    Calcula la cobertura DIN vs NIV en una ventana de fechas.

    Args:
        df_all:   DataFrame consolidado global (todas las mediciones)
        cov_from: fecha inicio de la ventana
        cov_to:   fecha fin de la ventana
        modo:     "historico" = todas las mediciones en ventana
                  "snapshot"  = solo última medición por pozo en ventana

    Returns:
        {
            "total_pozos":      int,
            "pozos_con_din":    int,
            "pozos_sin_din":    int,
            "lista_sin_din":    list[str],
        }
    """
    if df_all.empty or "DT_plot" not in df_all.columns:
        return {
            "total_pozos":   0,
            "pozos_con_din": 0,
            "pozos_sin_din": 0,
            "lista_sin_din": [],
        }

    df_cov = df_all.copy()
    df_cov["DT_plot"] = pd.to_datetime(df_cov["DT_plot"], errors="coerce")
    df_cov = df_cov.dropna(subset=["DT_plot"])
    df_cov = df_cov[
        (df_cov["DT_plot"] >= cov_from) & (df_cov["DT_plot"] <= cov_to)
    ].copy()

    if modo == "snapshot":
        df_cov = (
            df_cov
            .sort_values(["NO_key", "DT_plot"], na_position="last")
            .groupby("NO_key", as_index=False)
            .tail(1)
        )

    has_din    = set(
        df_cov[df_cov["ORIGEN"] == "DIN"]["NO_key"].dropna().unique().tolist()
    )
    all_pozos  = set(df_cov["NO_key"].dropna().unique().tolist())
    never_din  = sorted(list(all_pozos - has_din))

    return {
        "total_pozos":   len(all_pozos),
        "pozos_con_din": len(has_din),
        "pozos_sin_din": len(never_din),
        "lista_sin_din": never_din,
    }


# ==========================================================
# Pozos medidos por mes
# ==========================================================

def get_pozos_por_mes(df_all: pd.DataFrame) -> pd.DataFrame:
    """
    Cuenta la cantidad de pozos únicos medidos por mes.

    Args:
        df_all: DataFrame consolidado global

    Returns:
        DataFrame con columnas: Mes (str "YYYY-MM"), Pozos_medidos (int)
        Ordenado por Mes ascendente.
    """
    if df_all.empty or "DT_plot" not in df_all.columns:
        return pd.DataFrame(columns=["Mes", "Pozos_medidos"])

    df_m = df_all.copy()
    df_m["DT_plot"] = pd.to_datetime(df_m["DT_plot"], errors="coerce")
    df_m = df_m.dropna(subset=["DT_plot"])
    df_m["Mes"] = df_m["DT_plot"].dt.to_period("M").astype(str)

    result = (
        df_m.groupby("Mes")["NO_key"]
        .nunique()
        .reset_index(name="Pozos_medidos")
        .sort_values("Mes")
    )

    return result


# ==========================================================
# Filtrado por validación
# ==========================================================

def filtrar_por_validacion(
    snap_map: pd.DataFrame,
    todas_val: dict,
    normalize_no_fn,
    solo_validadas: bool,
) -> pd.DataFrame:
    """
    Filtra snap_map según el estado de validación de cada medición.

    A diferencia de la versión original en validaciones_tab.py,
    esta versión recibe las validaciones ya cargadas (todas_val)
    en vez de cargarlas desde GCS internamente.
    Esto permite reusar la carga en el endpoint y no hacer
    múltiples llamadas a GCS.

    Args:
        snap_map:        DataFrame con columnas NO_key y DT_plot
        todas_val:       dict { no_key: val_dict } ya cargado desde GCS
        normalize_no_fn: función para normalizar NO_key
        solo_validadas:  True  → solo las validadas (sumergencias reales)
                         False → solo las NO validadas (marcadas como dudosas)

    Returns:
        DataFrame filtrado.
    """
    from core.validaciones import get_validacion, make_fecha_key

    def es_valida(row) -> bool:
        no_key    = normalize_no_fn(str(row.get("NO_key", "")))
        fecha_key = make_fecha_key(row.get("DT_plot"))
        val_data  = todas_val.get(no_key, {})
        estado    = get_validacion(val_data, fecha_key)
        return estado.get("validada", True)

    mask = snap_map.apply(es_valida, axis=1)
    return snap_map[mask].copy() if solo_validadas else snap_map[~mask].copy()


# ==========================================================
# Snapshot filtrado con extras (para Tab Estadísticas)
# ==========================================================

def build_snap_filtrado(
    snap: pd.DataFrame,
    origen_sel:  list[str] | None = None,
    sum_range:   tuple[float, float] | None = None,
    est_range:   tuple[float, float] | None = None,
    bal_range:   tuple[float, float] | None = None,
) -> pd.DataFrame:
    """
    Aplica los filtros de la Tab Estadísticas al snapshot.

    Args:
        snap:        DataFrame snapshot (última medición por pozo)
        origen_sel:  lista de orígenes a incluir (ej. ["DIN", "NIV"])
        sum_range:   (min, max) para Sumergencia
        est_range:   (min, max) para %Estructura
        bal_range:   (min, max) para %Balance

    Returns:
        DataFrame filtrado.
    """
    snap_f = snap.copy()

    if origen_sel and "ORIGEN" in snap_f.columns:
        snap_f = snap_f[snap_f["ORIGEN"].isin(origen_sel)]

    if sum_range and "Sumergencia" in snap_f.columns:
        snap_f = snap_f[
            snap_f["Sumergencia"].isna()
            | snap_f["Sumergencia"].between(sum_range[0], sum_range[1])
        ]

    if est_range and "%Estructura" in snap_f.columns:
        snap_f = snap_f[
            snap_f["%Estructura"].isna()
            | snap_f["%Estructura"].between(est_range[0], est_range[1])
        ]

    if bal_range and "%Balance" in snap_f.columns:
        snap_f = snap_f[
            snap_f["%Balance"].isna()
            | snap_f["%Balance"].between(bal_range[0], bal_range[1])
        ]

    return snap_f.copy()


def get_kpis_snapshot(snap: pd.DataFrame) -> dict:
    """
    Calcula los KPIs principales del snapshot filtrado.

    Returns:
        {
            "total_pozos":      int,
            "ultima_din":       int,
            "ultima_niv":       int,
            "con_sumergencia":  int,
            "con_pb":           int,
        }
    """
    if snap.empty:
        return {
            "total_pozos":     0,
            "ultima_din":      0,
            "ultima_niv":      0,
            "con_sumergencia": 0,
            "con_pb":          0,
        }

    return {
        "total_pozos":     len(snap),
        "ultima_din":      int((snap["ORIGEN"] == "DIN").sum())
                           if "ORIGEN" in snap.columns else 0,
        "ultima_niv":      int((snap["ORIGEN"] == "NIV").sum())
                           if "ORIGEN" in snap.columns else 0,
        "con_sumergencia": int(snap["Sumergencia"].notna().sum())
                           if "Sumergencia" in snap.columns else 0,
        "con_pb":          int(snap["PB"].notna().sum())
                           if "PB" in snap.columns else 0,
    }
