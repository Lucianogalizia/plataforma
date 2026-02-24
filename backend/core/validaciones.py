# ==========================================================
# backend/core/validaciones.py
#
# Sistema de validación de sumergencias extraído de
# validaciones_tab.py
#
# Incluye:
#   - Normalización de fecha key
#   - Lectura de estado de validación por medición
#   - Escritura de validación con historial de cambios
#   - Construcción de tabla de validaciones para export
#   - Historial completo de cambios
# ==========================================================

from __future__ import annotations

from datetime import datetime, timezone


# ==========================================================
# Normalización de fecha key
# ==========================================================

def make_fecha_key(fecha) -> str:
    """
    Normaliza la fecha de una medición a string para usar
    como clave en el JSON de validaciones.

    Acepta: datetime, pd.Timestamp, string.
    Devuelve siempre formato "YYYY-MM-DD HH:MM" (16 chars).

    Ejemplo:
        make_fecha_key(pd.Timestamp("2025-03-15 09:30:00"))
        → "2025-03-15 09:30"
    """
    if hasattr(fecha, "strftime"):
        return fecha.strftime("%Y-%m-%d %H:%M")
    return str(fecha)[:16]


# ==========================================================
# Lectura de validaciones
# ==========================================================

def get_validacion(val_data: dict, fecha_key: str) -> dict:
    """
    Devuelve el estado de validación de una medición específica.

    Si no existe en el JSON, devuelve el estado por defecto:
    validada=True, sin comentario, sin historial.

    Args:
        val_data:  dict del pozo cargado desde GCS
                   { "pozo": ..., "mediciones": { fecha_key: {...} } }
        fecha_key: clave de la medición (formato "YYYY-MM-DD HH:MM")

    Returns:
        {
            "validada":   bool,
            "comentario": str,
            "historial":  list[dict]
        }
    """
    mediciones = val_data.get("mediciones", {})
    if fecha_key in mediciones:
        return mediciones[fecha_key]

    # Default: pre-validada sin comentario
    return {
        "validada":   True,
        "comentario": "",
        "historial":  [],
    }


def get_estado_validacion(
    todas_val: dict,
    no_key: str,
    fecha_key: str,
) -> dict:
    """
    Shortcut: obtiene el estado de validación dado el dict global,
    el no_key y la fecha_key.

    Args:
        todas_val: dict { no_key: val_dict } ya cargado desde GCS
        no_key:    identificador normalizado del pozo
        fecha_key: clave de la medición

    Returns:
        Mismo formato que get_validacion().
    """
    val_data = todas_val.get(no_key, {})
    return get_validacion(val_data, fecha_key)


# ==========================================================
# Escritura de validaciones
# ==========================================================

def set_validacion(
    val_data:   dict,
    no_key:     str,
    fecha_key:  str,
    validada:   bool,
    comentario: str,
    usuario:    str,
) -> dict:
    """
    Actualiza el estado de validación de una medición y agrega
    una entrada al historial de cambios (solo si algo cambió).

    Args:
        val_data:   dict actual del pozo (se modifica in-place y se devuelve)
        no_key:     identificador del pozo (para inicializar si está vacío)
        fecha_key:  clave de la medición
        validada:   nuevo estado (True = válida, False = dudosa/inválida)
        comentario: texto libre del usuario
        usuario:    nombre del usuario que realiza el cambio

    Returns:
        val_data actualizado con la nueva entrada y el historial.

    Estructura del JSON resultante:
        {
            "pozo": "POZO-001",
            "mediciones": {
                "2025-03-15 09:30": {
                    "validada":   true,
                    "comentario": "Dato confirmado en campo",
                    "historial": [
                        {
                            "timestamp":  "2025-03-15 12:00 UTC",
                            "usuario":    "jperez",
                            "validada":   true,
                            "comentario": "Dato confirmado en campo"
                        }
                    ]
                }
            }
        }
    """
    # Inicializar estructura si está vacía
    if "pozo" not in val_data:
        val_data["pozo"] = no_key
    if "mediciones" not in val_data:
        val_data["mediciones"] = {}

    entrada_actual = val_data["mediciones"].get(
        fecha_key, {"historial": []}
    )
    historial = entrada_actual.get("historial", [])

    # Solo agregar al historial si algo cambió
    cambio = (
        entrada_actual.get("validada")   != validada
        or entrada_actual.get("comentario", "") != comentario
    )

    if cambio:
        historial.append({
            "timestamp":  datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M UTC"
            ),
            "usuario":    usuario or "anónimo",
            "validada":   validada,
            "comentario": comentario,
        })

    val_data["mediciones"][fecha_key] = {
        "validada":   validada,
        "comentario": comentario,
        "historial":  historial,
    }

    return val_data


def set_validacion_bulk(
    todas_val: dict,
    cambios: list[dict],
    usuario: str,
) -> dict:
    """
    Aplica múltiples cambios de validación en una sola operación.
    Útil para guardar los cambios del data_editor de la tabla.

    Args:
        todas_val: dict global { no_key: val_dict }
        cambios:   lista de dicts con keys:
                   { no_key, fecha_key, validada, comentario }
        usuario:   nombre del usuario

    Returns:
        todas_val actualizado con todos los cambios aplicados.
    """
    for c in cambios:
        no_key     = c["no_key"]
        fecha_key  = c["fecha_key"]
        validada   = c["validada"]
        comentario = c.get("comentario", "")

        val_data = todas_val.get(no_key, {})
        val_data = set_validacion(
            val_data, no_key, fecha_key, validada, comentario, usuario
        )
        todas_val[no_key] = val_data

    return todas_val


# ==========================================================
# Construcción de tabla para visualización y export
# ==========================================================

def build_tabla_validaciones(
    df_tabla,
    todas_val: dict,
    normalize_no_fn,
) -> list[dict]:
    """
    Construye la tabla de validaciones a partir del DataFrame
    de pozos y el dict de validaciones cargado desde GCS.

    Args:
        df_tabla:        DataFrame con columnas: NO_key, nivel_5,
                         DT_plot, Sumergencia, Sumergencia_base
        todas_val:       dict { no_key: val_dict }
        normalize_no_fn: función para normalizar NO_key

    Returns:
        Lista de dicts con una entrada por fila del DataFrame:
        [
            {
                "validada":        bool,
                "pozo":            str,
                "bateria":         str,
                "fecha_medicion":  str,
                "sumergencia_m":   float | None,
                "base":            str,
                "comentario":      str,
                "usuario":         str,
                "no_key":          str,   # interno para updates
                "fecha_key":       str,   # interno para updates
            },
            ...
        ]
    """
    rows = []

    for _, row in df_tabla.iterrows():
        no_key    = normalize_no_fn(str(row.get("NO_key", "")))
        fecha_raw = row.get("DT_plot")
        fecha_key = make_fecha_key(fecha_raw)
        sumer     = row.get("Sumergencia")
        base      = row.get("Sumergencia_base", "")

        val_data = todas_val.get(no_key, {})
        estado   = get_validacion(val_data, fecha_key)
        historial = estado.get("historial", [])

        rows.append({
            "validada":       estado.get("validada",   True),
            "pozo":           row.get("NO_key",        ""),
            "bateria":        row.get("nivel_5",       ""),
            "fecha_medicion": fecha_key,
            "sumergencia_m":  float(sumer) if sumer is not None else None,
            "base":           str(base) if base else "",
            "comentario":     estado.get("comentario", ""),
            "usuario":        historial[-1].get("usuario", "") if historial else "",
            # Campos internos para updates desde el frontend
            "_no_key":        no_key,
            "_fecha_key":     fecha_key,
        })

    return rows


# ==========================================================
# Historial completo de validaciones
# ==========================================================

def build_historial_completo(todas_val: dict) -> list[dict]:
    """
    Construye el historial completo de todas las validaciones
    para exportar a CSV/Excel.

    Incluye:
        - Estado actual de cada medición
        - Cada cambio registrado en el historial

    Args:
        todas_val: dict { no_key: val_dict }

    Returns:
        Lista de dicts con columnas:
        Pozo, Fecha, Validada, Comentario, Tipo, Timestamp, Usuario
    """
    hist_rows = []

    for no_key, val_data in todas_val.items():
        for fecha_key, med in val_data.get("mediciones", {}).items():

            # Estado actual
            hist_rows.append({
                "Pozo":           no_key,
                "Fecha":          fecha_key,
                "Validada":       med.get("validada",   True),
                "Comentario":     med.get("comentario", ""),
                "Tipo":           "ESTADO_ACTUAL",
                "Timestamp":      "",
                "Usuario":        "",
            })

            # Cada cambio en el historial
            for h in med.get("historial", []):
                hist_rows.append({
                    "Pozo":       no_key,
                    "Fecha":      fecha_key,
                    "Validada":   h.get("validada",   True),
                    "Comentario": h.get("comentario", ""),
                    "Tipo":       "CAMBIO",
                    "Timestamp":  h.get("timestamp",  ""),
                    "Usuario":    h.get("usuario",    ""),
                })

    return hist_rows


# ==========================================================
# Helpers para el endpoint de validaciones
# ==========================================================

def detectar_cambios(
    df_original,
    df_editado,
    col_validada:   str = "validada",
    col_comentario: str = "comentario",
    col_no_key:     str = "_no_key",
    col_fecha_key:  str = "_fecha_key",
) -> list[dict]:
    """
    Detecta qué filas cambiaron entre el DataFrame original
    y el editado por el usuario (data_editor).

    Args:
        df_original: DataFrame antes de la edición
        df_editado:  DataFrame después de la edición
        col_*:       nombres de las columnas relevantes

    Returns:
        Lista de dicts con los cambios detectados:
        [{ no_key, fecha_key, validada, comentario }, ...]
    """
    cambios = []

    for i in range(len(df_original)):
        orig_val  = bool(df_original.iloc[i][col_validada])
        edit_val  = bool(df_editado.iloc[i][col_validada])
        orig_com  = str(df_original.iloc[i].get(col_comentario, "") or "").strip()
        edit_com  = str(df_editado.iloc[i].get(col_comentario, "")  or "").strip()

        if edit_val != orig_val or edit_com != orig_com:
            cambios.append({
                "no_key":     str(df_editado.iloc[i][col_no_key]),
                "fecha_key":  str(df_editado.iloc[i][col_fecha_key]),
                "validada":   edit_val,
                "comentario": edit_com,
            })

    return cambios


def resumen_validaciones(todas_val: dict) -> dict:
    """
    Calcula un resumen estadístico de las validaciones guardadas.

    Returns:
        {
            "total_pozos":       int,
            "total_mediciones":  int,
            "validadas":         int,
            "no_validadas":      int,
            "con_comentario":    int,
            "total_cambios":     int,
        }
    """
    total_med    = 0
    validadas    = 0
    no_validadas = 0
    con_comment  = 0
    total_cambios = 0

    for val_data in todas_val.values():
        for med in val_data.get("mediciones", {}).values():
            total_med += 1
            if med.get("validada", True):
                validadas += 1
            else:
                no_validadas += 1
            if med.get("comentario", "").strip():
                con_comment += 1
            total_cambios += len(med.get("historial", []))

    return {
        "total_pozos":      len(todas_val),
        "total_mediciones": total_med,
        "validadas":        validadas,
        "no_validadas":     no_validadas,
        "con_comentario":   con_comment,
        "total_cambios":    total_cambios,
    }
