# ==========================================================
# backend/core/acciones.py
#
# Lógica de persistencia de Acciones de Optimización en GCS
#
# Estructura GCS:
#   {GCS_PREFIX}/acciones/acciones.json  → JSON global con todas las acciones
#
# Cada acción tiene:
#   id, nombre_pozo, bateria, sist_extraccion,
#   fecha_accion, fecha_realizacion, fecha_fin (nullable),
#   tipo, accion, creado_utc, modificado_utc
#
# El estado (EN PROCESO / FINALIZADO) se calcula siempre
# en base a fecha_fin y NO se persiste.
# ==========================================================

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Optional

from core.gcs import GCS_BUCKET, GCS_PREFIX, get_gcs_client


# ==========================================================
# Ruta GCS del archivo global
# ==========================================================

def _acciones_blob_name() -> str:
    name = "acciones/acciones.json"
    return f"{GCS_PREFIX}/{name}" if GCS_PREFIX else name


# ==========================================================
# Lectura
# ==========================================================

def load_acciones() -> list[dict]:
    """
    Carga todas las acciones desde GCS.

    Returns:
        Lista de dicts con las acciones, [] si no existe aún.
    """
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return []

    try:
        blob = client.bucket(GCS_BUCKET).blob(_acciones_blob_name())
        if not blob.exists():
            return []
        data = json.loads(blob.download_as_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


# ==========================================================
# Escritura
# ==========================================================

def save_acciones(acciones: list[dict]) -> bool:
    """
    Guarda la lista completa de acciones en GCS.

    Returns:
        True si se guardó correctamente.
    """
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return False

    try:
        blob = client.bucket(GCS_BUCKET).blob(_acciones_blob_name())
        blob.upload_from_string(
            json.dumps(acciones, ensure_ascii=False, indent=2, default=str),
            content_type="application/json",
        )
        return True
    except Exception:
        return False


# ==========================================================
# CRUD helpers
# ==========================================================

def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _calcular_estado(accion: dict) -> str:
    """Calcula estado en base a fecha_fin."""
    return "FINALIZADO" if accion.get("fecha_fin") else "EN PROCESO"


def crear_accion(data: dict) -> dict:
    """
    Crea una nueva acción con id autogenerado y timestamps.
    Guarda en GCS.

    Returns:
        La acción creada con id y estado calculado.
    """
    now = _now_utc()
    nueva = {
        "id":                str(uuid.uuid4()),
        "nombre_pozo":       data.get("nombre_pozo", ""),
        "bateria":           data.get("bateria", ""),
        "sist_extraccion":   data.get("sist_extraccion", ""),
        "fecha_accion":      data.get("fecha_accion"),
        "fecha_realizacion": data.get("fecha_realizacion"),
        "fecha_fin":         data.get("fecha_fin"),
        "tipo":              data.get("tipo", ""),
        "tipo_accion":       data.get("tipo_accion", ""),
        "recurso":           data.get("recurso", ""),
        "neta_incremental":  data.get("neta_incremental"),
        "bruta_incremental": data.get("bruta_incremental"),
        "inyeccion":         data.get("inyeccion"),
        "accion":            data.get("accion", ""),
        "creado_utc":        now,
        "modificado_utc":    now,
    }

    acciones = load_acciones()
    acciones.append(nueva)
    save_acciones(acciones)

    nueva["estado"] = _calcular_estado(nueva)
    return nueva


def actualizar_accion(accion_id: str, data: dict) -> dict | None:
    """
    Actualiza una acción existente por id.
    Solo actualiza los campos provistos en data.

    Returns:
        La acción actualizada, o None si no se encontró.
    """
    acciones = load_acciones()
    idx = next((i for i, a in enumerate(acciones) if a.get("id") == accion_id), None)

    if idx is None:
        return None

    campos_editables = [
        "nombre_pozo", "bateria", "sist_extraccion",
        "fecha_accion", "fecha_realizacion", "fecha_fin",
        "tipo", "tipo_accion", "recurso",
        "neta_incremental", "bruta_incremental", "inyeccion",
        "accion",
    ]

    for campo in campos_editables:
        # Actualizamos si el campo está presente en data,
        # incluyendo cuando el valor es None (para borrar fecha_fin, etc.)
        if campo in data:
            acciones[idx][campo] = data[campo]

    acciones[idx]["modificado_utc"] = _now_utc()
    save_acciones(acciones)

    result = dict(acciones[idx])
    result["estado"] = _calcular_estado(result)
    return result


def eliminar_accion(accion_id: str) -> bool:
    """
    Elimina una acción por id.

    Returns:
        True si se encontró y eliminó.
    """
    acciones = load_acciones()
    nueva_lista = [a for a in acciones if a.get("id") != accion_id]

    if len(nueva_lista) == len(acciones):
        return False

    save_acciones(nueva_lista)
    return True


def get_accion_by_id(accion_id: str) -> dict | None:
    """Devuelve una acción por id, con estado calculado."""
    acciones = load_acciones()
    for a in acciones:
        if a.get("id") == accion_id:
            result = dict(a)
            result["estado"] = _calcular_estado(result)
            return result
    return None


def get_acciones_filtradas(
    nombre_pozo: Optional[str] = None,
    bateria: Optional[str] = None,
    estado: Optional[str] = None,
    tipo: Optional[str] = None,
    sist_extraccion: Optional[str] = None,
    mes: Optional[str] = None,          # formato "YYYY-MM"
    busqueda: Optional[str] = None,     # texto libre en campo accion
) -> list[dict]:
    """
    Carga todas las acciones, calcula estado y aplica filtros opcionales.

    Returns:
        Lista filtrada y ordenada por fecha_accion desc.
    """
    acciones = load_acciones()

    resultado = []
    for a in acciones:
        a = dict(a)
        a["estado"] = _calcular_estado(a)

        if nombre_pozo and a.get("nombre_pozo") != nombre_pozo:
            continue
        if bateria and a.get("bateria") != bateria:
            continue
        if estado and a.get("estado") != estado:
            continue
        if tipo and a.get("tipo") != tipo:
            continue
        if sist_extraccion and a.get("sist_extraccion") != sist_extraccion:
            continue
        if mes:
            fa = a.get("fecha_accion") or ""
            if not str(fa).startswith(mes):
                continue
        if busqueda:
            texto = (a.get("accion") or "").lower()
            if busqueda.lower() not in texto:
                continue

        resultado.append(a)

    # Ordenar por fecha_accion desc (más reciente primero)
    resultado.sort(key=lambda x: x.get("fecha_accion") or "", reverse=True)
    return resultado


def get_kpis_acciones() -> dict:
    """
    Calcula KPIs globales de acciones.

    Returns:
        { total, en_proceso, finalizadas }
    """
    acciones = load_acciones()
    en_proceso  = sum(1 for a in acciones if not a.get("fecha_fin"))
    finalizadas = sum(1 for a in acciones if a.get("fecha_fin"))
    return {
        "total":       len(acciones),
        "en_proceso":  en_proceso,
        "finalizadas": finalizadas,
    }
