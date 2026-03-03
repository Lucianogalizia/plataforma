# ==========================================================
# backend/api/acciones.py
#
# Endpoints REST para Acciones de Optimización
#
# Rutas:
#   GET  /api/acciones/pozos-lista   → picklist de pozos desde Excel
#   GET  /api/acciones/kpis          → KPIs globales
#   GET  /api/acciones               → todas las acciones (con filtros)
#   POST /api/acciones               → crear nueva acción
#   GET  /api/acciones/{id}          → obtener una acción
#   PUT  /api/acciones/{id}          → editar acción
#   DELETE /api/acciones/{id}        → eliminar acción
# ==========================================================

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from core.gcs import load_coords_repo
from core.parsers import normalize_no_exact
from core.acciones import (
    crear_accion,
    actualizar_accion,
    eliminar_accion,
    get_accion_by_id,
    get_acciones_filtradas,
    get_kpis_acciones,
)

router = APIRouter()

# Sistemas de extracción válidos
SIST_EXTRACCION_VALIDOS = ["AIB", "BES", "PCP", "SWABBING", "SURGENTE", "OTRO"]
TIPOS_VALIDOS = ["Superficie", "Fondo"]
TIPOS_ACCION_VALIDOS = ["Optimización", "Operativa"]
RECURSOS_VALIDOS = ["eléctricos", "Grúa", "Operador BES", "Operador PCP", "Pulling", "WO", "químicos", "CT"]


# ==========================================================
# Modelos Pydantic
# ==========================================================

class AccionBase(BaseModel):
    nombre_pozo:       str
    bateria:           str
    sist_extraccion:   str
    fecha_accion:      str
    fecha_realizacion: Optional[str] = None
    fecha_fin:         Optional[str] = None
    tipo:              str
    tipo_accion:       str
    recurso:           str
    neta_incremental:  float
    bruta_incremental: float
    inyeccion:         float
    accion:            str


class AccionCreate(AccionBase):
    pass


class AccionUpdate(BaseModel):
    nombre_pozo:       Optional[str] = None
    bateria:           Optional[str] = None
    sist_extraccion:   Optional[str] = None
    fecha_accion:      Optional[str] = None
    fecha_realizacion: Optional[str] = None
    fecha_fin:         Optional[str] = None
    tipo:              Optional[str] = None
    tipo_accion:       Optional[str] = None
    recurso:           Optional[str] = None
    neta_incremental:  Optional[float] = None
    bruta_incremental: Optional[float] = None
    inyeccion:         Optional[float] = None
    accion:            Optional[str] = None


# ==========================================================
# GET /api/acciones/pozos-lista
# ==========================================================

@router.get("/pozos-lista")
async def get_pozos_lista():
    """
    Devuelve la lista de pozos desde el Excel de coordenadas,
    con nombre_pozo y bateria (nivel_5).
    Usado como picklist en el modal de carga.
    """
    coords = load_coords_repo()

    if coords.empty:
        return {"pozos": [], "total": 0}

    cols_req = ["nombre_pozo", "nivel_5"]
    for c in cols_req:
        if c not in coords.columns:
            return {"pozos": [], "total": 0}

    df = (
        coords[cols_req]
        .dropna(subset=["nombre_pozo"])
        .drop_duplicates(subset=["nombre_pozo"])
        .copy()
    )
    df["nombre_pozo"] = df["nombre_pozo"].astype(str).str.strip()
    df["nivel_5"]     = df["nivel_5"].astype(str).str.strip()
    df = df[df["nombre_pozo"] != ""].sort_values("nombre_pozo")

    pozos = [
        {"nombre_pozo": row["nombre_pozo"], "bateria": row["nivel_5"]}
        for _, row in df.iterrows()
    ]

    return {"pozos": pozos, "total": len(pozos)}


# ==========================================================
# GET /api/acciones/kpis
# ==========================================================

@router.get("/kpis")
async def get_kpis():
    """KPIs globales: total, en_proceso, finalizadas."""
    return get_kpis_acciones()


# ==========================================================
# GET /api/acciones
# ==========================================================

@router.get("")
async def listar_acciones(
    nombre_pozo:     Optional[str] = Query(None),
    bateria:         Optional[str] = Query(None),
    estado:          Optional[str] = Query(None, description="EN PROCESO | FINALIZADO"),
    tipo:            Optional[str] = Query(None, description="Superficie | Fondo"),
    sist_extraccion: Optional[str] = Query(None),
    mes:             Optional[str] = Query(None, description="Formato YYYY-MM"),
    busqueda:        Optional[str] = Query(None, description="Texto libre en descripción"),
):
    """
    Devuelve todas las acciones con filtros opcionales.
    El campo 'estado' se calcula siempre (no se persiste).
    """
    acciones = get_acciones_filtradas(
        nombre_pozo=nombre_pozo,
        bateria=bateria,
        estado=estado,
        tipo=tipo,
        sist_extraccion=sist_extraccion,
        mes=mes,
        busqueda=busqueda,
    )
    return {"total": len(acciones), "acciones": acciones}


# ==========================================================
# POST /api/acciones
# ==========================================================

@router.post("", status_code=201)
async def crear(body: AccionCreate):
    """Crea una nueva acción de optimización."""
    if body.sist_extraccion not in SIST_EXTRACCION_VALIDOS:
        raise HTTPException(
            status_code=422,
            detail=f"sist_extraccion debe ser uno de: {SIST_EXTRACCION_VALIDOS}"
        )
    if body.tipo not in TIPOS_VALIDOS:
        raise HTTPException(
            status_code=422,
            detail=f"tipo debe ser uno de: {TIPOS_VALIDOS}"
        )
    if body.tipo_accion not in TIPOS_ACCION_VALIDOS:
        raise HTTPException(
            status_code=422,
            detail=f"tipo_accion debe ser uno de: {TIPOS_ACCION_VALIDOS}"
        )
    if body.recurso not in RECURSOS_VALIDOS:
        raise HTTPException(
            status_code=422,
            detail=f"recurso debe ser uno de: {RECURSOS_VALIDOS}"
        )

    accion = crear_accion(body.model_dump())
    return accion


# ==========================================================
# GET /api/acciones/{accion_id}
# ==========================================================

@router.get("/{accion_id}")
async def obtener(accion_id: str):
    """Obtiene una acción por id."""
    accion = get_accion_by_id(accion_id)
    if not accion:
        raise HTTPException(status_code=404, detail="Acción no encontrada")
    return accion


# ==========================================================
# PUT /api/acciones/{accion_id}
# ==========================================================

@router.put("/{accion_id}")
async def editar(accion_id: str, body: AccionUpdate):
    """
    Edita una acción existente.
    Solo actualiza los campos provistos en el body (exclude_unset).
    Los campos nullable (fecha_fin, fecha_realizacion) pueden enviarse
    como null explícito para borrarlos.
    """
    # exclude_unset=True: solo los campos que el cliente envió explícitamente
    # Esto permite enviar fecha_fin=null para borrarla (volver a EN PROCESO)
    data = body.model_dump(exclude_unset=True)

    if "sist_extraccion" in data and data["sist_extraccion"] not in SIST_EXTRACCION_VALIDOS:
        raise HTTPException(
            status_code=422,
            detail=f"sist_extraccion debe ser uno de: {SIST_EXTRACCION_VALIDOS}"
        )
    if "tipo" in data and data["tipo"] not in TIPOS_VALIDOS:
        raise HTTPException(
            status_code=422,
            detail=f"tipo debe ser uno de: {TIPOS_VALIDOS}"
        )
    if "tipo_accion" in data and data["tipo_accion"] not in TIPOS_ACCION_VALIDOS:
        raise HTTPException(
            status_code=422,
            detail=f"tipo_accion debe ser uno de: {TIPOS_ACCION_VALIDOS}"
        )
    if "recurso" in data and data["recurso"] not in RECURSOS_VALIDOS:
        raise HTTPException(
            status_code=422,
            detail=f"recurso debe ser uno de: {RECURSOS_VALIDOS}"
        )

    accion = actualizar_accion(accion_id, data)
    if not accion:
        raise HTTPException(status_code=404, detail="Acción no encontrada")
    return accion


# ==========================================================
# DELETE /api/acciones/{accion_id}
# ==========================================================

@router.delete("/{accion_id}")
async def eliminar(accion_id: str):
    """Elimina una acción por id."""
    ok = eliminar_accion(accion_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Acción no encontrada")
    return {"ok": True, "id": accion_id}
