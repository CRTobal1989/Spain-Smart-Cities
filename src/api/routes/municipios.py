# ============================================================
# routes/municipios.py - Endpoints de Municipios
# ============================================================
# Rutas para consultar información de los 7 municipios.
# ============================================================

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List

# Imports adaptados a la estructura del proyecto
from src.api.database import get_db
from src.api.models import Municipio
from src.api.schemas import MunicipioBase, MunicipioDetalle


# Creamos el router con prefijo /municipios
router = APIRouter(
    prefix="/municipios",
    tags=["Municipios"]
)


# ============================================================
# GET /municipios/ → Lista de todos los municipios
# ============================================================
@router.get(
    "/",
    response_model=List[MunicipioBase],
    summary="Listar todos los municipios"
)
def listar_municipios(db: Session = Depends(get_db)):
    """Devuelve la lista de municipios disponibles."""
    return db.query(Municipio).all()


# ============================================================
# GET /municipios/buscar/?nombre=Córdoba → Buscar por nombre
# ============================================================
# IMPORTANTE: esta ruta va ANTES de /{codigo_ine}
# Si no, FastAPI interpretaría "buscar" como un código INE
@router.get(
    "/buscar/",
    response_model=List[MunicipioBase],
    summary="Buscar municipios por nombre"
)
def buscar_municipios(
    nombre: str = Query(
        ...,                              # Obligatorio
        min_length=2,
        description="Texto a buscar en el nombre del municipio"
    ),
    db: Session = Depends(get_db)
):
    """Busca municipios cuyo nombre contenga el texto indicado."""
    # ilike = búsqueda sin distinguir mayúsculas/minúsculas
    # %nombre% = busca el texto en cualquier posición
    return db.query(Municipio).filter(
        Municipio.nombre.ilike(f"%{nombre}%")
    ).all()


# ============================================================
# GET /municipios/{codigo_ine} → Detalle de un municipio
# ============================================================
@router.get(
    "/{codigo_ine}",
    response_model=MunicipioDetalle,
    summary="Detalle de un municipio"
)
def obtener_municipio(
    codigo_ine: str,
    db: Session = Depends(get_db)
):
    """Devuelve información completa de un municipio por su código INE."""
    municipio = db.query(Municipio).filter(
        Municipio.codigo_ine == codigo_ine
    ).first()

    if not municipio:
        raise HTTPException(
            status_code=404,
            detail=f"Municipio con código INE '{codigo_ine}' no encontrado"
        )

    return municipio
