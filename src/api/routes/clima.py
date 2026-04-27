# ============================================================
# routes/clima.py - Endpoints de Datos Climáticos
# ============================================================
# Endpoints para consultar datos meteorológicos diarios:
# filtros, últimos N días, estadísticas mensuales y récords.
# ============================================================

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, extract, Integer
from typing import List, Optional
from datetime import date

from src.api.database import get_db
from src.api.models import ClimaDiario, Municipio
from src.api.schemas import ClimaRegistro, ClimaResumen, EstadisticasMensuales


router = APIRouter(
    prefix="/clima",
    tags=["Datos Climáticos"]
)


# ============================================================
# GET /clima/ → Consulta con filtros + paginación
# ============================================================
# Ejemplo: /clima/?municipio=Córdoba&fecha_inicio=2024-01-01&limit=50
@router.get(
    "/",
    response_model=List[ClimaRegistro],
    summary="Consultar datos climáticos diarios"
)
def consultar_clima(
    municipio: Optional[str] = Query(None, description="Nombre del municipio"),
    codigo_ine: Optional[str] = Query(None, description="Código INE"),
    fecha_inicio: Optional[date] = Query(None, description="Fecha inicio (YYYY-MM-DD)"),
    fecha_fin: Optional[date] = Query(None, description="Fecha fin (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000, description="Máximo de registros"),
    offset: int = Query(0, ge=0, description="Registros a saltar (paginación)"),
    db: Session = Depends(get_db)
):
    """Consulta datos climáticos con filtros opcionales y paginación."""

    query = db.query(ClimaDiario)

    if municipio:
        query = query.join(Municipio).filter(
            Municipio.nombre.ilike(f"%{municipio}%")
        )

    if codigo_ine:
        query = query.filter(ClimaDiario.codigo_ine == codigo_ine)

    if fecha_inicio:
        query = query.filter(ClimaDiario.fecha >= fecha_inicio)

    if fecha_fin:
        query = query.filter(ClimaDiario.fecha <= fecha_fin)

    return query.order_by(
        ClimaDiario.fecha.desc()
    ).offset(offset).limit(limit).all()


# ============================================================
# GET /clima/ultimos/{municipio}?dias=7 → Últimos N días
# ============================================================
@router.get(
    "/ultimos/{nombre_municipio}",
    response_model=List[ClimaResumen],
    summary="Últimos N días de clima"
)
def ultimos_dias(
    nombre_municipio: str,
    dias: int = Query(7, ge=1, le=365, description="Días hacia atrás"),
    db: Session = Depends(get_db)
):
    """Últimos N días de datos climáticos para un municipio."""

    registros = db.query(ClimaDiario).join(Municipio).filter(
        Municipio.nombre.ilike(f"%{nombre_municipio}%")
    ).order_by(
        ClimaDiario.fecha.desc()
    ).limit(dias).all()

    if not registros:
        raise HTTPException(
            status_code=404,
            detail=f"No se encontraron datos para '{nombre_municipio}'"
        )

    return registros


# ============================================================
# GET /clima/estadisticas/mensuales → Promedios mensuales
# ============================================================
@router.get(
    "/estadisticas/mensuales",
    response_model=List[EstadisticasMensuales],
    summary="Estadísticas mensuales agregadas"
)
def estadisticas_mensuales(
    municipio: Optional[str] = Query(None, description="Nombre del municipio"),
    anio: Optional[int] = Query(None, ge=2015, le=2026, description="Año"),
    db: Session = Depends(get_db)
):
    """
    Estadísticas mensuales: promedios de temperatura,
    precipitación total y días de lluvia.

    Usa GROUP BY para agrupar por año-mes.
    """

    query = db.query(
        extract('year', ClimaDiario.fecha).label('anio'),
        extract('month', ClimaDiario.fecha).label('mes'),
        func.round(func.avg(ClimaDiario.temp_media), 1).label('temp_media_promedio'),
        func.round(func.avg(ClimaDiario.temp_max), 1).label('temp_max_promedio'),
        func.round(func.avg(ClimaDiario.temp_min), 1).label('temp_min_promedio'),
        func.round(func.sum(ClimaDiario.precipitacion), 1).label('precipitacion_total'),
        func.sum(
            func.cast(ClimaDiario.precipitacion > 0, Integer)
        ).label('dias_lluvia')
    )

    if municipio:
        query = query.join(Municipio).filter(
            Municipio.nombre.ilike(f"%{municipio}%")
        )

    if anio:
        query = query.filter(extract('year', ClimaDiario.fecha) == anio)

    return query.group_by(
        extract('year', ClimaDiario.fecha),
        extract('month', ClimaDiario.fecha)
    ).order_by(
        extract('year', ClimaDiario.fecha),
        extract('month', ClimaDiario.fecha)
    ).all()


# ============================================================
# GET /clima/records/{municipio} → Récords climáticos
# ============================================================
@router.get(
    "/records/{nombre_municipio}",
    summary="Récords climáticos"
)
def records_climaticos(
    nombre_municipio: str,
    db: Session = Depends(get_db)
):
    """
    Récords climáticos: día más caluroso, más frío,
    más lluvioso y con más viento.
    """

    base = db.query(ClimaDiario).join(Municipio).filter(
        Municipio.nombre.ilike(f"%{nombre_municipio}%")
    )

    mas_caluroso = base.order_by(ClimaDiario.temp_max.desc().nullslast()).first()
    mas_frio = base.order_by(ClimaDiario.temp_min.asc().nullslast()).first()
    mas_lluvioso = base.order_by(ClimaDiario.precipitacion.desc().nullslast()).first()
    mas_ventoso = base.order_by(ClimaDiario.racha_viento.desc().nullslast()).first()

    if not mas_caluroso:
        raise HTTPException(
            status_code=404,
            detail=f"No se encontraron datos para '{nombre_municipio}'"
        )

    return {
        "municipio": nombre_municipio,
        "records": {
            "dia_mas_caluroso": {
                "fecha": str(mas_caluroso.fecha),
                "temp_max": mas_caluroso.temp_max
            },
            "dia_mas_frio": {
                "fecha": str(mas_frio.fecha),
                "temp_min": mas_frio.temp_min
            },
            "dia_mas_lluvioso": {
                "fecha": str(mas_lluvioso.fecha),
                "precipitacion_mm": mas_lluvioso.precipitacion
            },
            "dia_mas_ventoso": {
                "fecha": str(mas_ventoso.fecha),
                "racha_viento_kmh": mas_ventoso.racha_viento
            }
        }
    }
