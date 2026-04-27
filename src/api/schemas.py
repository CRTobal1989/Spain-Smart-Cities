# ============================================================
# schemas.py - Schemas de validación con Pydantic
# ============================================================
# Definen la FORMA de los datos que la API envía/recibe (JSON).
#
# Models (models.py) → representan las TABLAS de la BD
# Schemas (schemas.py) → representan los DATOS de la API
# ============================================================

from pydantic import BaseModel
from typing import Optional
from datetime import date


# ============================================================
# SCHEMA: MunicipioBase (resumen)
# ============================================================
class MunicipioBase(BaseModel):
    codigo_ine: str
    nombre: str
    provincia: str
    comunidad: str

    class Config:
        from_attributes = True  # Permite crear desde objetos SQLAlchemy


# ============================================================
# SCHEMA: MunicipioDetalle (completo)
# ============================================================
class MunicipioDetalle(MunicipioBase):
    estacion_aemet: Optional[str] = None
    nombre_estacion: Optional[str] = None
    altitud_estacion: Optional[float] = None
    latitud_4326: Optional[float] = None
    longitud_4326: Optional[float] = None
    coord_x_25830: Optional[float] = None
    coord_y_25830: Optional[float] = None
    srid_grados: Optional[str] = None
    srid_metros: Optional[str] = None


# ============================================================
# SCHEMA: ClimaRegistro (registro completo)
# ============================================================
class ClimaRegistro(BaseModel):
    id: int
    codigo_ine: str
    fecha: date

    temp_media: Optional[float] = None
    temp_max: Optional[float] = None
    temp_min: Optional[float] = None
    precipitacion: Optional[float] = None
    horas_sol: Optional[float] = None
    vel_viento: Optional[float] = None
    racha_viento: Optional[float] = None
    presion_max: Optional[float] = None
    presion_min: Optional[float] = None
    humedad_media: Optional[float] = None
    humedad_max: Optional[float] = None
    humedad_min: Optional[float] = None

    class Config:
        from_attributes = True


# ============================================================
# SCHEMA: ClimaResumen (versión ligera)
# ============================================================
class ClimaResumen(BaseModel):
    fecha: date
    temp_media: Optional[float] = None
    temp_max: Optional[float] = None
    temp_min: Optional[float] = None
    precipitacion: Optional[float] = None

    class Config:
        from_attributes = True


# ============================================================
# SCHEMA: EstadisticasMensuales (agregaciones)
# ============================================================
class EstadisticasMensuales(BaseModel):
    anio: int
    mes: int
    temp_media_promedio: Optional[float] = None
    temp_max_promedio: Optional[float] = None
    temp_min_promedio: Optional[float] = None
    precipitacion_total: Optional[float] = None
    dias_lluvia: Optional[int] = None

    class Config:
        from_attributes = True
