# ============================================================
# models.py - Modelos ORM (mapeo de tablas PostgreSQL)
# ============================================================
# Cada clase = una tabla de tu base de datos.
# Cada atributo = una columna de la tabla.
#
# Estas clases mapean EXACTAMENTE las tablas clima_diario y
# municipio que ya creaste con psycopg2.
# ============================================================

from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.orm import relationship

# Importamos Base desde nuestro database.py
from src.api.database import Base


# ============================================================
# MODELO: Municipio
# ============================================================
class Municipio(Base):
    __tablename__ = "municipios"

    # Clave primaria
    codigo_ine = Column(String, primary_key=True)

    # Datos del municipio
    nombre = Column(String)
    provincia = Column(String)
    comunidad = Column(String)

    # Datos de la estación AEMET
    estacion_aemet = Column(String)
    nombre_estacion = Column(String)
    altitud_estacion = Column(Float)

    # Coordenadas WGS84 (las de Google Maps)
    latitud_4326 = Column(Float)
    longitud_4326 = Column(Float)

    # Coordenadas UTM (las oficiales en España)
    coord_x_25830 = Column(Float)
    coord_y_25830 = Column(Float)

    # Códigos de referencia espacial
    srid_grados = Column(String)
    srid_metros = Column(String)

    # Relación: un municipio tiene MUCHOS registros de clima
    registros_clima = relationship("ClimaDiario", back_populates="municipio")


# ============================================================
# MODELO: ClimaDiario
# ============================================================
class ClimaDiario(Base):
    __tablename__ = "clima_diario"

    id = Column(Integer, primary_key=True)

    # Clave foránea → vincula con la tabla municipio
    codigo_ine = Column(String, ForeignKey("municipios.codigo_ine"))

    fecha = Column(Date)

    # Temperaturas (°C)
    temp_media = Column(Float)
    temp_max = Column(Float)
    temp_min = Column(Float)

    # Precipitación (mm)
    precipitacion = Column(Float)

    # Sol y viento
    horas_sol = Column(Float)
    vel_viento = Column(Float)
    racha_viento = Column(Float)

    # Presión atmosférica (hPa)
    presion_max = Column(Float)
    presion_min = Column(Float)

    # Humedad relativa (%)
    humedad_media = Column(Float)
    humedad_max = Column(Float)
    humedad_min = Column(Float)

    # Relación inversa: desde un registro de clima → su municipio
    municipio = relationship("Municipio", back_populates="registros_clima")
