"""
Procesa archivos European Pollen Reanalysis (1980-2022) para Córdoba.

Lee los archivos cnc_srf_OLIVE_YYYY.nc4 descargados del dataset
European Pollen Reanalysis (modelo SILAM + ERA5), extrae el punto
más cercano a Córdoba, calcula la media diaria a partir de datos
horarios, y carga el resultado en PostgreSQL (tabla polen_diario).

¿Qué es el European Pollen Reanalysis?
    Es un dataset de 43 años (1980-2022) de concentración de polen
    en Europa, generado por el Finnish Meteorological Institute (FMI).
    Usa el modelo SILAM (System for Integrated modeLling of Atmospheric
    composition) alimentado con datos meteorológicos de ERA5 (ECMWF).
    Es el dataset de polen más largo y completo que existe para Europa.

¿Qué diferencia hay con CAMS?
    - CAMS: previsiones/análisis diarios, ~3 años de archivo, 3 horas/día
    - Reanalysis: datos históricos 1980-2022, datos HORARIOS (24h/día)
    - CAMS usa ensemble de 11 modelos; Reanalysis usa solo SILAM
    - Ambos tienen resolución 0.1° × 0.1° (aprox. 10 km)
    - Ambos miden en granos/m³

¿Qué es cnc_srf?
    "cnc" = concentration (concentración)
    "srf" = surface (superficie, a nivel del suelo)
    Es decir: la concentración de polen que respiras a ras de suelo.

Se ejecuta desde la RAÍZ del proyecto:
    python -m src.processing.polen_consolidar_reanalysis

Requisitos:
    pip install xarray netcdf4 pandas psycopg2-binary python-dotenv
"""

import os
import glob                # glob: buscar archivos con patrones
import pandas as pd        # pandas: manipulación de datos tabulares
import xarray as xr        # xarray: datos multidimensionales (NetCDF)
import numpy as np         # numpy: operaciones numéricas
from datetime import datetime
from dotenv import load_dotenv  # dotenv: carga variables desde .env

# Cargar variables de entorno del archivo .env
load_dotenv()


# ============================================================
# CONFIGURACIÓN
# ============================================================

# Coordenadas de Córdoba
CORDOBA_LAT = 37.88
CORDOBA_LON = -4.78

# Carpeta donde están los archivos de Reanalysis
CARPETA_REANALYSIS = os.path.join(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    ),
    "data", "polen", "reanalysis"
)

# CSV de salida
ARCHIVO_CSV = os.path.join(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    ),
    "data", "polen", "polen_reanalysis_cordoba.csv"
)

# Meses de interés: marzo a junio (temporada de olivo en Córdoba)
# Filtramos para que coincida con los datos de CAMS
MESES_TEMPORADA = [3, 4, 5, 6]


# ============================================================
# PASO 1: EXPLORAR UN ARCHIVO (para entender la estructura)
# ============================================================

def explorar_estructura(archivo: str):
    """
    Abre un archivo .nc4 y muestra su estructura interna.
    Útil para saber qué variables, coordenadas y dimensiones tiene.

    Lo ejecutamos una vez para entender el formato y luego
    sabemos exactamente qué extraer en el procesamiento masivo.
    """

    print(f"\n{'='*60}")
    print(f"EXPLORACIÓN: {os.path.basename(archivo)}")
    print(f"{'='*60}")

    # Abrir el archivo NetCDF4
    # engine='netcdf4' usa la librería netCDF4 de Python
    ds = xr.open_dataset(archivo, engine="netcdf4")

    # Mostrar información general del dataset
    print(f"\n📋 Variables disponibles:")
    for var in ds.data_vars:
        print(f"   - {var}")
        # attrs son los "atributos" o metadatos de la variable
        print(f"     Descripción: {ds[var].attrs.get('long_name', '?')}")
        print(f"     Unidades: {ds[var].attrs.get('units', '?')}")
        print(f"     Dimensiones: {ds[var].dims}")
        print(f"     Forma: {ds[var].shape}")

    print(f"\n📐 Coordenadas:")
    for coord in ds.coords:
        valores = ds[coord].values
        print(f"   - {coord}: {len(valores)} valores")
        if len(valores) > 0 and len(valores) < 100:
            print(f"     Primeros: {valores[:5]}")
        elif len(valores) >= 100:
            print(f"     Rango: {valores.min()} → {valores.max()}")

    # Mostrar atributos globales (metadatos del archivo)
    print(f"\n📄 Atributos globales:")
    for attr, valor in ds.attrs.items():
        print(f"   - {attr}: {valor}")

    ds.close()


# ============================================================
# PASO 2: PROCESAR TODOS LOS ARCHIVOS
# ============================================================

def procesar_reanalysis():
    """
    Lee todos los archivos cnc_srf_OLIVE_*.nc4, extrae el punto
    de Córdoba, calcula la MEDIA DIARIA a partir de datos horarios,
    y devuelve un DataFrame con la serie temporal completa.

    ¿Por qué media diaria?
        Los archivos tienen datos HORARIOS (24 valores por día).
        Para comparar con CAMS (que tiene 3 valores/día) y para
        los gráficos, necesitamos un único valor por día.
        La media diaria es la métrica estándar en estudios de polen.

    ¿Por qué filtramos marzo-junio?
        Porque es la temporada de polen de olivo en Córdoba.
        El resto del año la concentración es prácticamente 0,
        y no tiene sentido guardar 365 días × 8 años de ceros.

    Returns:
        pd.DataFrame con columnas: [fecha, polen_olivo, fuente]
    """

    print("=" * 60)
    print("PROCESAMIENTO DE EUROPEAN POLLEN REANALYSIS")
    print("=" * 60)

    # Buscar archivos de Reanalysis
    patron = os.path.join(CARPETA_REANALYSIS, "cnc_srf_OLIVE_*.nc4")
    archivos = sorted(glob.glob(patron))

    if not archivos:
        print(f"   No se encontraron archivos en {CARPETA_REANALYSIS}")
        print(f"   Descarga los archivos de:")
        print(f"   https://european-pollen-reanalysis.lake.fmi.fi/cnc_srf_OLIVE/")
        return None

    print(f"   Archivos encontrados: {len(archivos)}")
    for a in archivos:
        tamano_mb = os.path.getsize(a) / (1024 * 1024)
        print(f"     {os.path.basename(a)} ({tamano_mb:.0f} MB)")

    # Explorar el primer archivo para entender la estructura
    explorar_estructura(archivos[0])

    # --- PROCESAR CADA ARCHIVO (= cada año) ---
    todas_las_filas = []

    for archivo in archivos:
        nombre = os.path.basename(archivo)
        # Extraer el año del nombre: "cnc_srf_OLIVE_2015.nc4" → 2015
        anio = nombre.split("_")[-1].replace(".nc4", "")

        print(f"\n   Procesando {nombre} (año {anio})...")

        try:
            # Abrir el dataset
            ds = xr.open_dataset(archivo, engine="netcdf4")

            # Seleccionar el punto más cercano a Córdoba
            # method='nearest' busca la celda de 10km más cercana
            punto = ds.sel(
                lat=CORDOBA_LAT,
                lon=CORDOBA_LON,
                method="nearest",
            )

            # Obtener el nombre de la variable de concentración
            # (puede llamarse 'cnc_srf_OLIVE' o similar)
            var_nombre = list(ds.data_vars)[0]  # normalmente hay solo 1

            # Extraer la serie temporal del punto de Córdoba
            # .values convierte de xarray a numpy array
            serie = punto[var_nombre]

            # Convertir a DataFrame con la coordenada temporal
            # .to_dataframe() crea una tabla con índice = tiempo
            df_punto = serie.to_dataframe().reset_index()

            # Renombrar columnas para claridad
            # La columna de tiempo puede llamarse 'time' o 'datetime'
            col_tiempo = [c for c in df_punto.columns if 'time' in c.lower()
                          or c == 'datetime'][0]
            df_punto = df_punto.rename(columns={
                col_tiempo: "datetime",
                var_nombre: "polen_olivo",
            })

            # Extraer solo las columnas que necesitamos
            df_punto = df_punto[["datetime", "polen_olivo"]].copy()

            # Crear columna de fecha (sin hora) para agrupar por día
            df_punto["fecha"] = pd.to_datetime(df_punto["datetime"]).dt.date

            # Filtrar solo meses de temporada (marzo-junio)
            df_punto["mes"] = pd.to_datetime(df_punto["datetime"]).dt.month
            df_punto = df_punto[df_punto["mes"].isin(MESES_TEMPORADA)]

            # Calcular la MEDIA DIARIA
            # groupby('fecha') agrupa todas las horas del mismo día
            # .mean() calcula la media de esas 24 horas
            df_diario = (
                df_punto
                .groupby("fecha")["polen_olivo"]
                .mean()
                .reset_index()
            )

            # Redondear a 2 decimales
            df_diario["polen_olivo"] = df_diario["polen_olivo"].round(2)

            # Añadir columna de fuente para distinguir de CAMS
            df_diario["fuente"] = "SILAM-Reanalysis"

            todas_las_filas.append(df_diario)

            print(f"     Días extraídos: {len(df_diario)}")
            print(f"     Media olivo: {df_diario['polen_olivo'].mean():.2f} granos/m³")
            print(f"     Pico olivo:  {df_diario['polen_olivo'].max():.2f} granos/m³")

            ds.close()

        except Exception as e:
            print(f"     Error: {e}")
            continue

    # --- UNIR TODOS LOS AÑOS ---
    if not todas_las_filas:
        print("   No se pudieron procesar archivos")
        return None

    # pd.concat() une varios DataFrames verticalmente (uno debajo de otro)
    df_final = pd.concat(todas_las_filas, ignore_index=True)

    # Convertir fecha a datetime
    df_final["fecha"] = pd.to_datetime(df_final["fecha"])

    # Ordenar por fecha
    df_final = df_final.sort_values("fecha").reset_index(drop=True)

    # Reemplazar NaN por 0 (meses sin polen)
    df_final["polen_olivo"] = df_final["polen_olivo"].fillna(0)

    # --- RESUMEN ---
    print(f"\n{'='*60}")
    print(f"RESUMEN REANALYSIS CONSOLIDADO")
    print(f"{'='*60}")
    print(f"   Periodo: {df_final['fecha'].min().date()} → "
          f"{df_final['fecha'].max().date()}")
    print(f"   Total días: {len(df_final)}")
    print(f"   Media olivo: {df_final['polen_olivo'].mean():.2f} granos/m³")
    print(f"   Pico olivo:  {df_final['polen_olivo'].max():.2f} granos/m³")
    print(f"   Día pico: "
          f"{df_final.loc[df_final['polen_olivo'].idxmax(), 'fecha'].date()}")

    return df_final


# ============================================================
# PASO 3: EXPORTAR A CSV
# ============================================================

def exportar_csv(df: pd.DataFrame):
    """Guarda el DataFrame en un CSV."""

    df.to_csv(ARCHIVO_CSV, index=False)
    tamano_kb = os.path.getsize(ARCHIVO_CSV) / 1024
    print(f"\n   CSV guardado: {ARCHIVO_CSV}")
    print(f"   Tamaño: {tamano_kb:.1f} KB")
    print(f"   Filas: {len(df)}")


# ============================================================
# PASO 4: CARGAR EN POSTGRESQL
# ============================================================

def cargar_en_postgresql(df: pd.DataFrame):
    """
    Inserta los datos de Reanalysis en la tabla polen_diario.

    Usa INSERT ... ON CONFLICT para no duplicar datos.
    Los registros de Reanalysis se marcan con fuente='SILAM-Reanalysis'
    para distinguirlos de los datos CAMS (fuente='CAMS-Copernicus').

    IMPORTANTE: No sobreescribe datos de CAMS si ya existen para
    la misma fecha (CAMS es más fiable para datos recientes).
    """

    import psycopg2

    print(f"\n{'='*60}")
    print(f"CARGA EN POSTGRESQL")
    print(f"{'='*60}")

    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Verificar que la tabla existe
        # (debería existir si ya ejecutaste polen_consolidar_netcdf.py)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS polen_diario (
                fecha           DATE NOT NULL,
                polen_gramineas FLOAT,
                polen_olivo     FLOAT,
                latitud         FLOAT DEFAULT 37.88,
                longitud        FLOAT DEFAULT -4.78,
                fuente          TEXT DEFAULT 'CAMS-Copernicus',
                PRIMARY KEY (fecha)
            );
        """)

        # --- INSERTAR DATOS ---
        # ON CONFLICT (fecha) DO NOTHING: si ya hay un registro CAMS
        # para esa fecha, NO lo sobreescribimos.
        # Esto es importante porque para 2023+ tenemos datos CAMS
        # que son más fiables que extrapolar Reanalysis.
        insertados = 0
        omitidos = 0

        for _, fila in df.iterrows():
            cursor.execute("""
                INSERT INTO polen_diario (fecha, polen_olivo, fuente)
                VALUES (%s, %s, %s)
                ON CONFLICT (fecha) DO NOTHING;
            """, (
                fila["fecha"].date(),
                fila["polen_olivo"],
                fila["fuente"],
            ))
            # rowcount = 1 si insertó, 0 si ya existía
            if cursor.rowcount == 1:
                insertados += 1
            else:
                omitidos += 1

        print(f"   Registros insertados: {insertados}")
        print(f"   Registros omitidos (ya existían): {omitidos}")

        # Verificar conteo total
        cursor.execute("SELECT fuente, COUNT(*) FROM polen_diario GROUP BY fuente;")
        for fuente, conteo in cursor.fetchall():
            print(f"   {fuente}: {conteo} registros")

        cursor.execute("SELECT MIN(fecha), MAX(fecha) FROM polen_diario;")
        fecha_min, fecha_max = cursor.fetchone()
        print(f"   Rango total: {fecha_min} → {fecha_max}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"   Error: {e}")
        print(f"   Los datos están en el CSV")


# ============================================================
# BLOQUE PRINCIPAL
# ============================================================

if __name__ == "__main__":

    print("PROCESAMIENTO DE EUROPEAN POLLEN REANALYSIS - CÓRDOBA")
    print("=" * 60)

    # Paso 1: Procesar archivos NetCDF4
    df = procesar_reanalysis()

    if df is not None:
        # Paso 2: Guardar CSV
        exportar_csv(df)

        # Paso 3: Cargar en PostgreSQL
        cargar_en_postgresql(df)

        # Paso 4: Mostrar muestra
        print(f"\n{'='*60}")
        print(f"PRIMERAS 5 FILAS:")
        print(df.head().to_string(index=False))
        print(f"\nÚLTIMAS 5 FILAS:")
        print(df.tail().to_string(index=False))

    print(f"\n{'='*60}")
    print(f"PROCESO COMPLETADO")
    print(f"{'='*60}")
