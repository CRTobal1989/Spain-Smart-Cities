"""
Consolida archivos NetCDF diarios de polen en un DataFrame/CSV.

Lee todos los archivos ENS_FORECAST_*.nc descargados de CAMS,
extrae los datos del punto más cercano a Córdoba, calcula la
media diaria de cada variable (polen de olivo y gramíneas),
y exporta todo a un CSV limpio + carga en PostgreSQL.

¿Qué hace exactamente?
    1. Busca todos los archivos ENS_FORECAST_*.nc en data/polen/
    2. Para cada archivo, abre el NetCDF con xarray
    3. Selecciona el punto de la cuadrícula más cercano a Córdoba
    4. Calcula la media de las 3 horas (0h, 12h, 24h) → 1 valor/día
    5. Guarda todo en un CSV: fecha, polen_gramineas, polen_olivo
    6. (Opcional) Carga los datos en PostgreSQL

Se ejecuta desde la RAÍZ del proyecto:
    python -m src.processing.polen_consolidar_netcdf

Requisitos:
    pip install xarray netcdf4 pandas psycopg2-binary
"""

import os
import glob                # glob: buscar archivos con patrones (*.nc)
import pandas as pd        # pandas: manipulación de datos tabulares
import xarray as xr        # xarray: datos multidimensionales (NetCDF)
import numpy as np         # numpy: operaciones numéricas
from datetime import datetime
from dotenv import load_dotenv  # dotenv: carga variables de entorno desde .env

# Cargar variables de entorno del archivo .env
# (contiene DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
load_dotenv()


# ============================================================
# CONFIGURACIÓN
# ============================================================

# Coordenadas de Córdoba (las mismas que en cams_polen_client.py)
CORDOBA_LAT = 37.88
CORDOBA_LON = -4.78

# Carpeta donde están los archivos NetCDF descargados
CARPETA_DATOS = os.path.join(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    ),
    "data", "polen"
)

# Archivo CSV de salida (consolidado)
ARCHIVO_CSV = os.path.join(CARPETA_DATOS, "polen_cordoba_diario.csv")


# ============================================================
# FUNCIÓN PRINCIPAL: CONSOLIDAR ARCHIVOS
# ============================================================

def consolidar_netcdf_a_dataframe():
    """
    Lee TODOS los archivos ENS_FORECAST_*.nc y extrae los datos
    del punto más cercano a Córdoba.

    Cada archivo NetCDF contiene los datos de UN día, con:
        - gpg_conc: concentración de gramíneas (granos/m³)
        - opg_conc: concentración de olivo (granos/m³)
        - 3 pasos temporales: hora 0, 12 y 24

    Calculamos la MEDIA DIARIA de las 3 horas para obtener
    un único valor por día y variable.

    Returns:
        pd.DataFrame: Con columnas [fecha, polen_gramineas, polen_olivo]
    """

    print("=" * 60)
    print("CONSOLIDACIÓN DE DATOS DE POLEN")
    print("=" * 60)

    # --- PASO 1: Buscar todos los archivos NetCDF ---
    # glob.glob() devuelve una lista de rutas que coinciden con el patrón
    patron = os.path.join(CARPETA_DATOS, "ENS_FORECAST_*.nc")
    archivos = sorted(glob.glob(patron))

    if not archivos:
        print(f"   No se encontraron archivos en {CARPETA_DATOS}")
        print(f"   Ejecuta primero: python -m src.ingestion.cams_polen_client")
        return None

    print(f"   Archivos encontrados: {len(archivos)}")
    print(f"   Primero: {os.path.basename(archivos[0])}")
    print(f"   Último:  {os.path.basename(archivos[-1])}")

    # --- PASO 2: Leer cada archivo y extraer datos de Córdoba ---
    # Vamos acumulando filas en una lista (más eficiente que
    # ir añadiendo filas a un DataFrame una a una)
    filas = []
    errores = 0

    for i, archivo in enumerate(archivos):
        nombre = os.path.basename(archivo)

        try:
            # Abrir el NetCDF con xarray
            # engine='netcdf4' le dice a xarray qué librería usar
            ds = xr.open_dataset(archivo, engine="netcdf4")

            # Extraer la fecha del nombre del archivo
            # "ENS_FORECAST_2025-04-01.nc" → "2025-04-01"
            fecha_str = nombre.replace("ENS_FORECAST_", "").replace(".nc", "")

            # Seleccionar el punto más cercano a Córdoba
            # .sel() busca en la cuadrícula el punto con lat/lon más cercano
            punto_cordoba = ds.sel(
                latitude=CORDOBA_LAT,
                longitude=CORDOBA_LON,
                method="nearest",  # busca el vecino más cercano
            )

            # Calcular la MEDIA DIARIA de cada variable
            # .mean() promedia los 3 pasos temporales (0h, 12h, 24h)
            # .values extrae el número puro (sin metadatos de xarray)

            # gpg_conc = Grass Pollen Grains Concentration (gramíneas)
            # opg_conc = Olive Pollen Grains Concentration (olivo)

            # Detectar nombres de variables (pueden variar entre versiones)
            vars_disponibles = list(ds.data_vars)

            # Gramíneas: buscar gpg_conc o grass_pollen
            polen_gram = None
            for var_name in ["gpg_conc", "grass_pollen"]:
                if var_name in vars_disponibles:
                    # .mean() calcula la media de las horas del día
                    # skipna=True ignora valores nulos (NaN)
                    polen_gram = float(
                        punto_cordoba[var_name].mean(skipna=True).values
                    )
                    break

            # Olivo: buscar opg_conc o olive_pollen
            polen_olivo = None
            for var_name in ["opg_conc", "olive_pollen"]:
                if var_name in vars_disponibles:
                    polen_olivo = float(
                        punto_cordoba[var_name].mean(skipna=True).values
                    )
                    break

            # Añadir fila a la lista
            filas.append({
                "fecha": fecha_str,
                "polen_gramineas": round(polen_gram, 2) if polen_gram else None,
                "polen_olivo": round(polen_olivo, 2) if polen_olivo else None,
            })

            # Cerrar el archivo (liberar memoria)
            ds.close()

            # Mostrar progreso cada 30 archivos
            if (i + 1) % 30 == 0 or (i + 1) == len(archivos):
                print(f"   Procesados: {i + 1}/{len(archivos)}")

        except Exception as e:
            errores += 1
            print(f"   Error en {nombre}: {e}")
            continue

    # --- PASO 3: Crear DataFrame ---
    if not filas:
        print("   No se pudieron extraer datos de ningún archivo")
        return None

    # pd.DataFrame() convierte la lista de diccionarios en una tabla
    df = pd.DataFrame(filas)

    # Convertir la columna 'fecha' a tipo datetime
    # (permite ordenar, filtrar por rango, etc.)
    df["fecha"] = pd.to_datetime(df["fecha"])

    # Ordenar por fecha (por si los archivos no estaban ordenados)
    df = df.sort_values("fecha").reset_index(drop=True)

    # Reemplazar NaN por 0
    # CAMS no genera previsión de olivo en marzo (fuera de temporada),
    # así que devuelve NaN. En realidad significa 0 granos/m³.
    df["polen_gramineas"] = df["polen_gramineas"].fillna(0)
    df["polen_olivo"] = df["polen_olivo"].fillna(0)

    # --- PASO 4: Resumen ---
    print(f"\n{'='*60}")
    print(f"RESUMEN DE DATOS CONSOLIDADOS")
    print(f"{'='*60}")
    print(f"   Periodo: {df['fecha'].min().date()} → {df['fecha'].max().date()}")
    print(f"   Total días: {len(df)}")
    print(f"   Errores: {errores}")
    print(f"\n   Polen de gramíneas (granos/m³):")
    print(f"     Media:  {df['polen_gramineas'].mean():.2f}")
    print(f"     Máximo: {df['polen_gramineas'].max():.2f}")
    print(f"     Día pico: {df.loc[df['polen_gramineas'].idxmax(), 'fecha'].date()}")
    print(f"\n   Polen de olivo (granos/m³):")
    print(f"     Media:  {df['polen_olivo'].mean():.2f}")
    print(f"     Máximo: {df['polen_olivo'].max():.2f}")
    print(f"     Día pico: {df.loc[df['polen_olivo'].idxmax(), 'fecha'].date()}")

    return df


# ============================================================
# EXPORTAR A CSV
# ============================================================

def exportar_csv(df: pd.DataFrame):
    """
    Guarda el DataFrame en un archivo CSV.

    ¿Por qué CSV?
        - Es un formato universal (Excel, Python, R, SQL lo leen)
        - Es legible por humanos (puedes abrirlo con el Bloc de notas)
        - Es ligero (no tiene formato visual, solo datos)
    """

    # index=False evita que pandas añada una columna de índice numérico
    df.to_csv(ARCHIVO_CSV, index=False)

    tamano_kb = os.path.getsize(ARCHIVO_CSV) / 1024
    print(f"\n   CSV guardado: {ARCHIVO_CSV}")
    print(f"   Tamaño: {tamano_kb:.1f} KB")
    print(f"   Filas: {len(df)}")

    return ARCHIVO_CSV


# ============================================================
# CARGAR EN POSTGRESQL
# ============================================================

def cargar_en_postgresql(df: pd.DataFrame):
    """
    Carga los datos consolidados en PostgreSQL.

    Crea la tabla 'polen_diario' si no existe, con las columnas:
        - fecha (DATE): día de la medición
        - polen_gramineas (FLOAT): concentración media de gramíneas
        - polen_olivo (FLOAT): concentración media de olivo
        - latitud (FLOAT): coordenada del punto CAMS
        - longitud (FLOAT): coordenada del punto CAMS
        - fuente (TEXT): origen de los datos

    Usa INSERT ... ON CONFLICT para no duplicar datos si
    ejecutamos el script varias veces (upsert).
    """

    # psycopg2: librería de Python para conectar con PostgreSQL
    import psycopg2

    print(f"\n{'='*60}")
    print(f"CARGA EN POSTGRESQL")
    print(f"{'='*60}")

    try:
        # Conectar a PostgreSQL
        # Usa las variables de entorno del archivo .env
        # (igual que el resto del proyecto)
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        # autocommit=True: cada SQL se ejecuta inmediatamente
        # (sin necesidad de hacer conn.commit() después)
        conn.autocommit = True
        cursor = conn.cursor()

        # --- CREAR TABLA SI NO EXISTE ---
        # IF NOT EXISTS evita error si la tabla ya existía
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS polen_diario (
                fecha           DATE NOT NULL,
                polen_gramineas FLOAT,          -- granos/m³ (media diaria)
                polen_olivo     FLOAT,          -- granos/m³ (media diaria)
                latitud         FLOAT DEFAULT 37.88,   -- punto CAMS
                longitud        FLOAT DEFAULT -4.78,   -- punto CAMS
                fuente          TEXT DEFAULT 'CAMS-Copernicus',
                PRIMARY KEY (fecha)             -- no puede haber 2 registros del mismo día
            );
        """)
        print(f"   Tabla 'polen_diario' lista")

        # --- INSERTAR DATOS ---
        # ON CONFLICT (fecha) DO UPDATE: si ya existe un registro
        # para esa fecha, lo actualiza en vez de dar error.
        # Esto se llama "upsert" (update + insert).
        insertados = 0
        for _, fila in df.iterrows():
            cursor.execute("""
                INSERT INTO polen_diario (fecha, polen_gramineas, polen_olivo)
                VALUES (%s, %s, %s)
                ON CONFLICT (fecha) DO UPDATE SET
                    polen_gramineas = EXCLUDED.polen_gramineas,
                    polen_olivo = EXCLUDED.polen_olivo;
            """, (
                fila["fecha"].date(),
                fila["polen_gramineas"],
                fila["polen_olivo"],
            ))
            insertados += 1

        print(f"   Registros insertados/actualizados: {insertados}")

        # Verificar conteo total
        cursor.execute("SELECT COUNT(*) FROM polen_diario;")
        total = cursor.fetchone()[0]
        print(f"   Total registros en tabla: {total}")

        # Verificar rango de fechas
        cursor.execute("""
            SELECT MIN(fecha), MAX(fecha) FROM polen_diario;
        """)
        fecha_min, fecha_max = cursor.fetchone()
        print(f"   Rango: {fecha_min} → {fecha_max}")

        cursor.close()
        conn.close()
        print(f"   Conexión cerrada")

    except Exception as e:
        print(f"   Error conectando a PostgreSQL: {e}")
        print(f"   Los datos están en el CSV, puedes cargarlos manualmente")


# ============================================================
# BLOQUE PRINCIPAL
# ============================================================

if __name__ == "__main__":

    print("CONSOLIDACIÓN DE DATOS DE POLEN - CÓRDOBA")
    print("=" * 60)

    # Paso 1: Leer todos los NetCDF y consolidar
    df = consolidar_netcdf_a_dataframe()

    if df is not None:
        # Paso 2: Guardar CSV
        exportar_csv(df)

        # Paso 3: Cargar en PostgreSQL
        cargar_en_postgresql(df)

        # Paso 4: Mostrar primeras y últimas filas
        print(f"\n{'='*60}")
        print(f"PRIMERAS 5 FILAS:")
        print(df.head().to_string(index=False))
        print(f"\nÚLTIMAS 5 FILAS:")
        print(df.tail().to_string(index=False))

    print(f"\n{'='*60}")
    print(f"PROCESO COMPLETADO")
    print(f"{'='*60}")
