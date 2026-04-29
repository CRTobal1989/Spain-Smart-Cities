"""
Cliente para descargar datos de polen de Copernicus CAMS.

Descarga datos de concentración de polen (olivo y gramíneas)
del servicio europeo CAMS (Copernicus Atmosphere Monitoring Service)
para la zona de Córdoba.

¿Qué es CAMS?
    Es el servicio europeo de monitorización atmosférica. Usa modelos
    matemáticos + datos de satélite + observaciones en tierra para
    calcular la concentración de contaminantes y polen en toda Europa.

¿Qué es NetCDF?
    Es un formato de archivo científico que guarda datos en forma de
    "cubo" multidimensional (latitud × longitud × tiempo × variable).
    Es el estándar en meteorología y ciencias del clima.
    QGIS lo abre directamente como capa ráster.

Requisitos:
    1. pip install cdsapi xarray netcdf4
    2. Archivo ~/.cdsapirc configurado con tu API Key de Copernicus
    3. Aceptar términos en:
       https://ads.atmosphere.copernicus.eu/datasets/cams-europe-air-quality-forecasts

Se ejecuta desde la RAÍZ del proyecto:
    python -m src.ingestion.cams_polen_client
"""

import os
import cdsapi          # cdsapi: librería oficial de Copernicus para descargar datos
import glob as globmod # glob: para buscar archivos con patrones (ej: *.nc)
from datetime import datetime, timedelta


# ============================================================
# CONFIGURACIÓN
# ============================================================

# Coordenadas de Córdoba (aproximadas)
# Las usaremos para recortar los datos después con xarray
CORDOBA_LAT = 37.88
CORDOBA_LON = -4.78

# Área de descarga (un cuadrado alrededor de Córdoba)
# Formato: [Norte, Oeste, Sur, Este]
# Cogemos un área amplia para tener contexto geográfico en QGIS
# (toda Andalucía aproximadamente)
AREA = [39.0, -7.5, 36.0, -1.5]  # Norte, Oeste, Sur, Este

# Directorio donde guardar los archivos NetCDF descargados
CARPETA_DATOS = os.path.join(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    ),
    "data", "polen"
)

# ============================================================
# TEMPORADAS A DESCARGAR
# ============================================================
# CAMS mantiene un archivo de ~3 años. Intentamos desde 2023.
# La temporada de polen de olivo y gramíneas en Córdoba va de
# marzo a junio (pico en abril-mayo).
#
# Formato: lista de tuplas (fecha_inicio, fecha_fin)
TEMPORADAS = [
    ("2023-04-23", "2023-06-30"),  # Primavera 2023 (CAMS empieza el 23/04)
    ("2024-03-01", "2024-06-30"),  # Primavera 2024 (completa)
    ("2025-03-01", "2025-06-30"),  # Primavera 2025 (completa)
    ("2026-03-01", "2026-04-29"),  # Primavera 2026 (actualizar fecha_fin)
]


# ============================================================
# FUNCIÓN PRINCIPAL DE DESCARGA
# ============================================================

def descargar_polen_cams(
    fecha_inicio: str,     # Formato 'YYYY-MM-DD'
    fecha_fin: str,        # Formato 'YYYY-MM-DD'
    variables: list = None # Lista de variables a descargar
):
    """
    Descarga datos de polen de CAMS para la zona de Andalucía.

    CAMS proporciona datos de concentración de polen (granos/m³)
    como parte de su servicio de calidad del aire europeo.

    El dataset se llama 'cams-europe-air-quality-forecasts' y
    contiene previsiones + análisis de polen para toda Europa.

    Args:
        fecha_inicio: Primera fecha a descargar (ej: '2024-04-01')
        fecha_fin: Última fecha a descargar (ej: '2024-06-30')
        variables: Lista de variables. Por defecto: olivo y gramíneas

    Returns:
        str: Ruta al archivo NetCDF descargado
    """

    # Variables por defecto: olivo y gramíneas
    # Son los dos pólenes más relevantes para Córdoba
    if variables is None:
        variables = [
            "olive_pollen",     # Polen de olivo (Olea europaea)
            "grass_pollen",     # Polen de gramíneas
        ]

    # Crear carpeta de datos si no existe
    os.makedirs(CARPETA_DATOS, exist_ok=True)

    # Nombre del archivo de salida
    # Ejemplo: cams_polen_2024-04-01_2024-06-30.nc
    archivo_salida = os.path.join(
        CARPETA_DATOS,
        f"cams_polen_{fecha_inicio}_{fecha_fin}.nc"
    )

    # Si ya existe el archivo, no lo descargamos de nuevo
    if os.path.exists(archivo_salida):
        print(f"   ℹ️ Ya existe: {archivo_salida}")
        print(f"   Borra el archivo si quieres re-descargarlo")
        return archivo_salida

    print(f"\n{'='*60}")
    print(f"DESCARGA DE DATOS DE POLEN - CAMS/COPERNICUS")
    print(f"{'='*60}")
    print(f"   Periodo: {fecha_inicio} → {fecha_fin}")
    print(f"   Variables: {', '.join(variables)}")
    print(f"   Área: Andalucía ({AREA})")
    print(f"   Formato: NetCDF")
    print(f"{'='*60}")

    # --- CREAR CLIENTE CDSAPI ---
    # Lee automáticamente tu archivo ~/.cdsapirc
    # (donde guardaste tu URL y API Key de Copernicus)
    cliente = cdsapi.Client()

    # --- CONSTRUIR PETICIÓN ---
    # Cada parámetro le dice a CAMS qué datos queremos:
    #
    # 'variable': qué medir (olive_pollen, grass_pollen...)
    # 'model': qué modelo usar. 'ensemble' = media de 11 modelos
    #          (más fiable que un solo modelo)
    # 'level': '0' = nivel de superficie (a ras de suelo)
    # 'type': 'forecast' = datos de previsión
    #         (los análisis no siempre tienen polen)
    # 'time': hora de inicio de la previsión
    # 'leadtime_hour': horas de predicción a futuro
    #         ['0','24'] = hora 0 (actual) y hora 24 (día siguiente)
    #         Esto nos da una media diaria aproximada
    # 'area': recorte geográfico [Norte, Oeste, Sur, Este]
    # 'format': formato del archivo de salida

    # Construir el rango de fechas como string
    # CAMS espera formato: 'YYYY-MM-DD/YYYY-MM-DD'
    rango_fechas = f"{fecha_inicio}/{fecha_fin}"

    peticion = {
        "variable": variables,
        "model": "ensemble",
        "level": "0",
        "type": "forecast",
        "time": "00:00",
        "leadtime_hour": [
            "0", "12", "24",    # Muestreamos 3 momentos del día
        ],
        "date": rango_fechas,
        "area": AREA,
        "format": "netcdf",
    }

    print(f"\n   📡 Enviando petición a CAMS...")
    print(f"   (esto puede tardar unos minutos)")

    # --- DESCARGAR ---
    # client.retrieve() envía la petición al servidor de Copernicus,
    # espera a que procese los datos, y descarga el resultado
    cliente.retrieve(
        "cams-europe-air-quality-forecasts",  # Nombre del dataset
        peticion,                              # Parámetros de la petición
        archivo_salida,                        # Dónde guardar el archivo
    )

    # Verificar que se descargó
    if not os.path.exists(archivo_salida):
        print(f"\n   ❌ Error: no se generó el archivo")
        return None

    tamano_mb = os.path.getsize(archivo_salida) / (1024 * 1024)
    print(f"\n   ✅ Descargado: {archivo_salida}")
    print(f"   📦 Tamaño: {tamano_mb:.1f} MB")

    # --- COMPROBAR SI ES UN ZIP ---
    # CAMS a veces envía los datos comprimidos en ZIP aunque pidamos NetCDF.
    # Detectamos leyendo los primeros 2 bytes: 'PK' = es un ZIP.
    import zipfile

    with open(archivo_salida, "rb") as f:
        cabecera = f.read(2)

    if cabecera == b"PK":
        print(f"   📦 El archivo es un ZIP. Descomprimiendo...")

        # zipfile: librería de Python para trabajar con archivos ZIP
        with zipfile.ZipFile(archivo_salida, "r") as zf:
            # Listar los archivos dentro del ZIP
            nombres = zf.namelist()
            print(f"   📂 Contenido del ZIP: {nombres}")

            # Extraer todos los archivos en la misma carpeta
            zf.extractall(CARPETA_DATOS)

        # Buscar el archivo NetCDF real dentro del ZIP
        # (normalmente hay un solo .nc dentro)
        archivo_nc_real = None
        for nombre in nombres:
            if nombre.endswith(".nc"):
                archivo_nc_real = os.path.join(CARPETA_DATOS, nombre)
                break

        if archivo_nc_real:
            print(f"   ✅ Extraído: {archivo_nc_real}")
            tamano_real = os.path.getsize(archivo_nc_real) / (1024 * 1024)
            print(f"   📦 Tamaño real: {tamano_real:.1f} MB")
            return archivo_nc_real
        else:
            # Si no hay .nc, devolver el primer archivo extraído
            archivo_extraido = os.path.join(CARPETA_DATOS, nombres[0])
            print(f"   ⚠️ No se encontró .nc. Archivo extraído: {archivo_extraido}")
            return archivo_extraido

    return archivo_salida


def explorar_netcdf(archivo: str):
    """
    Abre un archivo NetCDF y muestra su contenido.
    Útil para entender la estructura de los datos.

    ¿Qué es xarray?
        Es la librería de Python para trabajar con datos
        multidimensionales (como los NetCDF). Es como pandas
        pero para datos con coordenadas (lat, lon, tiempo).
    """

    # xarray: librería para datos multidimensionales (lat, lon, tiempo)
    import xarray as xr

    print(f"\n{'='*60}")
    print(f"EXPLORACIÓN DEL ARCHIVO")
    print(f"{'='*60}")

    # Intentamos abrir con distintos engines:
    # - 'netcdf4': formato NetCDF (el que pedimos)
    # - 'scipy': otro lector de NetCDF
    # - 'cfgrib': formato GRIB (a veces CAMS ignora el formato pedido)
    #
    # Si ninguno funciona, probamos con cfgrib (requiere pip install cfgrib)
    ds = None
    for engine in ["netcdf4", "scipy"]:
        try:
            ds = xr.open_dataset(archivo, engine=engine)
            print(f"   ✅ Abierto con engine: {engine}")
            break
        except Exception:
            continue

    # Si no funcionó, es probable que sea formato GRIB
    if ds is None:
        try:
            ds = xr.open_dataset(archivo, engine="cfgrib")
            print(f"   ✅ Abierto con engine: cfgrib (formato GRIB)")
        except ImportError:
            print(f"   ❌ El archivo parece estar en formato GRIB.")
            print(f"   Instala cfgrib: pip install cfgrib eccodes")
            return None
        except Exception as e:
            print(f"   ❌ No se pudo abrir el archivo: {e}")
            return None

    # Mostrar información general
    print(f"\n📋 Variables disponibles:")
    for var in ds.data_vars:
        print(f"   - {var}: {ds[var].attrs.get('long_name', 'sin descripción')}")
        print(f"     Unidades: {ds[var].attrs.get('units', 'sin unidades')}")
        print(f"     Dimensiones: {ds[var].dims}")
        print(f"     Forma: {ds[var].shape}")

    print(f"\n📐 Coordenadas:")
    for coord in ds.coords:
        valores = ds[coord].values
        print(f"   - {coord}: {len(valores)} valores")
        if len(valores) > 0:
            print(f"     Rango: {valores.min()} → {valores.max()}")

    # Extraer datos del punto más cercano a Córdoba
    print(f"\n📍 Datos en Córdoba (lat={CORDOBA_LAT}, lon={CORDOBA_LON}):")

    # .sel() selecciona datos por coordenadas
    # method='nearest' busca el punto de la cuadrícula más cercano
    cordoba = ds.sel(
        latitude=CORDOBA_LAT,
        longitude=CORDOBA_LON,
        method="nearest",
    )

    for var in ds.data_vars:
        datos_var = cordoba[var]
        # .mean() calcula la media ignorando nulos
        media = float(datos_var.mean(skipna=True))
        maximo = float(datos_var.max(skipna=True))
        print(f"   {var}:")
        print(f"     Media: {media:.2f} granos/m³")
        print(f"     Máximo: {maximo:.2f} granos/m³")

    ds.close()

    return ds


def listar_archivos_descargados():
    """
    Lista los archivos NetCDF ya descargados en la carpeta de datos.
    Devuelve un set con las fechas (YYYY-MM-DD) que ya tenemos.

    ¿Cómo funciona?
        Los archivos se llaman ENS_FORECAST_2025-04-01.nc
        Extraemos la parte de la fecha del nombre para saber
        qué días ya están descargados y no repetir la descarga.
    """

    archivos_existentes = set()

    if not os.path.exists(CARPETA_DATOS):
        return archivos_existentes

    # globmod.glob() busca archivos que coincidan con un patrón
    # El patrón "ENS_FORECAST_*.nc" encuentra todos los NetCDF diarios
    patron = os.path.join(CARPETA_DATOS, "ENS_FORECAST_*.nc")
    for ruta in globmod.glob(patron):
        # Extraer la fecha del nombre del archivo
        # "ENS_FORECAST_2025-04-01.nc" → "2025-04-01"
        nombre = os.path.basename(ruta)          # quita la carpeta
        fecha_str = nombre.replace("ENS_FORECAST_", "").replace(".nc", "")
        archivos_existentes.add(fecha_str)

    return archivos_existentes


def descargar_todas_temporadas():
    """
    Descarga TODAS las temporadas definidas en TEMPORADAS.
    Detecta automáticamente qué meses ya están descargados
    para no repetir peticiones a CAMS (ahorra tiempo y ancho de banda).

    Estrategia:
        1. Miramos qué archivos .nc ya tenemos en data/polen/
        2. Para cada temporada, comprobamos si faltan días
        3. Si faltan, hacemos la descarga mes a mes
           (CAMS funciona mejor con peticiones de 1 mes)
    """

    print("=" * 60)
    print("DESCARGA MULTITEMPORADA DE POLEN - CAMS/COPERNICUS")
    print("=" * 60)

    # Paso 1: Ver qué ya tenemos descargado
    ya_descargados = listar_archivos_descargados()
    print(f"\n   Archivos .nc ya descargados: {len(ya_descargados)}")

    # Paso 2: Recorrer cada temporada
    for fecha_inicio, fecha_fin in TEMPORADAS:

        print(f"\n{'─'*60}")
        print(f"   TEMPORADA: {fecha_inicio} → {fecha_fin}")
        print(f"{'─'*60}")

        # Convertir strings a objetos datetime para poder iterar por meses
        dt_inicio = datetime.strptime(fecha_inicio, "%Y-%m-%d")
        dt_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")

        # Iterar mes a mes dentro de la temporada
        # (CAMS funciona mejor con peticiones mensuales)
        mes_actual = dt_inicio.replace(day=1)  # Primer día del mes

        while mes_actual <= dt_fin:
            # Calcular el último día de este mes
            # Truco: ir al día 1 del mes siguiente y restar 1 día
            if mes_actual.month == 12:
                siguiente_mes = mes_actual.replace(year=mes_actual.year + 1, month=1)
            else:
                siguiente_mes = mes_actual.replace(month=mes_actual.month + 1)
            ultimo_dia_mes = siguiente_mes - timedelta(days=1)

            # Ajustar al rango de la temporada
            # (el primer mes puede empezar después del día 1,
            #  y el último mes puede acabar antes del último día)
            inicio_mes = max(mes_actual, dt_inicio)
            fin_mes = min(ultimo_dia_mes, dt_fin)

            # Comprobar si ya tenemos todos los días de este mes
            dias_mes = []
            d = inicio_mes
            while d <= fin_mes:
                dias_mes.append(d.strftime("%Y-%m-%d"))
                d += timedelta(days=1)

            # Contar cuántos días faltan
            dias_faltantes = [d for d in dias_mes if d not in ya_descargados]

            if len(dias_faltantes) == 0:
                print(f"   ✓ {inicio_mes.strftime('%Y-%m')}: "
                      f"completo ({len(dias_mes)} días)")
            else:
                print(f"   ↓ {inicio_mes.strftime('%Y-%m')}: "
                      f"faltan {len(dias_faltantes)} de {len(dias_mes)} días")

                # Descargar este mes
                try:
                    descargar_polen_cams(
                        fecha_inicio=inicio_mes.strftime("%Y-%m-%d"),
                        fecha_fin=fin_mes.strftime("%Y-%m-%d"),
                    )
                    # Actualizar el set de descargados
                    ya_descargados.update(dias_faltantes)
                except Exception as e:
                    print(f"   ❌ Error descargando {inicio_mes.strftime('%Y-%m')}: {e}")
                    print(f"   (continuamos con el siguiente mes)")

            # Avanzar al siguiente mes
            mes_actual = siguiente_mes

    # Resumen final
    total_final = len(listar_archivos_descargados())
    print(f"\n{'='*60}")
    print(f"   RESUMEN: {total_final} archivos .nc en total")
    print(f"{'='*60}")


# ============================================================
# BLOQUE PRINCIPAL
# ============================================================

if __name__ == "__main__":

    print("DESCARGA DE DATOS DE POLEN - COPERNICUS CAMS")
    print("=" * 60)

    # Descargar todas las temporadas (2023-2026)
    # Solo descarga los meses que faltan
    descargar_todas_temporadas()

    print("\n" + "=" * 60)
    print("PROCESO COMPLETADO")
    print("=" * 60)
    print(f"\nPara ver estos datos en QGIS:")
    print(f"   1. Abre QGIS")
    print(f"   2. Capa -> Añadir capa ráster")
    print(f"   3. Selecciona el archivo .nc descargado")
    print(f"   4. Elige la variable (olive_pollen o grass_pollen)")
