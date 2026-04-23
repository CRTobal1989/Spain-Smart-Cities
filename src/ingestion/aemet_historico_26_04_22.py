"""
Descarga masiva de datos históricos de AEMET.

Este script descarga datos climáticos mes a mes
desde 2015 hasta la actualidad, para TODAS las ciudades definidas en el archivo aemet_client_26_04_22.

AEMET tiene un límite de ~31 días por petición,
así que hacemos una petición por cada mes.
"""

# --- IMPORTACIONES ---
import json
import os
import time

# 'datetime' viene con Python. Sirve para manejar fechas.
# 'timedelta' permite sumar/restar días a una fecha.
from datetime import datetime, timedelta

# Importamos nuestro cliente de AEMET (el que ya creamos)
from aemet_client_26_04_22 import AemetClient


def descargar_historico(
    ciudad: str = "cordoba",
    anio_inicio: int = 2015,
    anio_fin: int = 2026,
) -> list:
    """
    Descarga datos históricos mes a mes para UNA ciudad.

    Args:
        ciudad: Nombre de la ciudad
        anio_inicio: Año desde el que empezar
        anio_fin: Año hasta el que descargar

    Returns:
        Lista con todos los registros descargados
    """

    # Crear el cliente de AEMET
    cliente = AemetClient()

    # Lista donde iremos guardando TODOS los datos
    todos_los_datos = []

    # Contador de errores
    errores = 0

    print(f"\n{'='*60}")
    print(f"DESCARGA HISTORICA: {ciudad.upper()}")
    print(f"Periodo: {anio_inicio} → {anio_fin}")
    print(f"{'='*60}\n")

    # --- BUCLE POR CADA AÑO ---
    # range(2015, 2027) genera: 2015, 2016, 2017, ..., 2026
    for anio in range(anio_inicio, anio_fin + 1):

        # --- BUCLE POR CADA MES ---
        # range(1, 13) genera: 1, 2, 3, ..., 12
        for mes in range(1, 13):

            # Calcular primer día del mes
            # f"{mes:02d}" convierte 1 → "01", 2 → "02", etc.
            fecha_inicio = f"{anio}-{mes:02d}-01"

            # Calcular último día del mes. Vamos al día 1 del mes siguiente y restamos 1 día
            if mes == 12:
                ultimo_dia = datetime(anio + 1, 1, 1) - timedelta(days=1)
            else:
                ultimo_dia = datetime(anio, mes + 1, 1) - timedelta(days=1)

            fecha_fin = ultimo_dia.strftime("%Y-%m-%d")

            # Si la fecha es futura, paramos
            if datetime(anio, mes, 1) > datetime.now():
                print(f"⏭️ {fecha_inicio} es futuro, paramos")
                break

            # --- DESCARGAR DATOS DEL MES ---
            try:
                datos_mes = cliente.get_datos_diarios(
                    fecha_inicio=fecha_inicio,
                    fecha_fin=fecha_fin,
                    ciudad=ciudad,
                )

                if datos_mes:
                    todos_los_datos.extend(datos_mes)

            except Exception as e:
                print(f"   ❌ Error: {e}")
                errores += 1

            # --- PAUSA ENTRE PETICIONES ---
            # Esperamos 1 segundo para no saturar la API
            time.sleep(1)

    # --- RESUMEN DE ESTA CIUDAD ---
    print(f"\n✅ {ciudad.upper()}: {len(todos_los_datos)} registros, {errores} errores")

    return todos_los_datos


def guardar_en_json(datos: list, nombre_archivo: str):
    """
    Guarda los datos descargados en un archivo JSON.

    Args:
        datos: Lista de diccionarios con los datos
        nombre_archivo: Nombre del archivo (sin extensión)
    """

    # Crear la carpeta si no existe
    carpeta = os.path.join("data", "sample")
    os.makedirs(carpeta, exist_ok=True)

    # Definir ruta completa del archivo
    archivo = os.path.join(carpeta, f"{nombre_archivo}.json")

    # Guardar datos en JSON
    with open(archivo, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)

    # Calcular tamaño del archivo
    tamano_mb = os.path.getsize(archivo) / 1024 / 1024

    print(f"💾 Guardado en: {archivo} ({tamano_mb:.2f} MB)")


def descargar_todas_las_ciudades(anio_inicio: int = 2015, anio_fin: int = 2026):
    """
    Descarga datos históricos de TODAS las ciudades definidas en AemetClient.

    Recorre el diccionario ESTACIONES del cliente y descarga una por una.
    Guarda un archivo JSON individual por ciudad y uno combinado con todo.

    Args:
        anio_inicio: Año desde el que empezar
        anio_fin: Año hasta el que descargar
    """

    # --- ACCEDER AL DICCIONARIO DE ESTACIONES ---
    # AemetClient.ESTACIONES accede a la variable de clase sin crear un objeto
    # .keys() devuelve las claves del diccionario: ["cordoba", "sevilla", ...]
    ciudades = list(AemetClient.ESTACIONES.keys())

    print(f"\n{'#'*60}")
    print(f"DESCARGA MASIVA DE DATOS AEMET")
    print(f"Ciudades: {', '.join(ciudades)}")
    print(f"Periodo: {anio_inicio} → {anio_fin}")
    print(f"{'#'*60}")

    # Lista para guardar TODOS los datos de TODAS las ciudades
    datos_totales = []

    # Diccionario para resumen final
    # {ciudad: numero_de_registros}
    resumen = {}

    # --- BUCLE POR CADA CIUDAD ---
    for i, ciudad in enumerate(ciudades):

        # enumerate() da el índice (i) y el valor (ciudad)
        # i=0, ciudad="cordoba" → i=1, ciudad="sevilla" → ...
        print(f"\n🏙️ [{i+1}/{len(ciudades)}] Descargando: {ciudad.upper()}")

        # Descargar datos de esta ciudad
        datos_ciudad = descargar_historico(
            ciudad=ciudad,
            anio_inicio=anio_inicio,
            anio_fin=anio_fin,
        )

        # Guardar archivo individual de esta ciudad
        if datos_ciudad:
            guardar_en_json(datos_ciudad, f"aemet_{ciudad}_historico")

            # Añadir a la lista total
            datos_totales.extend(datos_ciudad)

            # Guardar en resumen
            resumen[ciudad] = len(datos_ciudad)
        else:
            resumen[ciudad] = 0

    # --- GUARDAR ARCHIVO COMBINADO ---
    # Un solo archivo con TODAS las ciudades juntas
    if datos_totales:
        guardar_en_json(datos_totales, "aemet_todas_ciudades_historico")

    # --- RESUMEN FINAL ---
    print(f"\n{'#'*60}")
    print(f"RESUMEN FINAL")
    print(f"{'#'*60}")

    # Mostrar registros por ciudad
    for ciudad, registros in resumen.items():
        # Emoji según si descargó datos o no
        estado = "✅" if registros > 0 else "❌"
        print(f"  {estado} {ciudad.upper():15s} → {registros:,} registros")

    print(f"\n  📊 TOTAL: {len(datos_totales):,} registros")
    print(f"{'#'*60}")

    return datos_totales


# --- BLOQUE PRINCIPAL ---
if __name__ == "__main__":

    # Descargar TODAS las ciudades desde 2015
    datos = descargar_todas_las_ciudades(
        anio_inicio=2015,
        anio_fin=2026,
    )

    # Mostrar muestra con pandas
    if datos:
        import pandas as pd

        df = pd.DataFrame(datos)

        # Seleccionar columnas principales
        columnas = ["fecha", "nombre", "tmed", "tmax", "tmin", "prec", "sol"]
        columnas_existentes = []
        for col in columnas:
            if col in df.columns:
                columnas_existentes.append(col)

        df = df[columnas_existentes]

        # Renombrar columnas con unidades de medida
        df = df.rename(columns={
            "fecha": "Fecha",
            "nombre": "Estacion",
            "tmed": "Temp Media (C)",
            "tmax": "Temp Max (C)",
            "tmin": "Temp Min (C)",
            "prec": "Precipitacion (mm)",
            "sol": "Horas de Sol (h)",
        })

        print(f"\nPrimeros 5 registros:")
        print(df.head().to_string())

        print(f"\nUltimos 5 registros:")
        print(df.tail().to_string())

