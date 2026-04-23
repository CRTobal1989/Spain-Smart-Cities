"""
Re-descarga SOLO los meses que faltan.
Lee el resultado de verificar_datos y reintenta las descargas fallidas.
"""

import json
import os
import time
from datetime import datetime, timedelta

from aemet_client_26_04_22 import AemetClient


def encontrar_meses_faltantes(ciudad: str, carpeta: str) -> list:
    """
    Lee el JSON de una ciudad y devuelve los meses que faltan.

    Args:
        ciudad: Nombre de la ciudad
        carpeta: Ruta a la carpeta con los JSON

    Returns:
        Lista de tuplas (fecha_inicio, fecha_fin) de cada mes faltante
    """
    import pandas as pd

    # Leer archivo existente
    archivo = os.path.join(carpeta, f"aemet_{ciudad}_historico.json")

    # Si no existe el archivo, necesitamos todos los meses
    if not os.path.exists(archivo):
        print(f"   ❌ No existe archivo para {ciudad}")
        return []

    with open(archivo, "r", encoding="utf-8") as f:
        datos = json.load(f)

    df = pd.DataFrame(datos)
    df["fecha"] = pd.to_datetime(df["fecha"])

    # Obtener meses que YA tenemos
    df["anio_mes"] = df["fecha"].dt.to_period("M")
    meses_con_datos = set(df["anio_mes"])

    # Generar todos los meses que deberian existir
    todos_los_meses = pd.period_range(
        start="2015-01",
        end=datetime.now().strftime("%Y-%m"),
        freq="M"
    )

    # Encontrar los que faltan
    # Para cada mes faltante, calculamos fecha_inicio y fecha_fin
    meses_faltantes = []
    for mes in todos_los_meses:
        if mes not in meses_con_datos:
            # Primer dia del mes
            fecha_inicio = mes.start_time.strftime("%Y-%m-%d")

            # Ultimo dia del mes
            fecha_fin = mes.end_time.strftime("%Y-%m-%d")

            meses_faltantes.append((fecha_inicio, fecha_fin))

    return meses_faltantes


def rellenar_huecos():
    """
    Para cada ciudad, encuentra los meses faltantes,
    los re-descarga y actualiza el archivo JSON.
    """

    # Ruta a la carpeta de datos
    raiz = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
    carpeta = os.path.join(raiz, "data", "sample")

    # Crear cliente AEMET
    cliente = AemetClient()

    # Obtener lista de ciudades
    ciudades = list(AemetClient.ESTACIONES.keys())

    print(f"\n{'='*60}")
    print(f"RELLENANDO HUECOS EN DATOS")
    print(f"{'='*60}")

    # --- PROCESAR CADA CIUDAD ---
    for ciudad in ciudades:

        print(f"\n🏙️ {ciudad.upper()}")

        # Encontrar meses faltantes
        meses_faltantes = encontrar_meses_faltantes(ciudad, carpeta)

        if not meses_faltantes:
            print(f"   ✅ No faltan meses")
            continue

        print(f"   📋 Faltan {len(meses_faltantes)} meses. Reintentando...")

        # Leer datos existentes
        archivo = os.path.join(carpeta, f"aemet_{ciudad}_historico.json")
        with open(archivo, "r", encoding="utf-8") as f:
            datos_existentes = json.load(f)

        # Contador de nuevos registros
        nuevos = 0

        # --- DESCARGAR CADA MES FALTANTE ---
        for fecha_inicio, fecha_fin in meses_faltantes:

            try:
                datos_mes = cliente.get_datos_diarios(
                    fecha_inicio=fecha_inicio,
                    fecha_fin=fecha_fin,
                    ciudad=ciudad,
                )

                if datos_mes:
                    datos_existentes.extend(datos_mes)
                    nuevos += len(datos_mes)

            except Exception as e:
                print(f"   ❌ Error en {fecha_inicio}: {e}")

            # Pausa para no saturar la API
            time.sleep(1)

        # --- GUARDAR DATOS ACTUALIZADOS ---
        if nuevos > 0:
            # Ordenar por fecha antes de guardar
            datos_existentes.sort(key=lambda x: x.get("fecha", ""))

            with open(archivo, "w", encoding="utf-8") as f:
                json.dump(datos_existentes, f, ensure_ascii=False, indent=2)

            print(f"   ✅ Añadidos {nuevos} registros nuevos")
        else:
            print(f"   ⚠️ No se pudieron recuperar datos nuevos")

    # --- REGENERAR ARCHIVO COMBINADO ---
    print(f"\n📦 Regenerando archivo combinado...")
    todos = []
    for ciudad in ciudades:
        archivo = os.path.join(carpeta, f"aemet_{ciudad}_historico.json")
        if os.path.exists(archivo):
            with open(archivo, "r", encoding="utf-8") as f:
                todos.extend(json.load(f))

    archivo_total = os.path.join(carpeta, "aemet_todas_ciudades_historico.json")
    with open(archivo_total, "w", encoding="utf-8") as f:
        json.dump(todos, f, ensure_ascii=False, indent=2)

    print(f"   ✅ Archivo combinado: {len(todos):,} registros totales")


# --- BLOQUE PRINCIPAL ---
if __name__ == "__main__":
    rellenar_huecos()
