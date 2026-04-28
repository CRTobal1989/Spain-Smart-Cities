"""
Verifica qué datos tenemos y qué nos falta.
Identifica los huecos (días sin datos) por ciudad.
"""

import json
import os

import pandas as pd
from datetime import datetime, timedelta


def verificar_datos():
    """
    Lee el archivo JSON de cada ciudad y muestra
    qué meses tienen datos y cuáles no.
    """

    # __file__ = ruta de ESTE archivo Python
    # Subimos 3 niveles: src/analysis/ → src/ → raiz del proyecto
    # os.path.dirname() sube un nivel en la ruta
    raiz_proyecto = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )

    carpeta = os.path.join(raiz_proyecto, "data", "sample")

    # Verificar que la carpeta existe
    print(f"Buscando datos en: {carpeta}")

    # Buscar todos los archivos de ciudades individuales
    # os.listdir() lista todos los archivos de una carpeta
    # Filtramos solo los que empiezan por "aemet_" y terminan en "_historico.json"
    archivos = []
    for archivo in os.listdir(carpeta):
        if archivo.startswith("aemet_") and archivo.endswith("_historico.json"):
            if "todas" not in archivo:
                archivos.append(archivo)

    print(f"\n{'='*60}")
    print(f"VERIFICACION DE DATOS DESCARGADOS")
    print(f"{'='*60}")

    # Diccionario para guardar los meses que faltan por ciudad
    # Lo usaremos después para re-descargar
    meses_faltantes = {}

    # --- ANALIZAR CADA CIUDAD ---
    for archivo in sorted(archivos):

        # Extraer nombre de ciudad del nombre del archivo
        # "aemet_cordoba_historico.json" → "cordoba"
        ciudad = archivo.replace("aemet_", "").replace("_historico.json", "")

        # Leer el archivo JSON
        ruta = os.path.join(carpeta, archivo)
        with open(ruta, "r", encoding="utf-8") as f:
            datos = json.load(f)

        # Convertir a DataFrame de pandas
        df = pd.DataFrame(datos)

        # Convertir columna fecha a tipo fecha
        # pd.to_datetime() convierte texto "2024-01-01" a fecha
        df["fecha"] = pd.to_datetime(df["fecha"])

        # Obtener rango de fechas
        fecha_min = df["fecha"].min()
        fecha_max = df["fecha"].max()

        print(f"\n🏙️ {ciudad.upper()}")
        print(f"   Registros: {len(df):,}")
        print(f"   Desde: {fecha_min.strftime('%Y-%m-%d')}")
        print(f"   Hasta: {fecha_max.strftime('%Y-%m-%d')}")

        # --- BUSCAR MESES SIN DATOS ---
        # Agrupamos por año-mes y contamos registros
        # .dt.to_period('M') convierte fecha a periodo mensual
        df["anio_mes"] = df["fecha"].dt.to_period("M")
        datos_por_mes = df.groupby("anio_mes").size()

        # Generar todos los meses que deberian existir
        # pd.period_range genera una secuencia de meses
        todos_los_meses = pd.period_range(
            start="2015-01",
            end=datetime.now().strftime("%Y-%m"),
            freq="M"
        )

        # Encontrar meses que faltan
        meses_con_datos = set(datos_por_mes.index)
        faltantes = []
        pocos_datos = []

        for mes in todos_los_meses:
            if mes not in meses_con_datos:
                faltantes.append(mes)
            elif datos_por_mes[mes] < 28:
                # Meses con menos de 20 dias = incompletos
                pocos_datos.append(f"{mes} ({datos_por_mes[mes]} dias)")

        # Mostrar resultados
        if faltantes:
            print(f"   ❌ Meses SIN datos ({len(faltantes)}):")
            for mes in faltantes:
                print(f"      - {mes}")
        else:
            print(f"   ✅ Todos los meses tienen datos")

        if pocos_datos:
            print(f"   ⚠️ Meses INCOMPLETOS:")
            for mes in pocos_datos:
                print(f"      - {mes}")

        # Guardar meses faltantes para esta ciudad
        meses_faltantes[ciudad] = faltantes

    return meses_faltantes


# --- BLOQUE PRINCIPAL ---
if __name__ == "__main__":
    faltantes = verificar_datos()
