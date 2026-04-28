"""
Actualiza los datos meteorológicos desde la última fecha hasta hoy.

Este script:
1. Consulta PostgreSQL para saber cuál es la última fecha con datos
2. Descarga datos nuevos de AEMET (desde esa fecha hasta hoy)
3. Actualiza los archivos JSON
4. Carga los datos nuevos en PostgreSQL

FECHA_INICIO se calcula AUTOMÁTICAMENTE:
    - Consulta la última fecha en clima_diario
    - Le resta 2 días de margen (por si AEMET publicó datos con retraso)
    - Así nunca tienes que tocar el código manualmente

Incluye las 7 ciudades principales + 2 estaciones auxiliares
(Guadañuno y Prágdena) para tener datos completos.

Se ejecuta desde la RAÍZ del proyecto:
    python -m src.ingestion.actualizar_datos_27_04_26
"""

import json
import os
import time
from datetime import datetime, timedelta

# psycopg2: conector Python ↔ PostgreSQL
import psycopg2
from dotenv import load_dotenv

# Importamos nuestro cliente AEMET
from src.ingestion.aemet_client_26_04_22 import AemetClient


# ============================================================
# FUNCIONES DE BASE DE DATOS
# ============================================================
# Las incluimos aquí directamente porque el archivo original
# tiene puntos en el nombre (26.04.23) y Python no puede
# importar módulos con puntos en el nombre.

def conectar_db():
    """Crea una conexión con PostgreSQL."""
    os.environ["PGCLIENTENCODING"] = "UTF8"

    raiz = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
    load_dotenv(os.path.join(raiz, ".env"))

    conexion = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        client_encoding="UTF8",
    )
    print("✅ Conectado a PostgreSQL")
    return conexion


def limpiar_valor(valor: str):
    """
    Limpia un valor numérico de AEMET.
    Convierte comas a puntos, maneja 'Ip' y valores vacíos.
    """
    if valor is None or valor == "":
        return None
    if valor == "Ip":
        return 0.0
    try:
        return float(str(valor).replace(",", "."))
    except (ValueError, AttributeError):
        return None


# ============================================================
# CONFIGURACIÓN
# ============================================================

# Fecha hasta la que descargar (hoy)
FECHA_FIN = datetime.now().strftime("%Y-%m-%d")

# Mapa COMPLETO: ciudades principales + estaciones auxiliares
# Cada entrada tiene:
#   - codigo_ine: código para PostgreSQL
#   - nombre_aemet: nombre que usa nuestro AemetClient (clave del diccionario ESTACIONES)
CIUDADES = {
    "cordoba":   {"codigo_ine": "14021", "nombre_aemet": "cordoba"},
    "sevilla":   {"codigo_ine": "41091", "nombre_aemet": "sevilla"},
    "malaga":    {"codigo_ine": "29067", "nombre_aemet": "malaga"},
    "madrid":    {"codigo_ine": "28079", "nombre_aemet": "madrid"},
    "barcelona": {"codigo_ine": "08019", "nombre_aemet": "barcelona"},
    "valencia":  {"codigo_ine": "46250", "nombre_aemet": "valencia"},
    "granada":   {"codigo_ine": "18087", "nombre_aemet": "granada"},
    # Estaciones auxiliares (provincia de Córdoba)
    # No tienen código INE real, usamos códigos cortos de 5 caracteres
    "guadaluno": {"codigo_ine": "A5394", "nombre_aemet": "guadaluno"},
    "pragdena":  {"codigo_ine": "A5429", "nombre_aemet": "pragdena"},
}


def obtener_fecha_inicio(conexion):
    """
    Calcula FECHA_INICIO automáticamente consultando PostgreSQL.

    Lógica:
    1. Busca la fecha MÁS RECIENTE en clima_diario (MAX(fecha))
    2. Le resta 2 días de margen de seguridad
       (AEMET a veces publica datos con 1-2 días de retraso)
    3. Si no hay datos en la tabla, usa '2015-01-01' como fecha por defecto

    Returns:
        str con la fecha en formato 'YYYY-MM-DD'
    """
    cursor = conexion.cursor()

    # MAX(fecha): devuelve la fecha más reciente de toda la tabla
    cursor.execute("""
        SELECT MAX(fecha)
        FROM clima_diario
    """)
    ultima_fecha = cursor.fetchone()[0]
    cursor.close()

    if ultima_fecha:
        # Restamos 2 días como margen de seguridad
        # timedelta(days=2): representa una duración de 2 días
        # fecha - timedelta = fecha 2 días antes
        fecha_inicio = ultima_fecha - timedelta(days=2)
        print(f"   📅 Última fecha en BD: {ultima_fecha}")
        print(f"   📅 Descargando desde:  {fecha_inicio} (2 días de margen)")
    else:
        # Si la tabla está vacía, empezamos desde 2015
        fecha_inicio = datetime(2015, 1, 1).date()
        print(f"   ⚠️ Tabla vacía. Descargando desde: {fecha_inicio}")

    # .strftime(): convierte fecha a texto con formato específico
    return fecha_inicio.strftime("%Y-%m-%d")


def registrar_estaciones_auxiliares(conexion):
    """
    Registra las estaciones auxiliares en la tabla municipios
    (si no existen ya). Necesario para que clima_diario acepte
    sus datos (por la clave foránea codigo_ine).

    ON CONFLICT DO NOTHING: si ya existen, no hace nada.
    """
    cursor = conexion.cursor()

    # Datos de las estaciones auxiliares para la tabla municipios
    auxiliares = [
        ("A5394", "Embalse de Guadañuno", "Córdoba", "Andalucía", "5394X", "Embalse de Guadañuno"),
        ("A5429", "Prágdena",             "Córdoba", "Andalucía", "5429X", "Prágdena"),
    ]

    for datos in auxiliares:
        cursor.execute(
            """
            INSERT INTO municipios (
                codigo_ine, nombre, provincia, comunidad,
                estacion_aemet, nombre_estacion
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (codigo_ine) DO NOTHING
            """,
            datos,
        )

    conexion.commit()
    cursor.close()
    print("   ✅ Estaciones auxiliares registradas en municipios")


def actualizar_json(ciudad: str, datos_nuevos: list, carpeta: str):
    """
    Añade los datos nuevos al JSON existente de una ciudad,
    evitando duplicados por fecha.

    Args:
        ciudad: Nombre de la ciudad
        datos_nuevos: Lista de registros nuevos de AEMET
        carpeta: Ruta a la carpeta data/sample
    """
    archivo = os.path.join(carpeta, f"aemet_{ciudad}_historico.json")

    # Leer datos existentes
    if os.path.exists(archivo):
        with open(archivo, "r", encoding="utf-8") as f:
            datos_existentes = json.load(f)
    else:
        datos_existentes = []

    # Crear un set con las fechas que ya tenemos (para evitar duplicados)
    # Un set es como una lista pero sin elementos repetidos, y buscar
    # en un set es muchísimo más rápido que buscar en una lista
    fechas_existentes = {reg.get("fecha") for reg in datos_existentes}

    # Añadir solo los registros cuya fecha NO exista ya
    nuevos_sin_duplicados = [
        reg for reg in datos_nuevos
        if reg.get("fecha") not in fechas_existentes
    ]

    if nuevos_sin_duplicados:
        datos_existentes.extend(nuevos_sin_duplicados)

        # Ordenar por fecha
        datos_existentes.sort(key=lambda x: x.get("fecha", ""))

        # Guardar
        with open(archivo, "w", encoding="utf-8") as f:
            json.dump(datos_existentes, f, ensure_ascii=False, indent=2)

        print(f"   💾 JSON actualizado: +{len(nuevos_sin_duplicados)} registros nuevos")
    else:
        print(f"   ℹ️ No hay registros nuevos para el JSON")


def cargar_en_postgresql(datos_nuevos: list, codigo_ine: str, conexion):
    """
    Carga los datos nuevos en PostgreSQL.
    Usa ON CONFLICT para ignorar duplicados automáticamente.

    Args:
        datos_nuevos: Lista de registros de AEMET
        codigo_ine: Código INE del municipio
        conexion: Conexión a PostgreSQL
    """
    cursor = conexion.cursor()
    insertados = 0

    for registro in datos_nuevos:
        fecha = registro.get("fecha")
        if not fecha:
            continue

        try:
            cursor.execute(
                """
                INSERT INTO clima_diario (
                    codigo_ine, fecha,
                    temp_media, temp_max, temp_min,
                    precipitacion, horas_sol,
                    vel_viento, racha_viento,
                    presion_max, presion_min,
                    humedad_media, humedad_max, humedad_min
                ) VALUES (
                    %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (codigo_ine, fecha) DO NOTHING
                """,
                (
                    codigo_ine, fecha,
                    limpiar_valor(registro.get("tmed")),
                    limpiar_valor(registro.get("tmax")),
                    limpiar_valor(registro.get("tmin")),
                    limpiar_valor(registro.get("prec")),
                    limpiar_valor(registro.get("sol")),
                    limpiar_valor(registro.get("velmedia")),
                    limpiar_valor(registro.get("racha")),
                    limpiar_valor(registro.get("presMax")),
                    limpiar_valor(registro.get("presMin")),
                    limpiar_valor(registro.get("hrMedia")),
                    limpiar_valor(registro.get("hrMax")),
                    limpiar_valor(registro.get("hrMin")),
                ),
            )
            insertados += 1
        except Exception as e:
            pass  # Duplicados se ignoran silenciosamente

    conexion.commit()
    cursor.close()

    print(f"   🐘 PostgreSQL: {insertados} registros procesados")


def ejecutar_actualizacion():
    """
    Función principal: descarga datos nuevos de todas las ciudades
    (incluidas estaciones auxiliares), actualiza los JSON y carga
    en PostgreSQL.

    Todo es automático: no hay que cambiar ninguna fecha manualmente.
    """

    # Conectar a PostgreSQL
    conexion = conectar_db()

    # Registrar estaciones auxiliares en municipios (si no existen)
    registrar_estaciones_auxiliares(conexion)

    # Calcular FECHA_INICIO automáticamente desde la BD
    fecha_inicio = obtener_fecha_inicio(conexion)

    print(f"\n{'='*60}")
    print(f"ACTUALIZACIÓN DE DATOS METEOROLÓGICOS")
    print(f"Periodo: {fecha_inicio} → {FECHA_FIN}")
    print(f"Ciudades: {len(CIUDADES)} (7 principales + 2 auxiliares)")
    print(f"{'='*60}")

    # Crear cliente AEMET
    cliente = AemetClient()

    # Ruta a la carpeta de datos
    raiz = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
    carpeta = os.path.join(raiz, "data", "sample")

    # --- PROCESAR CADA CIUDAD / ESTACIÓN ---
    for ciudad, info in CIUDADES.items():
        codigo_ine = info["codigo_ine"]
        nombre_aemet = info["nombre_aemet"]

        # Emoji diferente para estaciones auxiliares
        emoji = "🏔️" if codigo_ine.startswith("A") else "🏙️"
        print(f"\n{emoji} {ciudad.upper()} (INE: {codigo_ine})")

        try:
            # Paso 1: Descargar datos nuevos de AEMET
            datos_nuevos = cliente.get_datos_diarios(
                fecha_inicio=fecha_inicio,
                fecha_fin=FECHA_FIN,
                ciudad=nombre_aemet,
            )

            if datos_nuevos:
                # Paso 2: Actualizar archivo JSON
                actualizar_json(ciudad, datos_nuevos, carpeta)

                # Paso 3: Cargar en PostgreSQL
                cargar_en_postgresql(datos_nuevos, codigo_ine, conexion)
            else:
                print(f"   ⚠️ No se obtuvieron datos nuevos")

        except Exception as e:
            print(f"   ❌ Error: {e}")

        # Pausa entre ciudades para no saturar la API
        time.sleep(1)

    # Cerrar conexión
    conexion.close()

    print(f"\n{'='*60}")
    print(f"✅ ACTUALIZACIÓN COMPLETADA")
    print(f"{'='*60}")


# --- BLOQUE PRINCIPAL ---
if __name__ == "__main__":
    ejecutar_actualizacion()
