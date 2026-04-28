"""
Carga los datos climáticos de los archivos JSON en PostgreSQL.

Lee cada archivo JSON de ciudad, transforma los datos
y los inserta en la tabla clima_diario.
"""

# --- IMPORTACIONES ---
import json
import os

# psycopg2 es el "puente" entre Python y PostgreSQL
# Nos permite enviar comandos SQL desde Python
import psycopg2

# dotenv para leer las variables del archivo .env
from dotenv import load_dotenv

# pandas para manejar los datos como tabla
import pandas as pd

# Cargar variables de entorno
load_dotenv()

def conectar_db():
    """
    Crea una conexión con la base de datos PostgreSQL.

    Usa psycopg2 con codificación UTF-8 forzada
    y lee las variables del archivo .env
    """

    # Forzamos la codificación ANTES de conectar
    # Esto soluciona el error de caracteres especiales en Windows
    os.environ["PGCLIENTENCODING"] = "UTF8"

    # Construir la ruta al archivo .env desde la raíz del proyecto
    raiz = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
    env_path = os.path.join(raiz, ".env")
    load_dotenv(env_path)

    # Conectar usando las variables del .env
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
    Limpia un valor numérico que viene de AEMET.

    AEMET usa coma como separador decimal (español): "12,5"
    PostgreSQL necesita punto decimal (inglés): "12.5"

    También maneja valores vacíos o con texto como "Ip"
    (Ip = precipitación inapreciable en AEMET)

    Args:
        valor: Texto con el número ("12,5", "Ip", "", etc.)

    Returns:
        float o None si no es un número válido
    """

    # Si el valor es None o está vacío, devolvemos None
    # None en SQL se convierte en NULL (valor vacío)
    if valor is None or valor == "":
        return None

    # Si es "Ip" (precipitación inapreciable), lo tratamos como 0
    if valor == "Ip":
        return 0.0

    # Intentamos convertir a número
    try:
        # Reemplazar coma por punto y convertir a float
        # float() convierte texto a número decimal
        return float(valor.replace(",", "."))
    except (ValueError, AttributeError):
        # Si falla la conversión, devolvemos None
        return None


def cargar_ciudad(conexion, ciudad: str, codigo_ine: str, carpeta: str):
    """
    Carga los datos de UNA ciudad en PostgreSQL.

    Args:
        conexion: Conexión a PostgreSQL
        ciudad: Nombre de la ciudad (para el archivo JSON)
        codigo_ine: Código INE del municipio
        carpeta: Ruta a la carpeta con los JSON
    """

    # Construir ruta al archivo JSON
    archivo = os.path.join(carpeta, f"aemet_{ciudad}_historico.json")

    # Verificar que el archivo existe
    if not os.path.exists(archivo):
        print(f"   ❌ No existe: {archivo}")
        return 0

    # Leer el archivo JSON
    with open(archivo, "r", encoding="utf-8") as f:
        datos = json.load(f)

    print(f"   📂 Leídos {len(datos)} registros del JSON")

    # --- CURSOR ---
    # El cursor es como un "bolígrafo" para escribir SQL
    # Creamos uno a partir de la conexión
    cursor = conexion.cursor()

    # Contador de registros insertados
    insertados = 0
    duplicados = 0

    # --- INSERTAR CADA REGISTRO ---
    for registro in datos:

        # Extraer y limpiar cada campo del registro
        fecha = registro.get("fecha")
        temp_media = limpiar_valor(registro.get("tmed"))
        temp_max = limpiar_valor(registro.get("tmax"))
        temp_min = limpiar_valor(registro.get("tmin"))
        precipitacion = limpiar_valor(registro.get("prec"))
        horas_sol = limpiar_valor(registro.get("sol"))
        vel_viento = limpiar_valor(registro.get("velmedia"))
        racha_viento = limpiar_valor(registro.get("racha"))
        presion_max = limpiar_valor(registro.get("presMax"))
        presion_min = limpiar_valor(registro.get("presMin"))
        humedad_media = limpiar_valor(registro.get("hrMedia"))
        humedad_max = limpiar_valor(registro.get("hrMax"))
        humedad_min = limpiar_valor(registro.get("hrMin"))

        # Si no hay fecha, saltamos este registro
        if not fecha:
            continue

        # --- INSERTAR EN SQL ---
        # %s son "placeholders" (huecos) que se rellenan con los valores
        # ON CONFLICT = si ya existe un registro con esa fecha y ciudad
        # DO NOTHING = no hacer nada (ignorar el duplicado)
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
                    temp_media, temp_max, temp_min,
                    precipitacion, horas_sol,
                    vel_viento, racha_viento,
                    presion_max, presion_min,
                    humedad_media, humedad_max, humedad_min,
                ),
            )
            insertados += 1

        except Exception as e:
            duplicados += 1

    # --- CONFIRMAR LOS CAMBIOS ---
    # commit() guarda todos los INSERT en la base de datos
    # Sin commit(), los datos se pierden al cerrar la conexión
    conexion.commit()

    # Cerrar el cursor
    cursor.close()

    print(f"   ✅ Insertados: {insertados} | Duplicados ignorados: {duplicados}")
    return insertados


def cargar_todas_las_ciudades():
    """
    Carga los datos de TODAS las ciudades en PostgreSQL.
    """

    # Mapa de ciudad → código INE
    # Necesario para vincular cada registro con su municipio
    ciudades = {
        "cordoba": "14021",
        "sevilla": "41091",
        "malaga": "29067",
        "madrid": "28079",
        "barcelona": "08019",
        "valencia": "46250",
        "granada": "18087",
    }

    # Ruta a la carpeta de datos
    raiz = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
    carpeta = os.path.join(raiz, "data", "sample")

    print(f"\n{'='*60}")
    print(f"CARGA DE DATOS EN POSTGRESQL")
    print(f"{'='*60}")

    # Conectar a la base de datos
    conexion = conectar_db()

    # Contador total
    total = 0

    # Cargar cada ciudad
    for ciudad, codigo_ine in ciudades.items():
        print(f"\n🏙️ {ciudad.upper()} (INE: {codigo_ine})")
        insertados = cargar_ciudad(conexion, ciudad, codigo_ine, carpeta)
        total += insertados

    # Cerrar conexión
    conexion.close()

    print(f"\n{'='*60}")
    print(f"CARGA COMPLETADA")
    print(f"Total registros insertados: {total:,}")
    print(f"{'='*60}")


# --- BLOQUE PRINCIPAL ---
if __name__ == "__main__":
    cargar_todas_las_ciudades()
