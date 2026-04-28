"""
Imputación de horas de sol para Córdoba.

Las estaciones auxiliares (Guadañuno y Prágdena) NO registran
horas de sol, así que no podemos usarlas para imputar.

ESTRATEGIA: media histórica del mismo día del año.
    Para cada fecha con horas_sol = NULL, calculamos la media
    de horas_sol del mismo día (mismo mes + mismo día) en todos
    los años donde SÍ hay dato.

    Ejemplo: si el 15-mar-2020 es NULL, calculamos:
        media(15-mar-2015, 15-mar-2016, 15-mar-2017, ...)
        (solo los años donde hay dato)

    Esto funciona bien porque las horas de sol dependen mucho
    de la época del año (astronomía solar), así que un 15 de marzo
    suele tener horas de sol parecidas de un año a otro.

Se ejecuta desde la RAÍZ del proyecto:
    python -m src.processing.clima_imputar_horas_sol
"""

import os
import psycopg2
from dotenv import load_dotenv


# ============================================================
# CONEXIÓN A BASE DE DATOS
# ============================================================

def conectar_db():
    """
    Crea conexión con PostgreSQL.
    Lee las credenciales del archivo .env en la raíz del proyecto.
    """
    # Forzar codificación UTF-8 para evitar problemas con tildes
    os.environ["PGCLIENTENCODING"] = "UTF8"

    # Navegar desde este archivo hasta la raíz del proyecto (3 niveles arriba)
    # src/processing/este_archivo.py → src/processing → src → raíz
    raiz = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
    # Cargar variables del .env
    load_dotenv(os.path.join(raiz, ".env"))

    # Crear conexión usando las variables de entorno
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


# ============================================================
# PASO 1: Diagnóstico — ver cuántos nulos hay y dónde
# ============================================================

def diagnosticar_nulos(conexion):
    """
    Muestra un resumen de los nulos de horas_sol en Córdoba,
    agrupados por año, para entender el alcance del problema.
    """

    print(f"\n{'='*60}")
    print("PASO 1: Diagnóstico de nulos de horas_sol en Córdoba")
    print(f"{'='*60}")

    cursor = conexion.cursor()

    # Contar nulos totales
    cursor.execute("""
        SELECT COUNT(*)
        FROM clima_diario
        WHERE codigo_ine = '14021'
          AND horas_sol IS NULL
    """)
    total_nulos = cursor.fetchone()[0]
    print(f"\n   📊 Total de nulos: {total_nulos}")

    if total_nulos == 0:
        print("   ✅ No hay nulos que imputar. ¡Datos completos!")
        cursor.close()
        return 0

    # Desglose por año para ver cuáles son los más afectados
    # EXTRACT(YEAR FROM fecha): extrae el año de una fecha
    # COUNT(*): cuenta cuántos registros hay en cada grupo
    cursor.execute("""
        SELECT
            EXTRACT(YEAR FROM fecha)::INTEGER AS anio,
            COUNT(*) AS nulos
        FROM clima_diario
        WHERE codigo_ine = '14021'
          AND horas_sol IS NULL
        GROUP BY anio
        ORDER BY anio
    """)

    print(f"\n   Desglose por año:")
    for anio, nulos in cursor.fetchall():
        print(f"      {anio}: {nulos} días sin dato")

    cursor.close()
    return total_nulos


# ============================================================
# PASO 2: Imputar usando media histórica del mismo día del año
# ============================================================

def imputar_horas_sol(conexion):
    """
    Imputa los valores nulos de horas_sol en Córdoba usando
    la media histórica del MISMO DÍA DEL AÑO.

    Lógica SQL:
    - Para cada registro con horas_sol = NULL en Córdoba (codigo_ine='14021')
    - Buscamos TODOS los registros de Córdoba del mismo día y mes
      que SÍ tengan dato (horas_sol IS NOT NULL)
    - Calculamos la media → AVG(horas_sol)
    - Redondeamos a 1 decimal → ROUND(..., 1)
    - Actualizamos el registro nulo con esa media

    Funciones SQL usadas:
    - EXTRACT(MONTH FROM fecha): extrae el mes (1-12)
    - EXTRACT(DAY FROM fecha): extrae el día (1-31)
    - AVG(): calcula la media aritmética
    - ROUND(valor, 1): redondea a 1 decimal
    - ::NUMERIC: convierte el tipo para que ROUND funcione
    """

    print(f"\n{'='*60}")
    print("PASO 2: Imputando horas de sol con media histórica")
    print(f"{'='*60}")

    cursor = conexion.cursor()

    # Contar nulos antes de imputar
    cursor.execute("""
        SELECT COUNT(*)
        FROM clima_diario
        WHERE codigo_ine = '14021'
          AND horas_sol IS NULL
    """)
    nulos_antes = cursor.fetchone()[0]
    print(f"\n   📊 Nulos ANTES de imputar: {nulos_antes}")

    if nulos_antes == 0:
        print("   ✅ No hay nulos que imputar")
        cursor.close()
        return

    # --- QUERY DE IMPUTACIÓN ---
    #
    # ¿Cómo funciona?
    # 1. La subconsulta "imputado" busca, para cada día nulo de Córdoba,
    #    la media de horas_sol de ese mismo día+mes en otros años
    # 2. El UPDATE aplica esa media al registro nulo
    #
    # Ejemplo visual:
    #    Fecha nula: 2020-03-15 (horas_sol = NULL)
    #    Datos disponibles del 15 de marzo:
    #       2015-03-15 → 7.2h
    #       2016-03-15 → 8.1h
    #       2017-03-15 → 6.9h
    #       2018-03-15 → 7.5h
    #    Media = (7.2 + 8.1 + 6.9 + 7.5) / 4 = 7.4h
    #    → Se imputa 7.4 en 2020-03-15
    cursor.execute("""
        UPDATE clima_diario AS target
        SET horas_sol = imputado.media_historica
        FROM (
            SELECT
                nulo.fecha,
                -- Calculamos la media del mismo día/mes en otros años
                ROUND(
                    AVG(historico.horas_sol)::NUMERIC,
                    1
                ) AS media_historica
            FROM clima_diario nulo
            -- INNER JOIN: cruzamos cada día nulo con todos los días
            -- del mismo mes+día que SÍ tienen dato
            INNER JOIN clima_diario historico
                ON historico.codigo_ine = '14021'
                AND historico.horas_sol IS NOT NULL
                -- Mismo mes (ej: ambos en marzo)
                AND EXTRACT(MONTH FROM historico.fecha)
                    = EXTRACT(MONTH FROM nulo.fecha)
                -- Mismo día del mes (ej: ambos día 15)
                AND EXTRACT(DAY FROM historico.fecha)
                    = EXTRACT(DAY FROM nulo.fecha)
            WHERE nulo.codigo_ine = '14021'
              AND nulo.horas_sol IS NULL
            -- Agrupamos por fecha nula para calcular UNA media por día
            GROUP BY nulo.fecha
        ) AS imputado
        WHERE target.codigo_ine = '14021'
          AND target.fecha = imputado.fecha
    """)

    # cursor.rowcount: número de filas actualizadas por el UPDATE
    imputados = cursor.rowcount
    conexion.commit()

    print(f"   ✅ Registros imputados: {imputados}")

    # --- VERIFICACIÓN ---
    # Comprobamos cuántos nulos quedan después
    cursor.execute("""
        SELECT COUNT(*)
        FROM clima_diario
        WHERE codigo_ine = '14021'
          AND horas_sol IS NULL
    """)
    nulos_despues = cursor.fetchone()[0]

    print(f"   📊 Nulos DESPUÉS de imputar: {nulos_despues}")

    # Si quedan nulos, es porque no hay datos históricos
    # de ese mismo día en ningún otro año
    if nulos_despues > 0:
        cursor.execute("""
            SELECT fecha
            FROM clima_diario
            WHERE codigo_ine = '14021'
              AND horas_sol IS NULL
            ORDER BY fecha
        """)
        fechas_sin_dato = [str(row[0]) for row in cursor.fetchall()]
        print(f"   ⚠️  Fechas sin imputar (no hay histórico para ese día):")
        for fecha in fechas_sin_dato:
            print(f"      - {fecha}")

    cursor.close()


# ============================================================
# PASO 3: Verificación — mostrar muestra de datos imputados
# ============================================================

def verificar_imputacion(conexion):
    """
    Muestra una muestra de los datos imputados para que el usuario
    pueda revisar si los valores son razonables.
    """

    print(f"\n{'='*60}")
    print("PASO 3: Verificación de la imputación")
    print(f"{'='*60}")

    cursor = conexion.cursor()

    # Estadísticas generales de horas_sol en Córdoba
    cursor.execute("""
        SELECT
            COUNT(*) AS total_registros,
            COUNT(horas_sol) AS con_dato,
            COUNT(*) - COUNT(horas_sol) AS sin_dato,
            ROUND(AVG(horas_sol)::NUMERIC, 1) AS media,
            ROUND(MIN(horas_sol)::NUMERIC, 1) AS minimo,
            ROUND(MAX(horas_sol)::NUMERIC, 1) AS maximo
        FROM clima_diario
        WHERE codigo_ine = '14021'
    """)
    stats = cursor.fetchone()

    print(f"\n   Estadísticas de horas_sol en Córdoba:")
    print(f"      Total registros: {stats[0]}")
    print(f"      Con dato:        {stats[1]}")
    print(f"      Sin dato (NULL): {stats[2]}")
    print(f"      Media:           {stats[3]} h")
    print(f"      Mínimo:          {stats[4]} h")
    print(f"      Máximo:          {stats[5]} h")

    # Resumen por año: total horas de sol acumuladas
    # Esto nos permite ver si los años imputados tienen valores coherentes
    cursor.execute("""
        SELECT
            EXTRACT(YEAR FROM fecha)::INTEGER AS anio,
            ROUND(SUM(horas_sol)::NUMERIC, 1) AS total_horas,
            COUNT(*) AS dias_con_dato
        FROM clima_diario
        WHERE codigo_ine = '14021'
          AND horas_sol IS NOT NULL
        GROUP BY anio
        ORDER BY anio
    """)

    print(f"\n   Acumulado anual de horas de sol:")
    for anio, total, dias in cursor.fetchall():
        print(f"      {anio}: {total} h ({dias} días con dato)")

    cursor.close()


# ============================================================
# BLOQUE PRINCIPAL
# ============================================================

if __name__ == "__main__":

    print("🔧 IMPUTACIÓN DE HORAS DE SOL - CÓRDOBA")
    print("   Método: media histórica del mismo día del año")
    print("=" * 60)

    # Conectar a PostgreSQL
    conexion = conectar_db()

    # Paso 1: Diagnóstico
    total_nulos = diagnosticar_nulos(conexion)

    if total_nulos > 0:
        # Paso 2: Imputar
        imputar_horas_sol(conexion)

        # Paso 3: Verificar
        verificar_imputacion(conexion)

    # Cerrar conexión
    conexion.close()

    print("\n" + "=" * 60)
    print("✅ PROCESO COMPLETADO")
    print("=" * 60)
