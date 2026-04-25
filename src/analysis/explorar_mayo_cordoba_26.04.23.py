"""
Exploración y validación de datos de Mayo en Córdoba.

Antes de crear gráficos, necesitamos:
1. Ver qué datos tenemos
2. Detectar valores ausentes (NULL)
3. Detectar outliers (valores imposibles)
4. Verificar coherencia (temp_max > temp_min siempre)
5. Entender la distribución de los datos

"""

# --- IMPORTACIONES ---
import os

import psycopg2
import pandas as pd
from dotenv import load_dotenv


def conectar_db():
    """Conecta con PostgreSQL."""
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
    return conexion


def obtener_datos_mayo():
    """Descarga todos los datos de mayo de Córdoba."""

    conexion = conectar_db()

    query = """
        SELECT 
            c.fecha,
            EXTRACT(YEAR FROM c.fecha) AS anio,
            EXTRACT(DAY FROM c.fecha) AS dia,
            c.temp_max,
            c.temp_min,
            c.temp_media,
            c.precipitacion,
            c.horas_sol,
            c.humedad_media,
            c.vel_viento
        FROM clima_diario c
        JOIN municipios m ON c.codigo_ine = m.codigo_ine
        WHERE m.nombre = 'Córdoba'
          AND EXTRACT(MONTH FROM c.fecha) = 5
        ORDER BY c.fecha;
    """

    df = pd.read_sql(query, conexion)
    conexion.close()

    return df


def explorar_estructura(df):
    """
    PASO 1: Explorar la estructura básica de los datos.

    Preguntas que respondemos:
    - ¿Cuántas filas y columnas tenemos?
    - ¿Qué tipo de dato tiene cada columna?
    - ¿Cuántos años de datos tenemos?
    """

    print("=" * 60)
    print("PASO 1: ESTRUCTURA DE LOS DATOS")
    print("=" * 60)

    # .shape devuelve (filas, columnas)
    print(f"\n📐 Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas")

    # .dtypes muestra el tipo de dato de cada columna
    # int64 = número entero
    # float64 = número decimal
    # object = texto
    # datetime64 = fecha
    print(f"\n📋 Tipos de dato:")
    print(df.dtypes)

    # Años disponibles
    anios = sorted(df["anio"].unique())
    print(f"\n📅 Años disponibles ({len(anios)}): {anios}")

    # Registros por año
    # .value_counts() cuenta cuántas veces aparece cada valor
    # .sort_index() ordena por el índice (año)
    print(f"\n📅 Registros por año:")
    registros_anio = df["anio"].value_counts().sort_index()
    for anio, registros in registros_anio.items():
        # Mayo tiene 31 días, así que esperamos 31 registros
        estado = "✅" if registros >= 28 else "⚠️"
        print(f"   {estado} {int(anio)}: {registros} días (de 31)")


def explorar_valores_ausentes(df):
    """
    PASO 2: Detectar valores ausentes (NULL / NaN).

    NaN = Not a Number = Dato que falta
    Es importante saber cuántos faltan y DÓNDE faltan.
    """

    print("\n" + "=" * 60)
    print("PASO 2: VALORES AUSENTES")
    print("=" * 60)

    # .isnull() devuelve True/False para cada celda
    # .sum() cuenta los True (valores ausentes) por columna
    nulos = df.isnull().sum()

    # Total de filas
    total = len(df)

    print(f"\n📋 Valores ausentes por columna:")
    for columna, n_nulos in nulos.items():
        # Calcular porcentaje de ausentes
        porcentaje = (n_nulos / total) * 100

        # Emoji según gravedad
        if porcentaje == 0:
            estado = "✅"
        elif porcentaje < 5:
            estado = "⚠️"
        else:
            estado = "❌"

        print(f"   {estado} {columna:20s}: {n_nulos:4d} ausentes ({porcentaje:.1f}%)")

    # Detalle: ¿en qué años faltan datos?
    print(f"\n📋 Años con datos de temperatura máxima ausentes:")
    nulos_tmax = df[df["temp_max"].isnull()]
    if len(nulos_tmax) > 0:
        for anio in sorted(nulos_tmax["anio"].unique()):
            n = len(nulos_tmax[nulos_tmax["anio"] == anio])
            print(f"   ⚠️ {int(anio)}: {n} días sin temp_max")
    else:
        print("   ✅ No hay ausentes en temp_max")

    print(f"\n📋 Años con datos de temperatura mínima ausentes:")
    nulos_tmin = df[df["temp_min"].isnull()]
    if len(nulos_tmin) > 0:
        for anio in sorted(nulos_tmin["anio"].unique()):
            n = len(nulos_tmin[nulos_tmin["anio"] == anio])
            print(f"   ⚠️ {int(anio)}: {n} días sin temp_min")
    else:
        print("   ✅ No hay ausentes en temp_min")

    print(f"\n📋 Años con datos de precipitación ausentes:")
    nulos_prec = df[df["precipitacion"].isnull()]
    if len(nulos_prec) > 0:
        for anio in sorted(nulos_prec["anio"].unique()):
            n = len(nulos_prec[nulos_prec["anio"] == anio])
            print(f"   ⚠️ {int(anio)}: {n} días sin precipitación")
    else:
        print("   ✅ No hay ausentes en precipitación")


def explorar_estadisticas(df):
    """
    PASO 3: Estadísticas descriptivas.

    .describe() calcula automáticamente:
    - count = número de valores no nulos
    - mean = media (promedio)
    - std = desviación estándar (cuánto varían los datos)
    - min = valor mínimo
    - 25% = primer cuartil (25% de datos están por debajo)
    - 50% = mediana (valor central)
    - 75% = tercer cuartil (75% de datos están por debajo)
    - max = valor máximo
    """

    print("\n" + "=" * 60)
    print("PASO 3: ESTADÍSTICAS DESCRIPTIVAS")
    print("=" * 60)

    # Columnas numéricas que nos interesan
    columnas = ["temp_max", "temp_min", "precipitacion"]

    # .describe() genera el resumen estadístico
    # .round(1) redondea a 1 decimal
    print(f"\n📊 Resumen estadístico:")
    print(df[columnas].describe().round(1).to_string())

    # Estadísticas por año para ver tendencias
    print(f"\n📊 Temperatura máxima media por año:")
    stats_anio = df.groupby("anio").agg(
        tmax_media=("temp_max", "mean"),
        tmax_record=("temp_max", "max"),
        dias_sobre_35=("temp_max", lambda x: (x > 35).sum()),
        lluvia_total=("precipitacion", "sum"),
        dias_lluvia=("precipitacion", lambda x: (x > 0).sum()),
    ).round(1)

    for anio, row in stats_anio.iterrows():
        print(
            f"   {int(anio)}: "
            f"Media={row['tmax_media']}°C | "
            f"Récord={row['tmax_record']}°C | "
            f"Días>35°C={int(row['dias_sobre_35'])} | "
            f"Lluvia={row['lluvia_total']}mm ({int(row['dias_lluvia'])} días)"
        )


def detectar_outliers(df):
    """
    PASO 4: Detectar outliers (valores atípicos).

    Un outlier es un dato que está muy lejos del resto.
    Puede ser:
    - Un error del sensor (termómetro roto)
    - Un dato real pero extremo (ola de calor)

    Usamos dos métodos:
    1. Rango imposible: ¿tiene sentido físicamente?
    2. IQR (rango intercuartílico): ¿está muy lejos de la mayoría?
    """

    print("\n" + "=" * 60)
    print("PASO 4: DETECCIÓN DE OUTLIERS")
    print("=" * 60)

    # --- METODO 1: Rangos imposibles ---
    # Valores que NO tienen sentido físico para mayo en Córdoba
    print("\n🔍 Método 1: Rangos imposibles")

    # Temperatura máxima en mayo en Córdoba:
    # Razonable: entre 15°C y 48°C
    # Por debajo de 15°C o por encima de 48°C = sospechoso
    outliers_tmax = df[
        (df["temp_max"] < 15) | (df["temp_max"] > 48)
    ]
    if len(outliers_tmax) > 0:
        print(f"   ⚠️ Temp máxima fuera de rango [15-48°C]: {len(outliers_tmax)}")
        print(outliers_tmax[["fecha", "temp_max"]].to_string())
    else:
        print(f"   ✅ Temp máxima: todos los valores en rango [15-48°C]")

    # Temperatura mínima en mayo en Córdoba:
    # Razonable: entre 2°C y 30°C
    outliers_tmin = df[
        (df["temp_min"] < 2) | (df["temp_min"] > 30)
    ]
    if len(outliers_tmin) > 0:
        print(f"   ⚠️ Temp mínima fuera de rango [2-30°C]: {len(outliers_tmin)}")
        print(outliers_tmin[["fecha", "temp_min"]].to_string())
    else:
        print(f"   ✅ Temp mínima: todos los valores en rango [2-30°C]")

    # Precipitación: no puede ser negativa
    outliers_prec = df[df["precipitacion"] < 0]
    if len(outliers_prec) > 0:
        print(f"   ⚠️ Precipitación negativa: {len(outliers_prec)}")
    else:
        print(f"   ✅ Precipitación: sin valores negativos")

    # --- METODO 2: Coherencia entre variables ---
    print("\n🔍 Método 2: Coherencia entre variables")

    # temp_max SIEMPRE debe ser >= temp_min
    incoherentes = df[df["temp_max"] < df["temp_min"]]
    if len(incoherentes) > 0:
        print(f"   ❌ Días donde temp_max < temp_min: {len(incoherentes)}")
        print(incoherentes[["fecha", "temp_max", "temp_min"]].to_string())
    else:
        print(f"   ✅ Siempre temp_max >= temp_min")

    # temp_media debería estar entre temp_min y temp_max
    incoherentes_media = df[
        (df["temp_media"] < df["temp_min"]) |
        (df["temp_media"] > df["temp_max"])
    ]
    # Filtramos solo donde hay datos
    incoherentes_media = incoherentes_media.dropna(
        subset=["temp_media", "temp_min", "temp_max"]
    )
    if len(incoherentes_media) > 0:
        print(f"   ⚠️ Días donde temp_media fuera de [tmin, tmax]: {len(incoherentes_media)}")
    else:
        print(f"   ✅ temp_media siempre entre temp_min y temp_max")

    # --- METODO 3: IQR (Rango Intercuartílico) ---
    print("\n🔍 Método 3: IQR (Rango Intercuartílico)")
    print("   Valores que están muy lejos de la mayoría")

    for col in ["temp_max", "temp_min", "precipitacion"]:
        # Eliminar nulos para el cálculo
        datos = df[col].dropna()

        # Q1 = percentil 25 (25% de datos están por debajo)
        # Q3 = percentil 75 (75% de datos están por debajo)
        # IQR = Q3 - Q1 (rango donde está el 50% central)
        q1 = datos.quantile(0.25)
        q3 = datos.quantile(0.75)
        iqr = q3 - q1

        # Límites: valores fuera de [Q1 - 1.5*IQR, Q3 + 1.5*IQR]
        # son considerados outliers
        limite_inferior = q1 - 1.5 * iqr
        limite_superior = q3 + 1.5 * iqr

        outliers = datos[(datos < limite_inferior) | (datos > limite_superior)]

        if len(outliers) > 0:
            print(
                f"   ⚠️ {col}: {len(outliers)} outliers "
                f"(fuera de [{limite_inferior:.1f}, {limite_superior:.1f}])"
            )
        else:
            print(
                f"   ✅ {col}: sin outliers "
                f"(rango IQR: [{limite_inferior:.1f}, {limite_superior:.1f}])"
            )


def resumen_final(df):
    """
    PASO 5: Resumen final y decisiones.
    """

    print("\n" + "=" * 60)
    print("PASO 5: RESUMEN Y DECISIONES")
    print("=" * 60)

    total = len(df)
    completos = len(df.dropna(subset=["temp_max", "temp_min", "precipitacion"]))

    print(f"\n📊 Total registros: {total}")
    print(f"📊 Registros completos (temp + precip): {completos}")
    print(f"📊 Porcentaje utilizable: {completos/total*100:.1f}%")

    # Contar años con datos suficientes (>=20 días)
    dias_por_anio = df.dropna(subset=["temp_max"]).groupby("anio").size()
    anios_buenos = dias_por_anio[dias_por_anio >= 20]

    print(f"\n📅 Años con datos suficientes (>=20 días en mayo):")
    for anio, dias in anios_buenos.items():
        print(f"   ✅ {int(anio)}: {dias} días")

    anios_malos = dias_por_anio[dias_por_anio < 20]
    if len(anios_malos) > 0:
        print(f"\n📅 Años con datos INSUFICIENTES (<20 días):")
        for anio, dias in anios_malos.items():
            print(f"   ❌ {int(anio)}: {dias} días → EXCLUIR de gráficos")


# --- BLOQUE PRINCIPAL ---
if __name__ == "__main__":

    print("🔍 EXPLORACIÓN DE DATOS: MAYO EN CÓRDOBA")
    print("🔍 Antes de graficar, validamos los datos\n")

    # Obtener datos
    df = obtener_datos_mayo()
    print(f"\n✅ Descargados {len(df)} registros")

    # Ejecutar los 5 pasos de validación
    explorar_estructura(df)
    explorar_valores_ausentes(df)
    explorar_estadisticas(df)
    detectar_outliers(df)
    resumen_final(df)
