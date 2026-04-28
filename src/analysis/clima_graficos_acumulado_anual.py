"""
Gráficos de barras: Acumulado anual de precipitación y horas de sol en Córdoba.

2 gráficos:
1. Precipitación acumulada por año (2015-2026) con media y mediana
2. Horas de sol acumuladas por año (2015-2026) con media y mediana

Incluye análisis de datos ausentes y proyección para 2026.

Se ejecuta desde la RAÍZ del proyecto:
    python -m src.analysis.clima_graficos_acumulado_anual
"""

# --- IMPORTACIONES ---
import os
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime


def conectar_db():
    """Conecta con PostgreSQL usando SQLAlchemy."""

    os.environ["PGCLIENTENCODING"] = "UTF8"

    raiz = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
    load_dotenv(os.path.join(raiz, ".env"))

    db_url = (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

    engine = create_engine(db_url, client_encoding="UTF8")
    return engine


def obtener_datos_cordoba():
    """
    Descarga datos de Córdoba desde PostgreSQL.
    Incluye información de datos ausentes para el análisis de calidad.
    """

    engine = conectar_db()

    # Descargamos TODOS los campos que necesitamos
    # incluyendo fecha para poder contar días con/sin datos
    query = """
        SELECT
            c.fecha,
            EXTRACT(YEAR FROM c.fecha) AS anio,
            EXTRACT(MONTH FROM c.fecha) AS mes,
            EXTRACT(DOY FROM c.fecha) AS dia_del_anio,
            c.precipitacion,
            c.horas_sol
        FROM clima_diario c
        JOIN municipios m ON c.codigo_ine = m.codigo_ine
        WHERE m.nombre = 'Córdoba'
        ORDER BY c.fecha;
    """

    df = pd.read_sql(query, engine)

    # Convertir tipos
    df["anio"] = df["anio"].astype(int)
    df["mes"] = df["mes"].astype(int)
    df["dia_del_anio"] = df["dia_del_anio"].astype(int)

    print(f"✅ Obtenidos {len(df)} registros de Córdoba")
    print(f"   Periodo: {df['fecha'].min()} → {df['fecha'].max()}")

    return df


def analizar_datos_ausentes(df):
    """
    Analiza y reporta datos ausentes en precipitación y horas de sol.
    Es importante saber cuántos datos faltan para interpretar los acumulados.

    Args:
        df: DataFrame con los datos de Córdoba

    Returns:
        DataFrame con el resumen de datos ausentes por año
    """

    print(f"\n{'='*60}")
    print(f"ANÁLISIS DE DATOS AUSENTES")
    print(f"{'='*60}")

    # Agrupar por año y contar nulos
    resumen = df.groupby("anio").agg(
        total_dias=("fecha", "count"),
        # .isna() devuelve True donde hay nulo
        # .sum() cuenta los True
        precip_nulos=("precipitacion", lambda x: x.isna().sum()),
        sol_nulos=("horas_sol", lambda x: x.isna().sum()),
    ).reset_index()

    # Calcular porcentaje de datos válidos
    # (100% = tenemos todos los datos del año)
    resumen["precip_validos_pct"] = round(
        (1 - resumen["precip_nulos"] / resumen["total_dias"]) * 100, 1
    )
    resumen["sol_validos_pct"] = round(
        (1 - resumen["sol_nulos"] / resumen["total_dias"]) * 100, 1
    )

    # Mostrar resumen por consola
    for _, row in resumen.iterrows():
        anio = int(row["anio"])
        # Emoji según calidad de datos
        emoji_p = "✅" if row["precip_validos_pct"] >= 90 else "⚠️" if row["precip_validos_pct"] >= 70 else "❌"
        emoji_s = "✅" if row["sol_validos_pct"] >= 90 else "⚠️" if row["sol_validos_pct"] >= 70 else "❌"

        print(
            f"   {anio}: "
            f"Precip {emoji_p} {row['precip_validos_pct']}% válidos ({int(row['precip_nulos'])} nulos) | "
            f"Sol {emoji_s} {row['sol_validos_pct']}% válidos ({int(row['sol_nulos'])} nulos)"
        )

    return resumen


def calcular_acumulados(df):
    """
    Calcula los acumulados anuales de precipitación y horas de sol.

    Para 2026 (año incompleto), también calcula una proyección
    a final de año basada en el ritmo actual.

    Args:
        df: DataFrame con datos diarios de Córdoba

    Returns:
        DataFrame con acumulados y proyecciones por año
    """

    # --- ACUMULADOS POR AÑO ---
    # Solo sumamos donde hay datos (los NaN se ignoran con sum())
    acumulado = df.groupby("anio").agg(
        precip_total=("precipitacion", "sum"),
        sol_total=("horas_sol", "sum"),
        dias_con_precip=("precipitacion", "count"),  # Días con dato (no nulo)
        dias_con_sol=("horas_sol", "count"),
        ultimo_dia=("dia_del_anio", "max"),           # Último día con datos
        ultima_fecha=("fecha", "max"),                 # Fecha real del último dato
    ).reset_index()

    # Redondear
    acumulado["precip_total"] = acumulado["precip_total"].round(1)
    acumulado["sol_total"] = acumulado["sol_total"].round(1)

    # --- PROYECCIÓN PARA 2026 ---
    # Calculamos cuánto llevaríamos a final de año al ritmo actual
    anio_actual = datetime.now().year
    mascara_actual = acumulado["anio"] == anio_actual

    if mascara_actual.any():
        row = acumulado.loc[mascara_actual].iloc[0]

        # Día del año actual (ej: 117 para el 27 de abril)
        dia_actual = row["ultimo_dia"]

        # Factor de escala: 365 / días transcurridos
        # Ejemplo: si llevamos 117 días → factor = 365/117 = 3.12
        factor = 365 / dia_actual if dia_actual > 0 else 1

        # Proyección = acumulado actual × factor
        acumulado.loc[mascara_actual, "precip_proyectada"] = round(
            row["precip_total"] * factor, 1
        )
        acumulado.loc[mascara_actual, "sol_proyectada"] = round(
            row["sol_total"] * factor, 1
        )

        print(f"\n📊 Proyección 2026 (basada en {int(dia_actual)} días):")
        print(f"   Precipitación: {row['precip_total']} mm → "
              f"proyección anual: {round(row['precip_total'] * factor, 1)} mm")
        print(f"   Horas de sol: {row['sol_total']} h → "
              f"proyección anual: {round(row['sol_total'] * factor, 1)} h")

    return acumulado


def obtener_carpeta_salida():
    """Devuelve la ruta a la carpeta de outputs."""
    raiz = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
    carpeta = os.path.join(raiz, "outputs", "cordoba")
    os.makedirs(carpeta, exist_ok=True)
    return carpeta


def grafico_precipitacion(acumulado):
    """
    GRÁFICO 1: Precipitación acumulada por año.

    Barras con colores que van de azul (más lluvia) a naranja (menos lluvia).
    Incluye líneas de media y mediana de los años COMPLETOS.
    Para 2026 muestra la barra real + la proyección en semitransparente.
    """

    print("\n📊 Generando Gráfico: Precipitación acumulada...")

    # --- SEPARAR AÑOS COMPLETOS Y 2026 ---
    anio_actual = datetime.now().year
    completos = acumulado[acumulado["anio"] < anio_actual].copy()
    parcial = acumulado[acumulado["anio"] == anio_actual].copy()

    # --- CALCULAR MEDIA Y MEDIANA (solo años completos) ---
    media_precip = round(completos["precip_total"].mean(), 1)
    mediana_precip = round(completos["precip_total"].median(), 1)

    print(f"   Media (2015-2025): {media_precip} mm")
    print(f"   Mediana (2015-2025): {mediana_precip} mm")

    # --- CREAR GRÁFICO ---
    fig = go.Figure()

    # Escala de colores: más lluvia → más azul, menos → más naranja
    max_precip = completos["precip_total"].max()
    min_precip = completos["precip_total"].min()

    # Colores para años completos
    colores = []
    for val in completos["precip_total"]:
        # Normalizar entre 0 y 1
        ratio = (val - min_precip) / (max_precip - min_precip) if max_precip > min_precip else 0.5
        # Interpolar entre naranja (#E67E22) y azul (#2980B9)
        r = int(230 - ratio * (230 - 41))
        g = int(126 - ratio * (126 - 128))
        b = int(34 + ratio * (185 - 34))
        colores.append(f"rgb({r},{g},{b})")

    # Barras de años completos (2015-2025)
    fig.add_trace(go.Bar(
        x=completos["anio"],
        y=completos["precip_total"],
        marker_color=colores,
        text=completos["precip_total"].apply(lambda x: f"{x:.0f} mm"),
        textposition="outside",
        textfont={"size": 12, "family": "Arial Black"},
        name="Acumulado anual",
        hovertemplate="Año: %{x}<br>Precipitación: %{y:.1f} mm<extra></extra>",
    ))

    # Barra de 2026 (parcial) — color más claro
    if not parcial.empty:
        # Obtener la fecha REAL del último dato (no la fecha de hoy)
        # porque AEMET publica con 2-3 días de retraso
        ultima_fecha_real = pd.to_datetime(parcial["ultima_fecha"].iloc[0])
        etiqueta_fecha = ultima_fecha_real.strftime("%d/%m")

        fig.add_trace(go.Bar(
            x=parcial["anio"],
            y=parcial["precip_total"],
            marker_color="rgba(52, 152, 219, 0.6)",
            marker_line_color="rgba(52, 152, 219, 1)",
            marker_line_width=2,
            text=parcial["precip_total"].apply(lambda x: f"{x:.0f} mm"),
            textposition="outside",
            textfont={"size": 12, "family": "Arial Black"},
            name=f"2026 (hasta {etiqueta_fecha})",
            hovertemplate="Año: %{x}<br>Precipitación acumulada: %{y:.1f} mm<extra></extra>",
        ))

    # --- LÍNEAS DE REFERENCIA ---
    # Media
    fig.add_hline(
        y=media_precip,
        line_dash="dash",
        line_color="#E74C3C",
        line_width=2,
        annotation_text=f"Media: {media_precip} mm",
        annotation_position="top right",
        annotation_font_color="#E74C3C",
        annotation_font_size=13,
    )

    # Mediana
    fig.add_hline(
        y=mediana_precip,
        line_dash="dot",
        line_color="#8E44AD",
        line_width=2,
        annotation_text=f"Mediana: {mediana_precip} mm",
        annotation_position="bottom right",
        annotation_font_color="#8E44AD",
        annotation_font_size=13,
    )

    # --- LAYOUT ---
    fig.update_layout(
        width=1200,
        height=700,
        plot_bgcolor="white",
        paper_bgcolor="white",
        title={
            "text": (
                "🌧️ Córdoba: Precipitación acumulada por año (2015-2026)<br>"
                "<sup>¿Cuánto llueve realmente cada año? | Fuente: AEMET</sup>"
            ),
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 22, "family": "Arial"},
        },
        xaxis_title={"text": "Año", "font": {"size": 16}},
        yaxis_title={"text": "Precipitación acumulada (mm)", "font": {"size": 16}},
        font={"family": "Arial", "size": 14},
        legend={
            "orientation": "h",
            "yanchor": "top",
            "y": -0.12,
            "xanchor": "center",
            "x": 0.5,
            "font": {"size": 12},
        },
        xaxis={"dtick": 1},
        yaxis={"gridcolor": "rgba(0,0,0,0.05)"},
        bargap=0.2,
        margin={"b": 120},
        showlegend=True,
    )

    # Firma
    fig.add_annotation(
        text="@sitexdatos | Datos: AEMET OpenData",
        xref="paper", yref="paper",
        x=0.5, y=-0.22,
        showarrow=False,
        font={"size": 12, "color": "gray"},
    )

    # --- GUARDAR ---
    carpeta = obtener_carpeta_salida()
    fig.write_html(os.path.join(carpeta, "04_precipitacion_anual.html"))
    fig.write_image(os.path.join(carpeta, "04_precipitacion_anual.png"), scale=2)
    print("   💾 04_precipitacion_anual.html + .png")

    fig.show()


def grafico_horas_sol(acumulado):
    """
    GRÁFICO 2: Horas de sol acumuladas por año.

    Mismo estilo que el de precipitación pero con colores cálidos
    (más sol → más amarillo/naranja).
    """

    print("\n📊 Generando Gráfico: Horas de sol acumuladas...")

    # --- SEPARAR AÑOS ---
    anio_actual = datetime.now().year
    completos = acumulado[acumulado["anio"] < anio_actual].copy()
    parcial = acumulado[acumulado["anio"] == anio_actual].copy()

    # --- MEDIA Y MEDIANA ---
    media_sol = round(completos["sol_total"].mean(), 1)
    mediana_sol = round(completos["sol_total"].median(), 1)

    print(f"   Media (2015-2025): {media_sol} h")
    print(f"   Mediana (2015-2025): {mediana_sol} h")

    # --- CREAR GRÁFICO ---
    fig = go.Figure()

    # Escala de colores: más sol → más amarillo, menos → más gris
    max_sol = completos["sol_total"].max()
    min_sol = completos["sol_total"].min()

    colores = []
    for val in completos["sol_total"]:
        ratio = (val - min_sol) / (max_sol - min_sol) if max_sol > min_sol else 0.5
        # Interpolar de gris azulado (#95A5A6) a naranja dorado (#F39C12)
        r = int(149 + ratio * (243 - 149))
        g = int(165 + ratio * (156 - 165))
        b = int(166 - ratio * (166 - 18))
        colores.append(f"rgb({r},{g},{b})")

    # Barras años completos
    fig.add_trace(go.Bar(
        x=completos["anio"],
        y=completos["sol_total"],
        marker_color=colores,
        text=completos["sol_total"].apply(lambda x: f"{x:.0f} h"),
        textposition="outside",
        textfont={"size": 12, "family": "Arial Black"},
        name="Acumulado anual",
        hovertemplate="Año: %{x}<br>Horas de sol: %{y:.1f} h<extra></extra>",
    ))

    # Barra 2026 parcial
    if not parcial.empty:
        # Fecha REAL del último dato (no la de hoy)
        ultima_fecha_real = pd.to_datetime(parcial["ultima_fecha"].iloc[0])
        etiqueta_fecha = ultima_fecha_real.strftime("%d/%m")

        fig.add_trace(go.Bar(
            x=parcial["anio"],
            y=parcial["sol_total"],
            marker_color="rgba(243, 156, 18, 0.6)",
            marker_line_color="rgba(243, 156, 18, 1)",
            marker_line_width=2,
            text=parcial["sol_total"].apply(lambda x: f"{x:.0f} h"),
            textposition="outside",
            textfont={"size": 12, "family": "Arial Black"},
            name=f"2026 (hasta {etiqueta_fecha})",
            hovertemplate="Año: %{x}<br>Horas de sol acumuladas: %{y:.1f} h<extra></extra>",
        ))

    # --- LÍNEAS DE REFERENCIA ---
    fig.add_hline(
        y=media_sol,
        line_dash="dash",
        line_color="#E74C3C",
        line_width=2,
        annotation_text=f"Media: {media_sol} h",
        annotation_position="top right",
        annotation_font_color="#E74C3C",
        annotation_font_size=13,
    )

    fig.add_hline(
        y=mediana_sol,
        line_dash="dot",
        line_color="#8E44AD",
        line_width=2,
        annotation_text=f"Mediana: {mediana_sol} h",
        annotation_position="bottom right",
        annotation_font_color="#8E44AD",
        annotation_font_size=13,
    )

    # --- LAYOUT ---
    fig.update_layout(
        width=1200,
        height=700,
        plot_bgcolor="white",
        paper_bgcolor="white",
        title={
            "text": (
                "☀️ Córdoba: Horas de sol acumuladas por año (2015-2026)<br>"
                "<sup>¿Cuánto sol recibe Córdoba cada año? | Fuente: AEMET</sup>"
            ),
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 22, "family": "Arial"},
        },
        xaxis_title={"text": "Año", "font": {"size": 16}},
        yaxis_title={"text": "Horas de sol acumuladas (h)", "font": {"size": 16}},
        font={"family": "Arial", "size": 14},
        legend={
            "orientation": "h",
            "yanchor": "top",
            "y": -0.12,
            "xanchor": "center",
            "x": 0.5,
            "font": {"size": 12},
        },
        xaxis={"dtick": 1},
        yaxis={"gridcolor": "rgba(0,0,0,0.05)"},
        bargap=0.2,
        margin={"b": 120},
        showlegend=True,
    )

    # Firma
    fig.add_annotation(
        text="@sitexdatos | Datos: AEMET OpenData",
        xref="paper", yref="paper",
        x=0.5, y=-0.22,
        showarrow=False,
        font={"size": 12, "color": "gray"},
    )

    # --- GUARDAR ---
    carpeta = obtener_carpeta_salida()
    fig.write_html(os.path.join(carpeta, "05_horas_sol_anual.html"))
    fig.write_image(os.path.join(carpeta, "05_horas_sol_anual.png"), scale=2)
    print("   💾 05_horas_sol_anual.html + .png")

    fig.show()


# ============================================================
# BLOQUE PRINCIPAL
# ============================================================
if __name__ == "__main__":

    print("📊 GRÁFICOS DE ACUMULADO ANUAL - CÓRDOBA")
    print("=" * 60)

    # 1. Obtener datos
    df = obtener_datos_cordoba()

    # 2. Análisis de datos ausentes
    resumen_ausentes = analizar_datos_ausentes(df)

    # 3. Calcular acumulados
    acumulado = calcular_acumulados(df)

    # 4. Mostrar tabla resumen
    print(f"\n📋 Acumulados por año:")
    print(acumulado[["anio", "precip_total", "sol_total", "dias_con_precip", "dias_con_sol"]].to_string(index=False))

    # 5. Generar gráficos
    grafico_precipitacion(acumulado)
    grafico_horas_sol(acumulado)

    print("\n" + "=" * 60)
    print("✅ ¡2 gráficos generados!")
    print("📁 Revisa outputs/cordoba/")
    print("=" * 60)
