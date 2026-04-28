"""

3 gráficos:
1. Boxplot: ¿Cada vez más calor en los Mayos?
2. Termómetro de las fiestas (Cruces, Patios, Feria)
3. Lluvia vs Calor: ¿Mayo seco = Mayo caluroso?

"""

# --- IMPORTACIONES ---
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
from sqlalchemy import create_engine


def conectar_db():
    """
    Conecta con PostgreSQL usando SQLAlchemy.
    SQLAlchemy es la forma que pandas prefiere para conectar.
    """

    os.environ["PGCLIENTENCODING"] = "UTF8"

    # Buscar el .env en la raíz del proyecto
    raiz = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
    load_dotenv(os.path.join(raiz, ".env"))

    # Construir URL de conexión
    # Formato: postgresql://usuario:contraseña@host:puerto/basedatos
    db_url = (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

    # create_engine() crea el motor de conexión
    engine = create_engine(db_url, client_encoding="UTF8")
    return engine


def obtener_datos_mayo():
    """Descarga datos de mayo de Córdoba desde PostgreSQL."""

    engine = conectar_db()

    query = """
        SELECT 
            c.fecha,
            EXTRACT(YEAR FROM c.fecha) AS anio,
            EXTRACT(DAY FROM c.fecha) AS dia,
            c.temp_max,
            c.temp_min,
            c.temp_media,
            c.precipitacion,
            c.horas_sol
        FROM clima_diario c
        JOIN municipios m ON c.codigo_ine = m.codigo_ine
        WHERE m.nombre = 'Córdoba'
          AND EXTRACT(MONTH FROM c.fecha) = 5
          AND c.temp_max IS NOT NULL
        ORDER BY c.fecha;
    """

    df = pd.read_sql(query, engine)

    # Convertir año a entero (viene como float)
    df["anio"] = df["anio"].astype(int)
    df["dia"] = df["dia"].astype(int)

    print(f"✅ Obtenidos {len(df)} registros de mayo en Córdoba")
    return df


def obtener_carpeta_salida():
    """Devuelve la ruta a la carpeta de outputs."""

    raiz = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
    carpeta = os.path.join(raiz, "outputs", "cordoba")

    # Crear carpeta si no existe
    os.makedirs(carpeta, exist_ok=True)
    return carpeta


def grafico_1_evolucion_mayos(df):
    """
    GRÁFICO 1: Evolución de la temperatura en los Mayos de Córdoba.

    Línea temporal con:
    - Temperatura máxima media de cada mayo (línea principal)
    - Rango entre temp máxima absoluta y mínima absoluta (zona sombreada)
    - Línea de tendencia
    - Puntos con los valores de cada año

    Más atractivo para Instagram que un boxplot.
    """

    print("\n📊 Generando Gráfico 1: Evolución Mayos...")

    # --- PREPARAR DATOS ---
    # Agrupamos por año y calculamos métricas
    df_anual = df.groupby("anio").agg(
        tmax_media=("temp_max", "mean"),      # Media de máximas del mes
        tmax_record=("temp_max", "max"),       # Día más caluroso
        tmax_minima=("temp_max", "min"),       # Día más fresco
        dias_extremos=("temp_max", lambda x: int((x > 35).sum())),
    ).reset_index()

    # Redondear a 1 decimal
    df_anual = df_anual.round(1)

    # --- CREAR GRÁFICO ---
    fig = go.Figure()

    # Capa 1: Zona sombreada (rango entre récord y mínimo del mes)
    # Muestra la "amplitud" de cada mayo
    fig.add_trace(go.Scatter(
        x=df_anual["anio"],
        y=df_anual["tmax_record"],
        mode="lines",
        line={"width": 0},
        showlegend=False,
        hoverinfo="skip",
    ))

    fig.add_trace(go.Scatter(
        x=df_anual["anio"],
        y=df_anual["tmax_minima"],
        mode="lines",
        line={"width": 0},
        fill="tonexty",
        fillcolor="rgba(231, 76, 60, 0.15)",
        name="Rango del mes (máx - mín)",
    ))

    # Capa 2: Línea principal (temperatura media de máximas)
    fig.add_trace(go.Scatter(
        x=df_anual["anio"],
        y=df_anual["tmax_media"],
        mode="lines+markers+text",
        line={"color": "#E74C3C", "width": 3},
        marker={
            "size": 14,
            "color": "#E74C3C",
            "line": {"width": 2, "color": "white"},
        },
        # Mostrar el valor encima de cada punto
        text=df_anual["tmax_media"].apply(lambda x: f"{x}°C"),
        textposition="top center",
        textfont={"size": 13, "color": "#E74C3C", "family": "Arial Black"},
        name="Temp. Máxima media",
    ))

    # Capa 3: Línea de tendencia
    # numpy calcula la recta que mejor se ajusta a los datos
    import numpy as np

    # np.polyfit(x, y, 1) calcula una recta (grado 1)
    # Devuelve [pendiente, ordenada]
    z = np.polyfit(df_anual["anio"], df_anual["tmax_media"], 1)

    # np.poly1d() crea la función de la recta
    tendencia = np.poly1d(z)

    # Calcular valores de la línea de tendencia
    x_tendencia = df_anual["anio"]
    y_tendencia = tendencia(x_tendencia)

    # Determinar si sube o baja
    # z<a href="" class="citation-link" target="_blank" style="vertical-align: super; font-size: 0.8em; margin-left: 3px;">[0]</a> = pendiente: positiva = sube, negativa = baja
    cambio_por_decada = z[0] * 10
    direccion = "📈" if cambio_por_decada > 0 else "📉"

    fig.add_trace(go.Scatter(
        x=x_tendencia,
        y=y_tendencia,
        mode="lines",
        line={"color": "rgba(0,0,0,0.3)", "width": 2, "dash": "dash"},
        name=f"Tendencia ({direccion} {cambio_por_decada:+.1f}°C/década)",
    ))

    # --- PERSONALIZAR ---
    fig.update_layout(
        width=1200,
        height=700,
        plot_bgcolor="white",
        paper_bgcolor="white",

        title={
            "text": (
                "🌡️ Los Mayos de Córdoba: Temperatura máxima media (2015-2025) <br>"
                "<br>"
                "Fuente: AEMET</sup>"
            ),
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 24, "family": "Arial"},
        },

        xaxis_title={
            "text": "Año",
            "font": {"size": 16},
        },
        yaxis_title={
            "text": "Temperatura Máxima (°C)",
            "font": {"size": 16},
        },

        font={"family": "Arial", "size": 14},

        # Leyenda abajo centrada
        legend={
            "orientation": "h",
            "yanchor": "top",
            "y": -0.1,
            "xanchor": "center",
            "x": 0.5,
            "font": {"size": 12},
        },

        margin={"b": 120},
    )

    # Sin cuadrícula (como pediste)
    fig.update_xaxes(
        showgrid=False,
        dtick=1,
    )
    fig.update_yaxes(
        showgrid=False,
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

    fig.write_html(os.path.join(carpeta, "01_mayo_evolucion.html"))
    fig.write_image(os.path.join(carpeta, "01_mayo_evolucion.png"), scale=2)

    print("   💾 01_mayo_evolucion.html (interactivo)")
    print("   💾 01_mayo_evolucion.png (Instagram)")

    fig.show()


def grafico_2_fiestas(df):
    """
    GRÁFICO 2: Termómetro de las fiestas de Mayo.

    Muestra la temperatura media por día del mes,
    promediando todos los años, con franjas de color
    para cada fiesta:
      🔵 Cruces de Mayo (1-5)
      🔵 Patios de Córdoba (7-20)
      🔵 Feria de Córdoba (21-28)

    Incluye:
      - Rango histórico de temperaturas MÁXIMAS (zona roja)
      - Rango histórico de temperaturas MÍNIMAS (zona azul)
    """

    print("\n📊 Generando Gráfico 2: Termómetro de las fiestas...")

    # --- CALCULAR PROMEDIOS POR DÍA ---
    df_dia = df.groupby("dia").agg(
        temp_max_media=("temp_max", "mean"),
        temp_max_record=("temp_max", "max"),
        temp_max_minima=("temp_max", "min"),
        temp_min_media=("temp_min", "mean"),
        temp_min_record=("temp_min", "max"),  # Máxima de las mínimas
        temp_min_minima=("temp_min", "min"),  # Mínima de las mínimas (récord frío)
    ).reset_index()

    # --- CREAR GRÁFICO ---
    fig = go.Figure()

    # ══════════════════════════════════════════════
    # CAPA 1: Rango histórico MÁXIMAS (zona roja)
    # ══════════════════════════════════════════════
    fig.add_trace(go.Scatter(
        x=df_dia["dia"],
        y=df_dia["temp_max_record"],
        mode="lines",
        line={"width": 0},
        name="Récord máximo histórico",
        showlegend=False,
    ))

    fig.add_trace(go.Scatter(
        x=df_dia["dia"],
        y=df_dia["temp_max_minima"],
        mode="lines",
        line={"width": 0},
        fill="tonexty",
        fillcolor="rgba(231, 76, 60, 0.15)",  # Rojo suave
        name="Rango histórico máximas",
    ))

    # ══════════════════════════════════════════════
    # CAPA 2: Rango histórico MÍNIMAS (zona azul)
    # ══════════════════════════════════════════════
    fig.add_trace(go.Scatter(
        x=df_dia["dia"],
        y=df_dia["temp_min_record"],
        mode="lines",
        line={"width": 0},
        name="Récord mínimo (más cálido)",
        showlegend=False,
    ))

    fig.add_trace(go.Scatter(
        x=df_dia["dia"],
        y=df_dia["temp_min_minima"],
        mode="lines",
        line={"width": 0},
        fill="tonexty",
        fillcolor="rgba(52, 152, 219, 0.15)",  # Azul suave
        name="Rango histórico mínimas",
    ))

    # ══════════════════════════════════════════════
    # CAPA 3: Temperatura máxima media (línea roja)
    # ══════════════════════════════════════════════
    fig.add_trace(go.Scatter(
        x=df_dia["dia"],
        y=df_dia["temp_max_media"],
        mode="lines+markers",
        line={"color": "#E74C3C", "width": 3},
        marker={"size": 8, "color": "#E74C3C"},
        name="Temp. Máxima media",
    ))

    # ══════════════════════════════════════════════
    # CAPA 4: Temperatura mínima media (línea azul)
    # ══════════════════════════════════════════════
    fig.add_trace(go.Scatter(
        x=df_dia["dia"],
        y=df_dia["temp_min_media"],
        mode="lines+markers",
        line={"color": "#2980B9", "width": 2, "dash": "dot"},
        marker={"size": 6, "color": "#2980B9"},
        name="Temp. Mínima media",
    ))

    # ══════════════════════════════════════════════
    # FRANJAS DE FIESTAS — Tonalidades de AZUL
    # ══════════════════════════════════════════════

    # 🔵 Cruces de Mayo — Azul oscuro
    fig.add_vrect(
        x0=1, x1=5,
        fillcolor="rgba(31, 97, 141, 0.12)",
        line_width=1.5,
        line_color="rgba(31, 97, 141, 0.4)",
        annotation_text="<b>✝️ Cruces de Mayo</b>",
        annotation_position="top",
        annotation_font_size=12,
        annotation_font_color="#1F618D",
    )

    # 🔵 Patios de Córdoba — Azul medio
    fig.add_vrect(
        x0=7, x1=20,
        fillcolor="rgba(52, 152, 219, 0.10)",
        line_width=1.5,
        line_color="rgba(52, 152, 219, 0.4)",
        annotation_text="<b>🌺 Patios de Córdoba</b>",
        annotation_position="top",
        annotation_font_size=12,
        annotation_font_color="#2471A3",
    )

    # 🔵 Feria de Córdoba — Azul claro/celeste
    fig.add_vrect(
        x0=21, x1=28,
        fillcolor="rgba(133, 193, 233, 0.15)",
        line_width=1.5,
        line_color="rgba(133, 193, 233, 0.5)",
        annotation_text="<b>🎡 Feria</b>",
        annotation_position="top",
        annotation_font_size=12,
        annotation_font_color="#1A5276",
    )

    # --- PERSONALIZAR LAYOUT ---
    fig.update_layout(
        width=1200,
        height=700,
        plot_bgcolor="white",
        paper_bgcolor="white",
        title={
            "text": (
                "🌺 El termómetro de las fiestas de Mayo en Córdoba<br>"
                "<sup>Temperatura media por día (promedio 2015-2025) | "
                "Fuente: AEMET</sup>"
            ),
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 22},
        },
        xaxis_title="Días de mayo",
        yaxis_title="Temperatura (°C)",
        font={"family": "Arial", "size": 14},
        legend={
            "yanchor": "bottom",
            "y": 0.01,
            "xanchor": "right",
            "x": 0.99,
            "bgcolor": "rgba(255,255,255,0.8)",
            "bordercolor": "lightgray",
            "borderwidth": 1,
        },
        margin={"b": 80},
    )

    fig.update_yaxes(gridcolor="lightgray")
    fig.update_xaxes(
        gridcolor="lightgray",
        dtick=1,
        range=[0.5, 31.5],
    )

    # Firma
    fig.add_annotation(
        text="@sitexdatos | Datos: AEMET OpenData",
        xref="paper", yref="paper",
        x=0.5, y=-0.12,
        showarrow=False,
        font={"size": 11, "color": "gray"},
    )

    # --- GUARDAR ---
    carpeta = obtener_carpeta_salida()

    fig.write_html(os.path.join(carpeta, "02_mayo_fiestas.html"))
    fig.write_image(os.path.join(carpeta, "02_mayo_fiestas.png"), scale=2)

    print("   💾 02_mayo_fiestas.html (interactivo)")
    print("   💾 02_mayo_fiestas.png (Instagram)")

    fig.show()


def grafico_3_lluvia_vs_calor(df):
    """
    GRÁFICO 3: Relación Lluvia vs Calor por año.

    Pregunta: ¿Los mayos secos son los más calurosos?

    Gráfico de burbujas:
    - Eje X: Precipitación total del mes
    - Eje Y: Temperatura máxima media del mes
    - Tamaño burbuja: Días con precipitación > 0mm en mayo
    - Color: Gradiente según temperatura máxima media
    """

    print("\n📊 Generando Gráfico 3: Lluvia vs Calor...")

    # --- PREPARAR DATOS ---
    df_anual = df.groupby("anio").agg(
        temp_max_media=("temp_max", "mean"),
        precipitacion_total=("precipitacion", "sum"),
        dias_extremos=("temp_max", lambda x: int((x > 35).sum())),
        dias_lluvia=("precipitacion", lambda x: int((x > 0).sum())),
    ).reset_index()

    # Tamaño mínimo de 1 para que siempre sea visible la burbuja
    df_anual["tamano_burbuja"] = df_anual["dias_lluvia"].clip(lower=1)

    # --- CREAR GRÁFICO ---
    fig = px.scatter(
        df_anual,
        x="precipitacion_total",
        y="temp_max_media",
        size="tamano_burbuja",
        color="temp_max_media",
        text="anio",
        color_continuous_scale="RdYlBu_r",
        size_max=50,
        hover_data={                            # ← Solo columnas que existen en df_anual
            "anio": True,
            "precipitacion_total": ":.1f",
            "temp_max_media": ":.1f",
            "dias_lluvia": True,
            "dias_extremos": True,
            "tamano_burbuja": False,            # Ocultamos columna auxiliar
        },
        labels={
            "anio": "Año",
            "precipitacion_total": "Precipitación total (mm)",
            "temp_max_media": "Temp. máx. media (°C)",
            "dias_lluvia": "Días con lluvia",
            "dias_extremos": "Días >35°C",
        },
    )

    # Etiquetas del año encima de cada burbuja
    fig.update_traces(
        textposition="top center",
        textfont={"size": 12, "color": "black"},
    )

    # --- PERSONALIZAR LAYOUT ---
    fig.update_layout(
        width=1200,
        height=700,
        plot_bgcolor="white",
        paper_bgcolor="white",
        title={
            "text": (
                "🌧️🌡️ Mayo en Córdoba: ¿Seco = Caluroso?<br>"
                "<sup>Precipitación vs Temperatura | "
                "Tamaño de burbuja = días con lluvia en mayo | "
                "Fuente: AEMET</sup>"
            ),
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 22},
        },
        xaxis_title="Precipitación total en mayo (mm)",
        yaxis_title="Temperatura máxima media (°C)",
        font={"family": "Arial", "size": 14},
        coloraxis_colorbar={
            "title": "Temp. máx.<br>media (°C)",
            "thickness": 15,
            "len": 0.6,
        },
        margin={"b": 80},
    )

    fig.update_yaxes(gridcolor="lightgray")
    fig.update_xaxes(gridcolor="lightgray")

    # --- CUADRANTES ---
    media_lluvia = df_anual["precipitacion_total"].median()
    media_temp   = df_anual["temp_max_media"].median()

    fig.add_hline(y=media_temp,   line_dash="dot", line_color="gray", opacity=0.5)
    fig.add_vline(x=media_lluvia, line_dash="dot", line_color="gray", opacity=0.5)

    cuadrantes = [
        (0.05, 0.9, "<b>🔥 Seco y Caluroso</b>", "red"),
        (0.85, 0.9, "<b>🌧️🔥 Lluvioso y Caluroso</b>", "orange"),
        (0.05, 0.1, "<b>☀️ Seco y Fresco</b>", "blue"),
        (0.85, 0.1, "<b>🌧️❄️ Lluvioso y Fresco</b>", "green"),
    ]

    for x, y, texto, color in cuadrantes:
        fig.add_annotation(
            x=x, y=y,
            xref="paper", yref="paper",
            text=texto,
            showarrow=False,
            font={"size": 18, "color": color},  # ← Subido de 13 a 15
        )

    # Firma
    fig.add_annotation(
        text="@sitexdatos | Datos: AEMET OpenData",
        xref="paper", yref="paper",
        x=0.5, y=-0.12,
        showarrow=False,
        font={"size": 11, "color": "gray"},
    )

    # --- GUARDAR ---
    carpeta = obtener_carpeta_salida()

    fig.write_html(os.path.join(carpeta, "03_mayo_lluvia_vs_calor.html"))
    fig.write_image(os.path.join(carpeta, "03_mayo_lluvia_vs_calor.png"), scale=2)

    print("   💾 03_mayo_lluvia_vs_calor.html (interactivo)")
    print("   💾 03_mayo_lluvia_vs_calor.png (Instagram)")

    fig.show()

# --- BLOQUE PRINCIPAL ---
if __name__ == "__main__":

    print("📊 GRÁFICOS DE MAYO EN CÓRDOBA")
    print("=" * 50)

    # Obtener datos
    df = obtener_datos_mayo()

    # Resumen rápido
    print(f"\n📋 Resumen:")
    print(f"   Años: {df['anio'].min()} → {df['anio'].max()}")
    print(f"   Récord temp: {df['temp_max'].max()}°C")
    print(f"   Mayo más seco: {df.groupby('anio')['precipitacion'].sum().idxmin()}")
    print(f"   Mayo más lluvioso: {df.groupby('anio')['precipitacion'].sum().idxmax()}")

    # Generar los 3 gráficos
    grafico_1_evolucion_mayos(df)
    grafico_2_fiestas(df)
    grafico_3_lluvia_vs_calor(df)

    print("\n" + "=" * 50)
    print("✅ ¡3 gráficos generados!")
    print("📁 Revisa outputs/cordoba/")
    print("=" * 50)
