"""
Carrusel de Instagram: Polen del olivo en Córdoba

5 slides:
1. Portada: "¿Qué nos espera este mayo?"
2. Evolución: Polen acumulado por temporada (2015-2026)
3. Clima: Curvas acumuladas de lluvia y sol (año gemelo)
4. Predicción: Comparativa 2026 vs año gemelo
5. Cierre: Consejos + CTA

Genera versiones light y dark de cada slide.
Usa datos de PostgreSQL (tablas: polen_diario, clima_diario, municipios).

Librerías utilizadas:
- plotly: Librería de gráficos interactivos. Genera HTML (interactivo) y PNG (estático)
- sqlalchemy: Motor de conexión a bases de datos. pandas lo usa para leer SQL
- dotenv: Lee variables de entorno desde el archivo .env (contraseñas, etc.)
- numpy: Cálculos numéricos (distancias, normalizaciones)
- kaleido: Motor de exportación de Plotly a PNG (se instala con pip install kaleido)
"""

# ============================================================
# IMPORTACIONES
# ============================================================
import os

import numpy as np                 # numpy: cálculos matemáticos (arrays, medias, distancias)
import pandas as pd                # pandas: manipulación de datos tabulares (DataFrames)
import plotly.graph_objects as go   # plotly: gráficos interactivos y estáticos
from dotenv import load_dotenv     # dotenv: carga variables del archivo .env
from sqlalchemy import create_engine  # sqlalchemy: conexión a PostgreSQL


# ============================================================
# CONEXIÓN A BASE DE DATOS
# ============================================================
def conectar_db():
    """
    Conecta con PostgreSQL usando SQLAlchemy.
    Lee las credenciales del archivo .env en la raíz del proyecto.
    """
    # Forzar codificación UTF-8 para caracteres españoles (tildes, ñ)
    os.environ["PGCLIENTENCODING"] = "UTF8"

    # Buscar el .env subiendo 3 niveles: analysis → src → raíz
    raiz = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
    load_dotenv(os.path.join(raiz, ".env"))

    # Construir URL de conexión con las variables del .env
    db_url = (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

    engine = create_engine(db_url, client_encoding="UTF8")
    return engine


def obtener_carpeta_salida():
    """Devuelve la ruta a outputs/cordoba/ y la crea si no existe."""
    raiz = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
    # Subcarpeta específica para el carrusel de polen
    carpeta = os.path.join(raiz, "outputs", "cordoba", "polen_instagram")
    os.makedirs(carpeta, exist_ok=True)
    return carpeta


# ============================================================
# OBTENCIÓN DE DATOS
# ============================================================
def obtener_datos_polen():
    """
    Descarga datos de polen del olivo de Córdoba (2015-2026).
    Devuelve un DataFrame con el resumen por año.
    """
    engine = conectar_db()

    # Resumen anual de polen
    # SUM = total acumulado de la temporada
    # MAX = pico máximo (el peor día para los alérgicos)
    # AVG = concentración media diaria
    query_anual = """
        SELECT
            EXTRACT(YEAR FROM fecha) AS anio,
            COUNT(*) AS dias,
            ROUND(AVG(polen_olivo)::NUMERIC, 2) AS media_olivo,
            ROUND(MAX(polen_olivo)::NUMERIC, 2) AS pico_olivo,
            ROUND(SUM(polen_olivo)::NUMERIC, 0) AS acumulado_olivo,
            ROUND(AVG(polen_gramineas)::NUMERIC, 2) AS media_gramineas,
            ROUND(MAX(polen_gramineas)::NUMERIC, 2) AS pico_gramineas,
            ROUND(SUM(polen_gramineas)::NUMERIC, 0) AS acumulado_gramineas
        FROM polen_diario
        GROUP BY EXTRACT(YEAR FROM fecha)
        ORDER BY anio;
    """

    df = pd.read_sql(query_anual, engine)
    df["anio"] = df["anio"].astype(int)

    print(f"✅ Polen: {len(df)} años cargados ({df['anio'].min()}-{df['anio'].max()})")
    return df


def obtener_datos_clima_acumulado():
    """
    Descarga la precipitación y horas de sol acumuladas día a día
    para Córdoba Aeropuerto (todos los años disponibles).

    Usa una CTE (Common Table Expression) para filtrar solo Córdoba
    antes de calcular el acumulado con ventana (OVER).
    """
    engine = conectar_db()

    # CTE: filtra solo Córdoba y luego calcula acumulados con ventana
    # SUM(...) OVER (PARTITION BY anio ORDER BY fecha)
    #   = suma acumulada dentro de cada año, ordenada por fecha
    query = """
        WITH clima_cordoba AS (
            SELECT
                c.fecha,
                EXTRACT(YEAR FROM c.fecha) AS anio,
                EXTRACT(DOY FROM c.fecha) AS dia_del_anio,
                c.precipitacion,
                c.horas_sol
            FROM clima_diario c
            JOIN municipios m ON c.codigo_ine = m.codigo_ine
            WHERE m.nombre = 'Córdoba'
        )
        SELECT
            anio,
            fecha,
            dia_del_anio,
            ROUND(SUM(precipitacion) OVER (
                PARTITION BY anio ORDER BY fecha
            )::NUMERIC, 2) AS precip_acum_mm,
            ROUND(SUM(horas_sol) OVER (
                PARTITION BY anio ORDER BY fecha
            )::NUMERIC, 2) AS sol_acum_horas
        FROM clima_cordoba
        ORDER BY anio, fecha;
    """

    df = pd.read_sql(query, engine)
    df["anio"] = df["anio"].astype(int)

    print(f"✅ Clima: {len(df)} registros diarios cargados")
    return df


def encontrar_anio_gemelo(df_clima, anio_objetivo=2026):
    """
    Encuentra el 'año gemelo' de 2026: el año cuyo patrón de
    lluvia + sol acumulados es más similar hasta la fecha actual.

    Metodo: distancia euclidiana normalizada entre los vectores
    (precipitación_acumulada, horas_sol_acumuladas) al mismo día del año.

    Returns:
        tuple: (anio_gemelo, df_comparacion, similitud_porcentaje)
    """
    # Buscar hasta qué día del año llega 2026
    datos_2026 = df_clima[df_clima["anio"] == anio_objetivo]
    dia_corte = int(datos_2026["dia_del_anio"].max())

    print(f"\n🔍 Buscando año gemelo de {anio_objetivo} (datos hasta día {dia_corte})...")

    # Extraer valor acumulado al día de corte para cada año
    comparacion = []
    for anio in sorted(df_clima["anio"].unique()):
        datos_anio = df_clima[df_clima["anio"] == anio]
        # Filtrar hasta el día de corte
        datos_corte = datos_anio[datos_anio["dia_del_anio"] <= dia_corte]
        if len(datos_corte) == 0:
            continue
        # Tomar el último registro (= valor acumulado más reciente)
        fila = datos_corte.iloc[-1]
        comparacion.append({
            "anio": int(anio),
            "precip_acum": fila["precip_acum_mm"],
            "sol_acum": fila["sol_acum_horas"],
        })

    df_comp = pd.DataFrame(comparacion)

    # Valores de 2026
    val_2026 = df_comp[df_comp["anio"] == anio_objetivo].iloc[0]

    # Calcular distancia euclidiana normalizada
    # Normalizar: dividir por el rango para que lluvia y sol pesen igual
    otros = df_comp[df_comp["anio"] != anio_objetivo].copy()
    precip_rango = otros["precip_acum"].max() - otros["precip_acum"].min()
    sol_rango = otros["sol_acum"].max() - otros["sol_acum"].min()

    # Distancia normalizada = sqrt((Δprecip/rango)² + (Δsol/rango)²)
    otros["distancia"] = np.sqrt(
        ((otros["precip_acum"] - val_2026["precip_acum"]) / precip_rango) ** 2
        + ((otros["sol_acum"] - val_2026["sol_acum"]) / sol_rango) ** 2
    )
    otros = otros.sort_values("distancia")

    # El año más cercano es el gemelo
    anio_gemelo = int(otros.iloc[0]["anio"])

    # Calcular similitud como porcentaje (1 - distancia normalizada)
    # La distancia máxima posible es sqrt(2) ≈ 1.414
    similitud = round((1 - otros.iloc[0]["distancia"] / np.sqrt(2)) * 100, 0)

    print(f">>> Año gemelo de {anio_objetivo}: {anio_gemelo} (similitud: {similitud}%)")
    print(f"    {anio_objetivo}: precip={val_2026['precip_acum']}mm, sol={val_2026['sol_acum']}h")
    gemelo_datos = otros.iloc[0]
    print(f"    {anio_gemelo}: precip={gemelo_datos['precip_acum']}mm, sol={gemelo_datos['sol_acum']}h")

    return anio_gemelo, df_comp, similitud, dia_corte


# ============================================================
# ESTILOS PARA LOS GRÁFICOS
# ============================================================
def get_style(mode):
    """
    Devuelve un diccionario con los colores según el modo (light/dark).
    Así podemos generar los mismos gráficos con 2 estéticas diferentes
    y que el usuario elija cuál publicar en Instagram.
    """
    if mode == "light":
        return {
            "bg": "#FFFFFF",              # Fondo blanco
            "text": "#2D2D2D",            # Texto oscuro
            "text2": "#666666",           # Texto secundario (gris)
            "accent": "#E85D3A",          # Color principal (coral/rojo)
            "accent2": "#3A8EE8",         # Color secundario (azul)
            "accent3": "#4CAF50",         # Color terciario (verde)
            "grid": "#E8E8E8",            # Líneas de grid
            "bar_normal": "#F0C8A0",      # Barras normales (beige)
            "bar_highlight": "#E85D3A",   # Barra destacada (peor año)
            "line_others": "rgba(200,200,200,0.4)",  # Líneas de otros años
            "brand": "#999999",           # Color del watermark
        }
    else:
        return {
            "bg": "#0D1117",              # Fondo oscuro (GitHub dark)
            "text": "#E6EDF3",            # Texto claro
            "text2": "#8B949E",           # Texto secundario
            "accent": "#FF6B35",          # Naranja neón
            "accent2": "#58A6FF",         # Azul neón
            "accent3": "#3FB950",         # Verde neón
            "grid": "#21262D",            # Grid oscuro
            "bar_normal": "#21262D",      # Barras normales
            "bar_highlight": "#FF6B35",   # Barra destacada
            "line_others": "rgba(48,54,61,0.6)",  # Líneas de otros años
            "brand": "#484F58",           # Watermark
        }


# ============================================================
# OBTENCIÓN DE DATOS DIARIOS DE POLEN (TODOS LOS AÑOS)
# ============================================================
def obtener_datos_polen_todos():
    """
    Descarga los datos DIARIOS de polen de olivo para TODOS los años
    y todos los meses de temporada (marzo-junio).

    Se usa para los boxplots mensuales, donde necesitamos la distribución
    diaria de cada año.

    Returns:
        DataFrame con columnas: fecha, anio, mes, polen_olivo, polen_gramineas
    """
    engine = conectar_db()

    query = """
        SELECT
            fecha,
            EXTRACT(YEAR FROM fecha) AS anio,
            EXTRACT(MONTH FROM fecha) AS mes,
            ROUND(polen_olivo::NUMERIC, 2) AS polen_olivo,
            ROUND(polen_gramineas::NUMERIC, 2) AS polen_gramineas
        FROM polen_diario
        WHERE EXTRACT(MONTH FROM fecha) BETWEEN 4 AND 6
        ORDER BY fecha;
    """

    df = pd.read_sql(query, engine)
    df["anio"] = df["anio"].astype(int)
    df["mes"] = df["mes"].astype(int)

    print(f"✅ Polen diario (todos): {len(df)} registros cargados")
    return df


# ============================================================
# SLIDE 2: BOXPLOTS MENSUALES DE POLEN DEL OLIVO
# ============================================================
def slide_boxplot_mes(df_todos, mes, anio_gemelo, mode="light"):
    """
    Genera UN gráfico de boxplots (cajas y bigotes) para un mes concreto.

    Cada boxplot muestra la distribución diaria de polen del olivo
    de un año. Así se ve de un vistazo:
    - La mediana (línea central): el día "típico" de ese mes
    - La caja (Q1-Q3): donde se concentra el 50% central de los días
    - Los bigotes: hasta dónde llegan los valores normales
    - Los puntos: días extremos (picos de polen)

    Destaca 2026 y el año gemelo con colores diferentes.
    Para mayo y junio se excluye 2026 (datos incompletos).

    Args:
        df_todos: DataFrame con columnas fecha, anio, mes, polen_olivo
        mes: número del mes (4=Abril, 5=Mayo, 6=Junio)
        anio_gemelo: año gemelo de 2026 (se destaca en azul)
        mode: "light" o "dark"

    Returns:
        Figura de Plotly con los boxplots de ese mes
    """
    s = get_style(mode)

    # Nombres de los meses para los títulos
    nombres_mes = {4: "Abril", 5: "Mayo", 6: "Junio"}
    nombre = nombres_mes[mes]

    print(f"\n📊 Generando boxplot {nombre} - modo {mode}...")

    # Filtrar solo el mes que nos interesa
    df_mes = df_todos[df_todos["mes"] == mes].copy()

    # Para mayo y junio, excluir 2026 porque no tiene datos completos
    # (la temporada alta aún no ha pasado a fecha de hoy)
    if mes in [5, 6]:
        df_mes = df_mes[df_mes["anio"] != 2026]

    # Años disponibles (ordenados)
    anios = sorted(df_mes["anio"].unique())

    # Crear figura individual (no subplots)
    fig = go.Figure()

    # --- PALETA DE COLORES PARA "OTROS AÑOS" ---
    # En vez de usar un solo color beige apagado para todos,
    # usamos una paleta degradada que da vida al gráfico
    # pero sin robar protagonismo al año gemelo ni a 2026.
    if mode == "light":
        # Tonos cálidos suaves pero VISIBLES sobre fondo blanco
        paleta_otros = [
            "#C4A882",  # arena oscuro
            "#D4A574",  # terracota suave
            "#B8C4A0",  # verde oliva claro
            "#A0B4C8",  # azul grisáceo
            "#C8A0B4",  # malva suave
            "#C4B882",  # dorado apagado
            "#A0C4B8",  # verde agua
            "#C89A82",  # salmón suave
            "#B4A8C8",  # lavanda
            "#A8C4A0",  # verde menta
            "#C4A0A0",  # rosa viejo
        ]
    else:
        # Tonos neón apagados para modo oscuro
        paleta_otros = [
            "#6B7B5E",  # verde militar
            "#7B6B5E",  # marrón claro
            "#5E6B7B",  # azul acero
            "#7B5E6B",  # burdeos apagado
            "#6B7B6B",  # verde grisáceo
            "#7B7B5E",  # oliva oscuro
            "#5E7B6B",  # verde azulado
            "#7B6B6B",  # gris cálido
            "#6B5E7B",  # morado oscuro
            "#5E7B7B",  # teal oscuro
            "#7B5E5E",  # rojo apagado
        ]

    # Contador para asignar colores de la paleta a los años "normales"
    idx_color = 0

    for anio in anios:
        df_anio = df_mes[df_mes["anio"] == anio]

        # Si no hay datos para este año-mes, saltar
        if len(df_anio) == 0:
            continue

        # Color según el año:
        #   - 2026 → rojo/naranja (accent) - protagonista
        #   - Año gemelo → azul (accent2) - co-protagonista
        #   - Resto → color de la paleta rotativa
        if anio == 2026:
            color = s["accent"]
            color_linea = s["accent"]
            ancho_linea = 2.5
            opacidad = 1.0
        elif anio == anio_gemelo:
            color = s["accent2"]
            color_linea = s["accent2"]
            ancho_linea = 2.5
            opacidad = 1.0
        else:
            color = paleta_otros[idx_color % len(paleta_otros)]
            color_linea = color
            ancho_linea = 1.5
            opacidad = 0.85
            idx_color += 1

        # go.Box: crea un boxplot
        # boxpoints="outliers" → solo muestra los puntos atípicos
        # (los que están fuera de 1.5 × IQR)
        # width=0.6 → ancho de la caja (más grande = más visible)
        fig.add_trace(go.Box(
            y=df_anio["polen_olivo"],
            name=str(anio),
            marker_color=color_linea,
            marker_size=5,
            line={"width": ancho_linea, "color": color_linea},
            fillcolor=color,
            opacity=opacidad,
            boxpoints="outliers",
            jitter=0.3,
            pointpos=0,
            width=0.6,           # Ancho de la caja (0-1). Más ancho = más visible
            hovertemplate=(
                f"<b>{anio} - {nombre}</b><br>"
                "Mediana: %{y:.0f} gr/m³<extra></extra>"
            ),
        ))

    # --- Texto de leyenda manual ---
    # Si 2026 está incluido (solo en abril), mostramos su color
    if mes == 4:
        leyenda_text = (
            f'<span style="color:{s["accent"]}">■</span> 2026 &nbsp;&nbsp;'
            f'<span style="color:{s["accent2"]}">■</span> {anio_gemelo} (gemelo)'
        )
    else:
        # Mayo/junio: sin 2026
        leyenda_text = (
            f'<span style="color:{s["accent2"]}">■</span> {anio_gemelo} (gemelo)'
        )

    # --- DISEÑO ---
    fig.update_layout(
        title={
            "text": (
                f"Polen del olivo en {nombre}: distribución diaria por año<br>"
                f"<sub>Concentración diaria en Córdoba (granos/m³)</sub>"
            ),
            "font": {"size": 22, "color": s["text"]},
            "x": 0.5,
        },
        plot_bgcolor=s["bg"],
        paper_bgcolor=s["bg"],
        font={"color": s["text"]},
        boxmode="group",          # Agrupa los boxplots por año
        showlegend=False,         # Usamos leyenda manual (abajo)
        width=1080,
        height=1080,
        margin={"t": 120, "b": 100, "l": 80, "r": 40},
        yaxis_title="granos/m³",
        # Eje X: rotar etiquetas de años para que no se pisen
        xaxis={"tickangle": -45, "tickfont": {"size": 13}},
        annotations=[
            # Leyenda manual (colores de los años destacados)
            {
                "text": leyenda_text,
                "xref": "paper", "yref": "paper",
                "x": 0.5, "y": -0.07,
                "showarrow": False,
                "font": {"size": 14, "color": s["text2"]},
            },
            # Watermark / créditos
            {
                "text": "@sitexdatos  |  Datos: CAMS-Copernicus · SILAM",
                "xref": "paper", "yref": "paper",
                "x": 0.5, "y": -0.10,
                "showarrow": False,
                "font": {"size": 10, "color": s["brand"]},
            },
        ],
    )

    # Configurar ejes
    fig.update_yaxes(gridcolor=s["grid"])
    fig.update_xaxes(gridcolor=s["grid"])

    return fig


# ============================================================
# SLIDE 3: CURVAS ACUMULADAS DE CLIMA
# ============================================================
def slide_clima(df_clima, anio_gemelo, dia_corte, mode="light"):
    """
    Dos subgráficos superpuestos:
    - Arriba: Precipitación acumulada (mm)
    - Abajo: Horas de sol acumuladas

    Destaca 2026 y su año gemelo sobre el fondo del resto de años.
    """
    s = get_style(mode)

    print(f"\n📊 Generando Slide 3 (clima) - modo {mode}...")

    from plotly.subplots import make_subplots

    # make_subplots: crea una figura con varias gráficas apiladas
    # No usamos subplot_titles porque salen centrados y chocan con el título.
    # En su lugar añadimos anotaciones manuales alineadas a la izquierda.
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,         # Comparten el eje X (día del año)
        vertical_spacing=0.12,     # Espacio entre gráficas (más para el título)
    )

    # Meses para el eje X (día del año → nombre del mes)
    meses_dias = {1: "Ene", 32: "Feb", 60: "Mar", 91: "Abr", 121: "May",
                  152: "Jun", 182: "Jul", 213: "Ago", 244: "Sep",
                  274: "Oct", 305: "Nov", 335: "Dic"}

    # --- DIBUJAR LÍNEAS POR AÑO ---
    for anio in sorted(df_clima["anio"].unique()):
        datos = df_clima[df_clima["anio"] == anio]

        # Estilo según si es 2026, gemelo, u otro
        # Los "otros años" ahora usan un gris más visible (0.6 opacidad, grosor 1.5)
        if anio == 2026:
            color, ancho, opacidad = s["accent"], 3.5, 1
            nombre = "2026"
        elif anio == anio_gemelo:
            color, ancho, opacidad = s["accent2"], 3, 0.9
            nombre = f"{anio_gemelo} (gemelo)"
        else:
            color = "rgba(150,150,150,0.7)" if mode == "light" else "rgba(120,130,140,0.7)"
            ancho, opacidad = 1.5, 0.6
            nombre = str(anio)

        # Mostrar leyenda solo para 2026, gemelo, y un "otros"
        # bool() convierte numpy.bool_ a bool nativo de Python
        # (Plotly no acepta numpy.bool_, da error de validación)
        mostrar = bool(anio in [2026, anio_gemelo] or anio == 2015)

        # Precipitación (gráfico superior)
        fig.add_trace(go.Scatter(
            x=datos["dia_del_anio"],
            y=datos["precip_acum_mm"],
            mode="lines",
            line={"color": color, "width": ancho},
            opacity=opacidad,
            name=nombre if anio != 2015 else "Otros años",
            showlegend=mostrar,
            legendgroup=nombre,
            hovertemplate=f"<b>{anio}</b><br>Día: %{{x}}<br>Precip: %{{y:.1f}} mm<extra></extra>",
        ), row=1, col=1)

        # Horas de sol (gráfico inferior)
        fig.add_trace(go.Scatter(
            x=datos["dia_del_anio"],
            y=datos["sol_acum_horas"],
            mode="lines",
            line={"color": color, "width": ancho},
            opacity=opacidad,
            name=nombre,
            showlegend=False,      # No duplicar leyenda
            legendgroup=nombre,
            hovertemplate=f"<b>{anio}</b><br>Día: %{{x}}<br>Sol: %{{y:.1f}} h<extra></extra>",
        ), row=2, col=1)

    # Línea vertical en el día de corte (hasta donde llega 2026)
    # Solo el gráfico superior lleva etiqueta "Hoy (29 abr)"
    fig.add_vline(
        x=dia_corte, row=1, col=1,
        line_dash="dot", line_color=s["accent"], line_width=1,
        annotation_text="Hoy", annotation_position="top",
        annotation_font={"size": 11, "color": s["accent"]},
    )
    # Gráfico inferior: solo la línea, sin texto
    fig.add_vline(
        x=dia_corte, row=2, col=1,
        line_dash="dot", line_color=s["accent"], line_width=1,
    )

    # --- DISEÑO ---
    fig.update_layout(
        title={
            "text": (
                "2026 vs histórico: lluvia y sol acumulados<br>"
                "<sub>Córdoba Aeropuerto  |  ¿A qué año se parece 2026?</sub>"
            ),
            "font": {"size": 20, "color": s["text"]},
            "x": 0.5,
        },
        plot_bgcolor=s["bg"],
        paper_bgcolor=s["bg"],
        font={"color": s["text"]},
        legend={
            "bgcolor": s["bg"],
            "bordercolor": s["grid"],
            "font": {"size": 11},
            "x": 0.02, "y": 0.98,
        },
        width=1080,
        height=1080,
        margin={"t": 100, "b": 80, "l": 70, "r": 30},
    )

    # Configurar ejes X con nombres de meses
    for row in [1, 2]:
        fig.update_xaxes(
            tickvals=list(meses_dias.keys()),
            ticktext=list(meses_dias.values()),
            gridcolor=s["grid"],
            range=[1, 366],
            row=row, col=1,
        )
        fig.update_yaxes(gridcolor=s["grid"], row=row, col=1)

    # Título del eje Y de cada subgráfico
    fig.update_yaxes(title_text="Precipitación (mm)", row=1, col=1)
    fig.update_yaxes(title_text="Horas de sol (h)", row=2, col=1)

    # Títulos de cada subgráfico alineados a la izquierda
    # xref/yref="paper" → coordenadas relativas al papel (0-1)
    # y=0.98 → arriba del gráfico superior, y=0.45 → arriba del inferior
    fig.add_annotation(
        text="<b>Precipitación acumulada</b>",
        xref="paper", yref="paper",
        x=0.0, y=1.01, xanchor="left",
        showarrow=False,
        font={"size": 15, "color": s["text"]},
    )
    fig.add_annotation(
        text="<b>Horas de sol acumuladas</b>",
        xref="paper", yref="paper",
        x=0.0, y=0.45, xanchor="left",
        showarrow=False,
        font={"size": 15, "color": s["text"]},
    )

    # Watermark
    fig.add_annotation(
        text="@sitexdatos  |  Datos: AEMET OpenData",
        xref="paper", yref="paper",
        x=0.5, y=-0.06,
        showarrow=False,
        font={"size": 10, "color": s["brand"]},
    )

    return fig


# ============================================================
# SLIDE 4: PREDICCIÓN (AÑO GEMELO)
# ============================================================
def slide_prediccion(df_polen, df_comp, anio_gemelo, similitud, dia_corte, mode="light"):
    """
    Slide informativa: muestra la comparación 2026 vs año gemelo
    y la predicción de polen (olivo + gramíneas) para mayo-junio.

    Incluye:
    - Año gemelo destacado
    - Tabla comparativa lluvia/sol acumulados
    - Predicción de olivo (pico + acumulado)
    - Predicción de gramíneas (pico + acumulado)
    - Insight: en 2024 el fuerte impacto fue en abril, en mayo bajó

    Usa anotaciones de Plotly para crear un diseño tipo infografía.
    """
    s = get_style(mode)

    print(f"\n📊 Generando Slide 4 (predicción) - modo {mode}...")

    # Datos del año gemelo (olivo + gramíneas)
    polen_gemelo = df_polen[df_polen["anio"] == anio_gemelo].iloc[0]
    val_2026 = df_comp[df_comp["anio"] == 2026].iloc[0]
    val_gemelo = df_comp[df_comp["anio"] == anio_gemelo].iloc[0]

    # Verificar si hay datos de gramíneas para el gemelo
    tiene_gramineas = (
        "pico_gramineas" in polen_gemelo.index
        and pd.notna(polen_gemelo.get("pico_gramineas"))
        and polen_gemelo["pico_gramineas"] > 0
    )

    # Crear figura vacía (solo anotaciones)
    fig = go.Figure()

    # Fondo invisible
    fig.add_trace(go.Scatter(
        x=[0, 1], y=[0, 1],
        mode="markers",
        marker={"opacity": 0},
        showlegend=False,
        hoverinfo="skip",
    ))

    # --- ANOTACIONES DE LA INFOGRAFÍA ---
    anotaciones = [
        # Subtítulo
        {"text": "El año gemelo de 2026 es...",
         "x": 0.5, "y": 0.96, "showarrow": False,
         "font": {"size": 24, "color": s["text2"]}},

        # Número grande
        {"text": f"<b>{anio_gemelo}</b>",
         "x": 0.5, "y": 0.88, "showarrow": False,
         "font": {"size": 70, "color": s["accent2"]}},

        # --- TABLA COMPARATIVA CLIMA ---
        # Headers
        {"text": "<b>2026</b>", "x": 0.55, "y": 0.79, "showarrow": False,
         "font": {"size": 16, "color": s["accent"]}},
        {"text": f"<b>{anio_gemelo}</b>", "x": 0.80, "y": 0.79, "showarrow": False,
         "font": {"size": 16, "color": s["accent2"]}},

        # Fila 1: Lluvia (fecha actualizada a 29 abr)
        {"text": "Lluvia acum. (29 abr)", "x": 0.12, "y": 0.74, "showarrow": False,
         "font": {"size": 14, "color": s["text2"]}, "xanchor": "left"},
        {"text": f"<b>{val_2026['precip_acum']:.1f} mm</b>", "x": 0.55, "y": 0.74,
         "showarrow": False, "font": {"size": 14, "color": s["accent"]}},
        {"text": f"<b>{val_gemelo['precip_acum']:.1f} mm</b>", "x": 0.80, "y": 0.74,
         "showarrow": False, "font": {"size": 14, "color": s["accent2"]}},

        # Fila 2: Sol (fecha actualizada a 29 abr)
        {"text": "Horas de sol acum. (29 abr)", "x": 0.12, "y": 0.69, "showarrow": False,
         "font": {"size": 14, "color": s["text2"]}, "xanchor": "left"},
        {"text": f"<b>{val_2026['sol_acum']:.1f} h</b>", "x": 0.55, "y": 0.69,
         "showarrow": False, "font": {"size": 14, "color": s["accent"]}},
        {"text": f"<b>{val_gemelo['sol_acum']:.1f} h</b>", "x": 0.80, "y": 0.69,
         "showarrow": False, "font": {"size": 14, "color": s["accent2"]}},

        # Similitud
        {"text": f"<b>SIMILITUD: {similitud:.0f}%</b>",
         "x": 0.5, "y": 0.62, "showarrow": False,
         "font": {"size": 20, "color": s["accent3"]}},

        # === PREDICCIÓN OLIVO ===
        {"text": "<b>PREDICCIÓN OLIVO — MAYO-JUNIO 2026</b>",
         "x": 0.5, "y": 0.54, "showarrow": False,
         "font": {"size": 16, "color": s["accent"]}},

        # Pico olivo
        {"text": "Pico esperado", "x": 0.30, "y": 0.49, "showarrow": False,
         "font": {"size": 13, "color": s["text2"]}},
        {"text": f"<b>~{polen_gemelo['pico_olivo']:,.0f}</b>",
         "x": 0.30, "y": 0.44, "showarrow": False,
         "font": {"size": 28, "color": s["accent"]}},
        {"text": "granos/m³", "x": 0.30, "y": 0.41, "showarrow": False,
         "font": {"size": 11, "color": s["text2"]}},

        # Acumulado olivo
        {"text": "Acumulado total", "x": 0.70, "y": 0.49, "showarrow": False,
         "font": {"size": 13, "color": s["text2"]}},
        {"text": f"<b>~{polen_gemelo['acumulado_olivo']:,.0f}</b>",
         "x": 0.70, "y": 0.44, "showarrow": False,
         "font": {"size": 28, "color": s["accent2"]}},
        {"text": "granos/m³", "x": 0.70, "y": 0.41, "showarrow": False,
         "font": {"size": 11, "color": s["text2"]}},
    ]

    # --- Líneas decorativas ---
    formas = [
        # Línea bajo el número grande
        {"type": "line", "x0": 0.1, "x1": 0.9, "y0": 0.82, "y1": 0.82,
         "line": {"color": s["grid"], "width": 1.5}},
        # Separador antes de predicción olivo
        {"type": "line", "x0": 0.1, "x1": 0.9, "y0": 0.57, "y1": 0.57,
         "line": {"color": s["grid"], "width": 1, "dash": "dash"}},
        # Box predicción olivo
        {"type": "rect", "x0": 0.08, "x1": 0.92, "y0": 0.39, "y1": 0.56,
         "line": {"color": s["accent"], "width": 2},
         "fillcolor": s["bg"]},
    ]

    # === PREDICCIÓN GRAMÍNEAS (solo si hay datos) ===
    if tiene_gramineas:
        anotaciones += [
            {"text": "<b>PREDICCIÓN GRAMÍNEAS — MAYO-JUNIO 2026</b>",
             "x": 0.5, "y": 0.34, "showarrow": False,
             "font": {"size": 16, "color": s["accent3"]}},

            # Pico gramíneas
            {"text": "Pico esperado", "x": 0.30, "y": 0.29, "showarrow": False,
             "font": {"size": 13, "color": s["text2"]}},
            {"text": f"<b>~{polen_gemelo['pico_gramineas']:,.0f}</b>",
             "x": 0.30, "y": 0.24, "showarrow": False,
             "font": {"size": 28, "color": s["accent3"]}},
            {"text": "granos/m³", "x": 0.30, "y": 0.21, "showarrow": False,
             "font": {"size": 11, "color": s["text2"]}},

            # Acumulado gramíneas
            {"text": "Acumulado total", "x": 0.70, "y": 0.29, "showarrow": False,
             "font": {"size": 13, "color": s["text2"]}},
            {"text": f"<b>~{polen_gemelo['acumulado_gramineas']:,.0f}</b>",
             "x": 0.70, "y": 0.24, "showarrow": False,
             "font": {"size": 28, "color": s["accent3"]}},
            {"text": "granos/m³", "x": 0.70, "y": 0.21, "showarrow": False,
             "font": {"size": 11, "color": s["text2"]}},
        ]
        # Box predicción gramíneas
        formas.append(
            {"type": "rect", "x0": 0.08, "x1": 0.92, "y0": 0.19, "y1": 0.36,
             "line": {"color": s["accent3"], "width": 2},
             "fillcolor": s["bg"]},
        )

    # === INSIGHT DE LOS BOXPLOTS ===
    anotaciones.append(
        {"text": (
            "<i>En 2024, el olivo tuvo su mayor impacto en abril;<br>"
            "en mayo disminuyó considerablemente.<br>"
            "Las gramíneas, en cambio, empiezan a impactar a partir de mayo.</i>"
        ),
         "x": 0.5, "y": 0.12, "showarrow": False,
         "font": {"size": 13, "color": s["text2"]}},
    )

    # Contexto: media histórica
    anotaciones.append(
        {"text": (
            f"Temporada por debajo de la media histórica "
            f"({df_polen[df_polen['anio'] != 2026]['acumulado_olivo'].mean():,.0f} granos/m³)"
        ),
         "x": 0.5, "y": 0.07, "showarrow": False,
         "font": {"size": 13, "color": s["text"]}},
    )

    # Watermark (sin "Reanalysis")
    anotaciones.append(
        {"text": "@sitexdatos  |  Datos: CAMS-Copernicus · SILAM · AEMET",
         "x": 0.5, "y": 0.02, "showarrow": False,
         "font": {"size": 10, "color": s["brand"]}},
    )

    # --- DISEÑO TIPO INFOGRAFÍA ---
    fig.update_layout(
        plot_bgcolor=s["bg"],
        paper_bgcolor=s["bg"],
        width=1080,
        height=1080,
        xaxis={"visible": False, "range": [0, 1]},
        yaxis={"visible": False, "range": [0, 1]},
        margin={"t": 20, "b": 20, "l": 20, "r": 20},
        annotations=anotaciones,
        shapes=formas,
    )

    return fig


# ============================================================
# GUARDAR GRÁFICOS
# ============================================================
def guardar(fig, nombre, carpeta):
    """
    Guarda un gráfico de Plotly como HTML (interactivo) y PNG (Instagram).

    - HTML: se abre en navegador, puedes hacer zoom, hover, etc.
    - PNG: imagen estática 1080x1080 lista para Instagram

    Para exportar PNG necesitas tener instalado 'kaleido':
        pip install kaleido
    """
    # Guardar HTML (siempre funciona)
    ruta_html = os.path.join(carpeta, f"{nombre}.html")
    fig.write_html(ruta_html)
    print(f"  ✅ HTML: {ruta_html}")

    # Guardar PNG (necesita kaleido)
    try:
        ruta_png = os.path.join(carpeta, f"{nombre}.png")
        fig.write_image(ruta_png, width=1080, height=1080, scale=2)
        print(f"  ✅ PNG:  {ruta_png}")
    except Exception as e:
        print(f"  ⚠️  No se pudo exportar PNG: {e}")
        print("     Instala kaleido: pip install kaleido")


def guardar_matplotlib(fig, nombre, carpeta, mode="light"):
    """
    Guarda una figura de matplotlib como PNG.
    Se usa para las slides de texto (portada, cierre) que no son Plotly.

    Args:
        fig: figura de matplotlib
        nombre: nombre del archivo (sin extensión)
        carpeta: ruta de la carpeta de salida
        mode: "light" o "dark" (para el color de fondo al guardar)
    """
    import matplotlib.pyplot as plt

    # Color de fondo según el modo
    bg = "#FFFFFF" if mode == "light" else "#0D1117"

    ruta_png = os.path.join(carpeta, f"{nombre}.png")
    fig.savefig(ruta_png, facecolor=bg, dpi=150)
    plt.close(fig)
    print(f"  ✅ PNG:  {ruta_png}")


# ============================================================
# OBTENCIÓN DE DATOS DIARIOS DE POLEN
# ============================================================
def obtener_datos_polen_diario(anios=[2024, 2026]):
    """
    Descarga los datos DIARIOS de polen (olivo + gramíneas) para
    los años indicados, filtrando solo la temporada marzo-junio.

    A diferencia de obtener_datos_polen() que da un resumen anual,
    esta función devuelve un registro por día, necesario para
    dibujar las curvas de evolución diaria.

    Args:
        anios: lista de años a consultar (ej: [2024, 2026])

    Returns:
        DataFrame con columnas: fecha, anio, dia_del_anio, polen_olivo, polen_gramineas
    """
    engine = conectar_db()

    # Convertir la lista de años a texto para el SQL
    # Ejemplo: [2024, 2026] → '2024, 2026'
    anios_str = ", ".join(str(a) for a in anios)

    # Query: datos diarios de polen para los años seleccionados
    # EXTRACT(DOY FROM fecha) = día del año (1-366)
    #   Esto permite alinear 2024 y 2026 en el mismo eje X
    #   aunque las fechas sean diferentes (1-mar-2024 vs 1-mar-2026)
    query = f"""
        SELECT
            fecha,
            EXTRACT(YEAR FROM fecha) AS anio,
            EXTRACT(DOY FROM fecha) AS dia_del_anio,
            ROUND(polen_olivo::NUMERIC, 2) AS polen_olivo,
            ROUND(polen_gramineas::NUMERIC, 2) AS polen_gramineas
        FROM polen_diario
        WHERE EXTRACT(YEAR FROM fecha) IN ({anios_str})
          AND EXTRACT(MONTH FROM fecha) BETWEEN 3 AND 6
        ORDER BY fecha;
    """

    df = pd.read_sql(query, engine)
    df["anio"] = df["anio"].astype(int)

    for anio in anios:
        n = len(df[df["anio"] == anio])
        print(f"✅ Polen diario {anio}: {n} días cargados")

    return df


# ============================================================
# SLIDE 5: COMPARATIVA DIARIA 2026 vs AÑO GEMELO
# ============================================================
def slide_comparativa_diaria(df_diario, anio_gemelo, mode="light"):
    """
    Gráfico de líneas: evolución diaria del polen (olivo + gramíneas)
    comparando 2026 con su año gemelo.

    Estructura:
    - Arriba: Polen del olivo (2 líneas: 2026 vs gemelo)
    - Abajo: Polen de gramíneas (2 líneas: 2026 vs gemelo)

    El eje X usa el día del año (DOY) para alinear ambos años.
    La zona donde 2026 aún no tiene datos queda vacía, mostrando
    visualmente lo que "queda por venir" según el patrón del gemelo.
    """
    s = get_style(mode)

    print(f"\n📊 Generando Slide 5 (comparativa diaria) - modo {mode}...")

    from plotly.subplots import make_subplots

    # Sin subplot_titles: usamos anotaciones manuales a la izquierda
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.12,
    )

    # Separar datos por año
    df_2026 = df_diario[df_diario["anio"] == 2026]
    df_gemelo = df_diario[df_diario["anio"] == anio_gemelo]

    # Día del año donde acaban los datos de 2026
    dia_corte_2026 = int(df_2026["dia_del_anio"].max()) if len(df_2026) > 0 else 0

    # --- MARCAS DE MESES EN EL EJE X ---
    # Solo marzo-junio (la temporada de polen)
    meses = {60: "1 Mar", 75: "15 Mar", 91: "1 Abr", 106: "15 Abr",
             121: "1 May", 136: "15 May", 152: "1 Jun", 167: "15 Jun", 182: "30 Jun"}

    # ========================
    # GRÁFICO SUPERIOR: OLIVO
    # ========================

    # Línea del año gemelo (completa marzo-junio)
    # Se dibuja PRIMERO para que quede detrás
    fig.add_trace(go.Scatter(
        x=df_gemelo["dia_del_anio"],
        y=df_gemelo["polen_olivo"],
        mode="lines",
        line={"color": s["accent2"], "width": 2, "dash": "dash"},
        name=f"{anio_gemelo} (gemelo)",
        legendgroup="gemelo",
        hovertemplate=(
            f"<b>{anio_gemelo}</b><br>"
            "Día: %{x}<br>"
            "Olivo: %{y:.1f} gr/m³<extra></extra>"
        ),
    ), row=1, col=1)

    # Línea de 2026 (solo hasta donde hay datos)
    fig.add_trace(go.Scatter(
        x=df_2026["dia_del_anio"],
        y=df_2026["polen_olivo"],
        mode="lines",
        line={"color": s["accent"], "width": 3},
        name="2026",
        legendgroup="2026",
        hovertemplate=(
            "<b>2026</b><br>"
            "Día: %{x}<br>"
            "Olivo: %{y:.1f} gr/m³<extra></extra>"
        ),
    ), row=1, col=1)

    # Zona sombreada: "predicción" = datos del gemelo donde 2026 aún no tiene
    # Esto muestra visualmente lo que podría pasar
    df_gemelo_futuro = df_gemelo[df_gemelo["dia_del_anio"] > dia_corte_2026]
    if len(df_gemelo_futuro) > 0:
        fig.add_trace(go.Scatter(
            x=df_gemelo_futuro["dia_del_anio"],
            y=df_gemelo_futuro["polen_olivo"],
            mode="lines",
            line={"color": s["accent2"], "width": 0},
            fill="tozeroy",
            fillcolor=f"rgba({int(s['accent2'][1:3], 16)},{int(s['accent2'][3:5], 16)},{int(s['accent2'][5:7], 16)},0.1)",
            name=f"Proyección {anio_gemelo}",
            legendgroup="proyeccion",
            showlegend=True,
            hoverinfo="skip",
        ), row=1, col=1)

    # ==========================
    # GRÁFICO INFERIOR: GRAMÍNEAS
    # ==========================

    # Línea del año gemelo
    fig.add_trace(go.Scatter(
        x=df_gemelo["dia_del_anio"],
        y=df_gemelo["polen_gramineas"],
        mode="lines",
        line={"color": s["accent2"], "width": 2, "dash": "dash"},
        name=f"{anio_gemelo} (gemelo)",
        legendgroup="gemelo",
        showlegend=False,
        hovertemplate=(
            f"<b>{anio_gemelo}</b><br>"
            "Día: %{x}<br>"
            "Gramíneas: %{y:.1f} gr/m³<extra></extra>"
        ),
    ), row=2, col=1)

    # Línea de 2026
    fig.add_trace(go.Scatter(
        x=df_2026["dia_del_anio"],
        y=df_2026["polen_gramineas"],
        mode="lines",
        line={"color": s["accent"], "width": 3},
        name="2026",
        legendgroup="2026",
        showlegend=False,
        hovertemplate=(
            "<b>2026</b><br>"
            "Día: %{x}<br>"
            "Gramíneas: %{y:.1f} gr/m³<extra></extra>"
        ),
    ), row=2, col=1)

    # Zona sombreada gramíneas (más visible: opacidad 0.15)
    if len(df_gemelo_futuro) > 0:
        fig.add_trace(go.Scatter(
            x=df_gemelo_futuro["dia_del_anio"],
            y=df_gemelo_futuro["polen_gramineas"],
            mode="lines",
            line={"color": s["accent2"], "width": 0},
            fill="tozeroy",
            fillcolor=f"rgba({int(s['accent2'][1:3], 16)},{int(s['accent2'][3:5], 16)},{int(s['accent2'][5:7], 16)},0.15)",
            name=f"Proyección {anio_gemelo}",
            legendgroup="proyeccion",
            showlegend=False,
            hoverinfo="skip",
        ), row=2, col=1)

    # Línea vertical: "29 de abril" (hasta donde llegan los datos de 2026)
    # Solo el gráfico superior lleva etiqueta
    fig.add_vline(
        x=dia_corte_2026, row=1, col=1,
        line_dash="dot", line_color=s["accent"], line_width=1.5,
        annotation_text="29 de abril",
        annotation_font={"size": 11, "color": s["accent"]},
        annotation_position="top right",
    )
    fig.add_vline(
        x=dia_corte_2026, row=2, col=1,
        line_dash="dot", line_color=s["accent"], line_width=1.5,
    )

    # --- DISEÑO GENERAL ---
    fig.update_layout(
        title={
            "text": (
                f"2026 vs {anio_gemelo}: evolución diaria del polen<br>"
                f"<sub>Córdoba  |  La zona sombreada muestra la proyección según el año gemelo</sub>"
            ),
            "font": {"size": 20, "color": s["text"]},
            "x": 0.5,
        },
        plot_bgcolor=s["bg"],
        paper_bgcolor=s["bg"],
        font={"color": s["text"]},
        legend={
            "bgcolor": s["bg"],
            "bordercolor": s["grid"],
            "font": {"size": 11},
            "x": 0.02, "y": 0.98,
        },
        width=1080,
        height=1080,
        margin={"t": 100, "b": 80, "l": 80, "r": 30},
    )

    # Configurar ejes X con fechas legibles
    for row in [1, 2]:
        fig.update_xaxes(
            tickvals=list(meses.keys()),
            ticktext=list(meses.values()),
            gridcolor=s["grid"],
            range=[59, 183],          # 1 marzo (día 60) a 30 junio (día 182)
            row=row, col=1,
        )
        fig.update_yaxes(gridcolor=s["grid"], row=row, col=1)

    # Eje Y con unidades (granos/m³)
    fig.update_yaxes(title_text="granos/m³", row=1, col=1)
    fig.update_yaxes(title_text="granos/m³", row=2, col=1)

    # Títulos de cada subgráfico alineados a la izquierda
    fig.add_annotation(
        text="<b>Polen del olivo</b>",
        xref="paper", yref="paper",
        x=0.0, y=1.01, xanchor="left",
        showarrow=False,
        font={"size": 15, "color": s["text"]},
    )
    fig.add_annotation(
        text="<b>Polen de gramíneas</b>",
        xref="paper", yref="paper",
        x=0.0, y=0.45, xanchor="left",
        showarrow=False,
        font={"size": 15, "color": s["text"]},
    )

    # Watermark (solo uno)
    fig.add_annotation(
        text="@sitexdatos  |  Datos: CAMS-Copernicus",
        xref="paper", yref="paper",
        x=0.5, y=-0.06,
        showarrow=False,
        font={"size": 10, "color": s["brand"]},
    )

    return fig


# ============================================================
# SLIDE 6: CIERRE (CONCLUSIONES)
# ============================================================
def slide_cierre(df_polen, anio_gemelo, similitud, mode="light"):
    """
    Slide de cierre con las conclusiones del análisis.
    Usa matplotlib porque es una slide de texto puro (sin gráficos de datos).

    Resumen de conclusiones:
    - Año gemelo y similitud
    - Predicción olivo (pico + acumulado)
    - Predicción gramíneas
    - Insight temporal (olivo en abril, gramíneas en mayo)
    """
    import matplotlib.pyplot as plt
    from matplotlib.patches import FancyBboxPatch

    # Estilos (misma paleta que get_style pero para matplotlib)
    if mode == "light":
        bg = "#FFFFFF"
        text_color = "#2D2D2D"
        text2_color = "#666666"
        accent = "#E85D3A"
        accent2 = "#3A8EE8"
        accent3 = "#4CAF50"
        brand = "#999999"
        box_bg = "#FFF5F2"
        box_border = "#E85D3A"
    else:
        bg = "#0D1117"
        text_color = "#E6EDF3"
        text2_color = "#8B949E"
        accent = "#FF6B35"
        accent2 = "#58A6FF"
        accent3 = "#3FB950"
        brand = "#484F58"
        box_bg = "#1C1510"
        box_border = "#FF6B35"

    print(f"\n📊 Generando Slide 6 (cierre) - modo {mode}...")

    # Datos del año gemelo
    polen_gemelo = df_polen[df_polen["anio"] == anio_gemelo].iloc[0]

    # Verificar gramíneas
    tiene_gramineas = (
        "pico_gramineas" in polen_gemelo.index
        and pd.notna(polen_gemelo.get("pico_gramineas"))
        and polen_gemelo["pico_gramineas"] > 0
    )

    # --- CREAR FIGURA ---
    fig = plt.figure(figsize=(10.8, 10.8), dpi=100)
    fig.patch.set_facecolor(bg)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_facecolor(bg)
    ax.axis('off')

    # --- TÍTULO ---
    ax.text(0.5, 0.92, 'Conclusiones',
            fontsize=38, fontweight='bold', color=text_color,
            ha='center', va='center')

    # Línea decorativa bajo el título
    ax.plot([0.20, 0.80], [0.87, 0.87], color=accent, linewidth=3, alpha=0.5)

    # --- CONCLUSIÓN 1: AÑO GEMELO ---
    ax.text(0.5, 0.80,
            f'El año gemelo de 2026 es {anio_gemelo}',
            fontsize=22, fontweight='bold', color=accent2,
            ha='center', va='center')
    ax.text(0.5, 0.75,
            f'Similitud del {similitud:.0f}% en patron de lluvia y sol',
            fontsize=16, color=text2_color,
            ha='center', va='center')

    # --- BOX: PREDICCIÓN OLIVO ---
    box1 = FancyBboxPatch((0.08, 0.56), 0.84, 0.14,
                           boxstyle="round,pad=0.02",
                           facecolor=box_bg, edgecolor=box_border,
                           linewidth=1.5)
    ax.add_patch(box1)

    ax.text(0.5, 0.67, 'Polen del olivo',
            fontsize=18, fontweight='bold', color=accent,
            ha='center', va='center')
    ax.text(0.5, 0.62,
            f'Pico esperado: ~{polen_gemelo["pico_olivo"]:,.0f} granos/m3',
            fontsize=15, color=text_color,
            ha='center', va='center')
    ax.text(0.5, 0.58,
            f'Acumulado total: ~{polen_gemelo["acumulado_olivo"]:,.0f} granos/m3',
            fontsize=15, color=text_color,
            ha='center', va='center')

    # --- BOX: PREDICCIÓN GRAMÍNEAS ---
    if tiene_gramineas:
        box2 = FancyBboxPatch((0.08, 0.38), 0.84, 0.14,
                               boxstyle="round,pad=0.02",
                               facecolor=box_bg, edgecolor=accent3,
                               linewidth=1.5)
        ax.add_patch(box2)

        ax.text(0.5, 0.49, 'Polen de gramineas',
                fontsize=18, fontweight='bold', color=accent3,
                ha='center', va='center')
        ax.text(0.5, 0.44,
                f'Pico esperado: ~{polen_gemelo["pico_gramineas"]:,.0f} granos/m3',
                fontsize=15, color=text_color,
                ha='center', va='center')
        ax.text(0.5, 0.40,
                f'Acumulado total: ~{polen_gemelo["acumulado_gramineas"]:,.0f} granos/m3',
                fontsize=15, color=text_color,
                ha='center', va='center')

    # --- INSIGHT TEMPORAL ---
    media_historica = df_polen[df_polen['anio'] != 2026]['acumulado_olivo'].mean()
    ax.text(0.5, 0.30,
            'Temporada por debajo de la media historica',
            fontsize=16, fontweight='bold', color=text_color,
            ha='center', va='center')
    ax.text(0.5, 0.26,
            f'(media: {media_historica:,.0f} granos/m3)',
            fontsize=14, color=text2_color,
            ha='center', va='center')

    ax.text(0.5, 0.19,
            'El olivo tuvo su mayor impacto en abril.',
            fontsize=14, color=text2_color,
            ha='center', va='center')
    ax.text(0.5, 0.15,
            'Las gramineas empiezan a impactar a partir de mayo.',
            fontsize=14, color=text2_color,
            ha='center', va='center')

    # --- CTA ---
    ax.text(0.5, 0.08,
            'Guarda este post y compartelo',
            fontsize=16, fontstyle='italic', color=accent,
            ha='center', va='center')

    # --- WATERMARK ---
    ax.text(0.5, 0.02,
            '@sitexdatos  |  Datos: CAMS-Copernicus - SILAM - AEMET',
            fontsize=10, color=brand,
            ha='center', va='center')

    return fig


# ============================================================
# FUNCIÓN PRINCIPAL
# ============================================================
def main():
    """
    Ejecuta todo el pipeline:
    1. Conectar a PostgreSQL y descargar datos
    2. Encontrar el año gemelo de 2026
    3. Generar los gráficos en ambos estilos
    4. Guardar HTML + PNG
    """
    print("=" * 60)
    print("CARRUSEL INSTAGRAM: POLEN DEL OLIVO EN CÓRDOBA")
    print("=" * 60)

    # --- 1. OBTENER DATOS ---
    df_polen = obtener_datos_polen()
    df_clima = obtener_datos_clima_acumulado()

    # --- 2. ENCONTRAR AÑO GEMELO ---
    anio_gemelo, df_comp, similitud, dia_corte = encontrar_anio_gemelo(df_clima)

    # --- 3. OBTENER DATOS DIARIOS ---
    # Todos los años (para boxplots)
    df_todos = obtener_datos_polen_todos()
    # Solo 2026 y gemelo (para comparativa diaria)
    df_diario = obtener_datos_polen_diario(anios=[anio_gemelo, 2026])

    # --- 4. GENERAR GRÁFICOS ---
    # Todos los gráficos se guardan en outputs/cordoba/02_polen_instagram/
    raiz = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
    carpeta = os.path.join(raiz, "outputs", "cordoba", "02_polen_instagram")
    os.makedirs(carpeta, exist_ok=True)

    # Generar en ambos estilos (light y dark)
    for mode in ["light", "dark"]:
        print(f"\n{'='*40}")
        print(f"  ESTILO: {mode.upper()}")
        print(f"{'='*40}")

        # Slide 2: Boxplots individuales por mes (abril, mayo, junio)
        for mes in [4, 5, 6]:
            # Nombre del archivo: 02a_abril_light, 02b_mayo_dark, etc.
            letra = {4: "a", 5: "b", 6: "c"}[mes]
            nombre_mes = {4: "abril", 5: "mayo", 6: "junio"}[mes]
            fig_box = slide_boxplot_mes(df_todos, mes, anio_gemelo, mode)
            guardar(fig_box, f"02{letra}_{nombre_mes}_{mode}", carpeta)

        # Slide 3: Clima
        fig3 = slide_clima(df_clima, anio_gemelo, dia_corte, mode)
        guardar(fig3, f"03_clima_{mode}", carpeta)

        # Slide 4: Predicción
        fig4 = slide_prediccion(df_polen, df_comp, anio_gemelo, similitud, dia_corte, mode)
        guardar(fig4, f"04_prediccion_{mode}", carpeta)

        # Slide 5: Comparativa diaria 2026 vs año gemelo
        fig5 = slide_comparativa_diaria(df_diario, anio_gemelo, mode)
        guardar(fig5, f"05_comparativa_diaria_{mode}", carpeta)

        # Slide 6: Cierre (conclusiones) — matplotlib, solo PNG
        fig6 = slide_cierre(df_polen, anio_gemelo, similitud, mode)
        guardar_matplotlib(fig6, f"06_cierre_{mode}", carpeta, mode)

    print(f"\n{'='*60}")
    print(f"✅ CARRUSEL COMPLETO")
    print(f"   Archivos en: {carpeta}")
    print(f"   Año gemelo: {anio_gemelo} (similitud: {similitud}%)")
    print(f"{'='*60}")


# ============================================================
# PUNTO DE ENTRADA
# ============================================================
# if __name__ == "__main__":
#     Solo se ejecuta cuando haces: python -m src.analysis.polen_carrusel_instagram
#     No se ejecuta si otro script importa este archivo
if __name__ == "__main__":
    main()
