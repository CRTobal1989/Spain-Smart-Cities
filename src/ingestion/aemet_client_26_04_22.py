"""
Cliente para la API de AEMET OpenData.
Descarga datos climáticos de cualquier estación de España.

¿Qué es JSON?
    Es un formato de datos que parece un diccionario de Python:
    {"fecha": "2024-01-01", "tmax": "16.1", "tmin": "4.3"}
"""

# --- IMPORTACIONES ---
# Cada 'import' trae una herramienta que alguien ya programó

# 'os' viene con Python. Sirve para leer variables del sistema
import os

# 'requests' la instalamos con pip. Sirve para hacer peticiones a internet
import requests

# 'load_dotenv' la instalamos con pip. Lee el archivo .env
from dotenv import load_dotenv

# --- CARGA DE VARIABLES DE ENTORNO ---
# Esto lee tu archivo .env y carga las variables (API Key, contraseñas)
# A partir de aquí, podemos usar os.getenv("AEMET_API_KEY")
load_dotenv()


# --- DEFINICIÓN DE LA CLASE ---
# Una clase es como un "plano" para crear objetos.
# Es como una plantilla:
#   - La clase 'AemetClient' es el plano
#   - Cuando hacemos AemetClient() creamos un objeto a partir del plano
#   - Ese objeto tiene funciones (métodos) que podemos usar

class AemetClient:
    """
    Cliente para conectar con la API de AEMET.

    Uso:
        cliente = AemetClient()
        datos = cliente.get_datos_diarios("2024-01-01", "2024-01-31", "cordoba")
    """

    # --- VARIABLES DE CLASE ---
    # Son compartidas por todos los objetos de esta clase
    # Se definen fuera de cualquier función

    # URL base de la API de AEMET (todas las peticiones empiezan aquí)
    BASE_URL = "https://opendata.aemet.es/opendata/api"

    # Diccionario de estaciones meteorológicas por ciudad
    # Clave = nombre de la ciudad (lo que escribimos nosotros)
    # Valor = código de estación (lo que entiende AEMET)
    # ✅ ESCALABLE: para añadir una ciudad, solo añades una línea aquí
    ESTACIONES = {
        "cordoba": "5402",       # Córdoba Aeropuerto
        "sevilla": "5783",       # Sevilla Aeropuerto
        "malaga": "6155A",       # Málaga Aeropuerto
        "madrid": "3129",        # Madrid Retiro
        "barcelona": "0076",     # Barcelona Fabra
        "valencia": "8416",      # Valencia Aeropuerto
        "granada": "5530E",      # Granada Aeropuerto
    }

    # --- METODO __init__ ---
    # Es el "constructor". Se ejecuta automáticamente al crear el objeto.
    # Cuando haces: cliente = AemetClient()
    # Python ejecuta __init__ por dentro.
    #
    # 'self' es una referencia al propio objeto.
    # self.api_key = "..." guarda la API Key DENTRO del objeto
    # para poder usarla en otros métodos.

    def __init__(self):
        """Inicializa el cliente con la API Key del archivo .env"""

        # Lee la variable AEMET_API_KEY del archivo .env
        # os.getenv() devuelve None si no la encuentra
        self.api_key = os.getenv("AEMET_API_KEY")

        # Si no encontró la API Key, lanzamos un error
        # 'raise' = lanzar un error para que el programa se detenga
        if not self.api_key:
            raise ValueError(
                "❌ No se encontró AEMET_API_KEY en el archivo .env"
            )

        # Cabeceras HTTP que enviaremos con cada petición
        # La API de AEMET necesita la api_key en las cabeceras
        self.headers = {
            "api_key": self.api_key,
            "Accept": "application/json",  # Queremos los datos en JSON
        }

        print("✅ Cliente AEMET inicializado correctamente")

    # --- METODO get_datos_diarios ---
    # Es una función que pertenece a la clase.
    # Recibe parámetros y devuelve datos.
    #
    # Los "type hints" (: str, -> list[dict]) no son obligatorios
    # pero ayudan a entender qué tipo de dato espera y devuelve.
    #   str = texto
    #   list[dict] = lista de diccionarios

    def get_datos_diarios(
        self,
        fecha_inicio: str,        # Texto con formato 'YYYY-MM-DD'
        fecha_fin: str,           # Texto con formato 'YYYY-MM-DD'
        ciudad: str = "cordoba",  # Valor por defecto: cordoba
    ) -> list[dict]:              # Devuelve: lista de diccionarios
        """
        Descarga datos climáticos diarios de una ciudad.

        Args:
            fecha_inicio: Formato 'YYYY-MM-DD' (ej: '2024-01-01')
            fecha_fin: Formato 'YYYY-MM-DD' (ej: '2024-01-31')
            ciudad: Nombre de la ciudad (ej: 'cordoba', 'sevilla')

        Returns:
            Lista de diccionarios con los datos diarios
        """

        # --- PASO 1: Obtener código de estación ---
        # .get() busca la clave en el diccionario
        # .lower() convierte a minúsculas ("Cordoba" → "cordoba")
        # Si no encuentra la ciudad, devuelve None
        estacion = self.ESTACIONES.get(ciudad.lower())

        # Si la ciudad no está en nuestro diccionario, error
        if not estacion:
            raise ValueError(
                f"❌ Ciudad '{ciudad}' no encontrada. "
                f"Disponibles: {list(self.ESTACIONES.keys())}"
            )

        # --- PASO 2: Formatear fechas ---
        # Añadimos la hora UTC al final de la fecha
        # AEMET necesita formato: 2024-07-01T00:00:00UTC
        fi = fecha_inicio + "T00:00:00UTC"
        ff = fecha_fin + "T23:59:59UTC"

        # --- PASO 3: Construir la URL ---
        # La URL es la "dirección" a la que pedimos los datos
        # f"..." es un f-string: permite meter variables dentro del texto
        # {fi} se sustituye por el valor de la variable fi
        url = (
            f"{self.BASE_URL}"
            f"/valores/climatologicos/diarios/datos"
            f"/fechaini/{fi}"
            f"/fechafin/{ff}"
            f"/estacion/{estacion}"
        )

        print(f"📡 Descargando datos de {ciudad.upper()}: {fecha_inicio} → {fecha_fin}")

        # --- PASO 4: Primera petición a AEMET ---
        # AEMET funciona en DOS pasos:
        #   1) Pides datos → te devuelve una URL temporal
        #   2) Vas a esa URL temporal → te devuelve los datos reales
        #
        # requests.get() hace una petición HTTP GET (como abrir una web)
        # .raise_for_status() lanza error si algo falla (ej: 404, 500)
        # .json() convierte la respuesta de texto a diccionario Python
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        resultado = response.json()

        # --- PASO 5: Verificar respuesta ---
        # Si AEMET devuelve estado 404 = no hay datos para ese periodo
        if resultado.get("estado") == 404:
            print(f"⚠️ No hay datos para ese periodo")
            return []  # Devolvemos lista vacía

        # Extraer la URL donde están los datos reales
        datos_url = resultado.get("datos")
        if not datos_url:
            print(f"⚠️ No se obtuvo URL de datos")
            return []

        # --- PASO 6: Segunda petición (datos reales) ---
        # Ahora sí descargamos los datos climáticos
        datos_response = requests.get(datos_url)
        datos = datos_response.json()

        print(f"✅ Descargados {len(datos)} registros")

        # Devolvemos la lista de diccionarios con los datos
        return datos

    def test_conexion(self):
        """Prueba básica de conexión con AEMET."""

        # Endpoint simple: inventario de estaciones
        url = f"{self.BASE_URL}/valores/climatologicos/inventarioestaciones/todasestaciones"

        print("🔍 Probando conexión con AEMET...")

        response = requests.get(url, headers=self.headers)
        resultado = response.json()

        print(f"🔍 Estado: {resultado.get('estado')}")
        print(f"🔍 Descripción: {resultado.get('descripcion')}")

        # Si hay datos, descargamos las estaciones
        datos_url = resultado.get("datos")
        if datos_url:
            estaciones = requests.get(datos_url).json()
            print(f"✅ Conexión OK. Total estaciones: {len(estaciones)}")

            # Buscar estaciones en Córdoba
            print("\n📍 Estaciones en CÓRDOBA:")
            for est in estaciones:
                if "CÓRDOBA" in est.get("nombre", "").upper():
                    print(f"   {est['indicativo']} → {est['nombre']} ({est['provincia']})")

            return estaciones

        return []

# --- BLOQUE PRINCIPAL ---
# if __name__ == "__main__": significa:
#   "Ejecuta esto SOLO si corres este archivo directamente"
#   Si otro archivo importa este módulo, NO se ejecuta
#
# Es como un "modo prueba" del archivo

if __name__ == "__main__":

    # --- IMPORTAMOS PANDAS ---
    # pandas es la librería estrella para manejar tablas en Python
    # pd es el alias estándar (todo el mundo usa "pd")
    import pandas as pd

    # Crear cliente y descargar datos
    cliente = AemetClient()

    datos = cliente.get_datos_diarios(
        fecha_inicio="2026-01-01",
        fecha_fin="2026-04-21",
        ciudad="cordoba",
    )

    if datos:
        # --- CREAR TABLA (DataFrame) ---
        # pd.DataFrame() convierte una lista de diccionarios en una tabla
        # Es como pasar de JSON a Excel
        df = pd.DataFrame(datos)

        # --- SELECCIONAR COLUMNAS QUE NOS INTERESAN ---
        # De todos los campos, nos quedamos solo con los importantes
        # --- SELECCIONAR COLUMNAS QUE NOS INTERESAN ---
        columnas = ["fecha", "nombre", "tmed", "tmax", "tmin", "prec", "sol"]
        df = df[columnas]

        # --- RENOMBRAR COLUMNAS CON UNIDADES ---
        # .rename() cambia los nombres de las columnas
        # columns={} es un diccionario: {"nombre_viejo": "nombre_nuevo"}
        df = df.rename(columns={
            "fecha": "Fecha",
            "nombre": "Estacion",
            "tmed": "Temp Media (C)",
            "tmax": "Temp Max (C)",
            "tmin": "Temp Min (C)",
            "prec": "Precipitacion (mm)",
            "sol": "Horas de Sol (h)",
        })

        # --- MOSTRAR LA TABLA ---
        # .to_string() muestra la tabla completa en consola
        # Sin él, pandas recorta las filas si hay muchas
        print("\n📊 DATOS CLIMÁTICOS DE CÓRDOBA")
        print("=" * 80)
        print(df.to_string())

        # --- RESUMEN RÁPIDO ---
        # .shape devuelve (filas, columnas)
        print(f"\nTotal: {df.shape[0]} filas x {df.shape[1]} columnas")