# ============================================================
# database.py - Conexión a PostgreSQL
# ============================================================
# Configura la conexión entre la API y PostgreSQL.
# Reutiliza las mismas variables de tu .env que ya usas en
# los scripts de ingesta (DB_HOST, DB_PORT, etc.)
# ============================================================

import os
import sys

# --- Imports de SQLAlchemy ---
# create_engine: crea la conexión ("motor") con PostgreSQL
# sessionmaker: fábrica de sesiones para hacer consultas
# declarative_base: clase base para nuestros modelos ORM
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# dotenv: lee el archivo .env (la misma librería que ya usas)
from dotenv import load_dotenv


# ============================================================
# CARGAR VARIABLES DE ENTORNO
# ============================================================
# Buscamos el .env en la raíz del proyecto (Spain-smart-cities/.env)
# Subimos 3 niveles: api/ → src/ → Spain-smart-cities/
raiz_proyecto = os.path.dirname(
    os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
)
load_dotenv(os.path.join(raiz_proyecto, ".env"))


# ============================================================
# CONSTRUIR URL DE CONEXIÓN
# ============================================================
# Usamos las mismas variables que ya tienes en tu .env:
#   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
#
# El formato de la URL es:
#   postgresql://USUARIO:CONTRASEÑA@HOST:PUERTO/NOMBRE_BD
DATABASE_URL = (
    f"postgresql://"
    f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
    f"/{os.getenv('DB_NAME')}"
)


# ============================================================
# MOTOR DE BASE DE DATOS (ENGINE)
# ============================================================
# El "engine" es la conexión principal con PostgreSQL.
# echo=False: no muestra las queries SQL en consola
#             (cámbialo a True para depurar)
engine = create_engine(DATABASE_URL, echo=False)


# ============================================================
# FÁBRICA DE SESIONES
# ============================================================
# Una "sesión" es como una conversación con la base de datos.
# Cada petición HTTP abre una sesión, hace consultas, y la cierra.
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


# ============================================================
# CLASE BASE PARA MODELOS
# ============================================================
# Todos los modelos ORM (tablas) heredan de esta clase.
Base = declarative_base()


# ============================================================
# FUNCIÓN get_db() - Inyección de dependencias
# ============================================================
# FastAPI llama a esta función automáticamente en cada endpoint
# para proporcionar una sesión de base de datos.
#
# yield "presta" la sesión al endpoint.
# Cuando el endpoint termina, se ejecuta db.close().
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
