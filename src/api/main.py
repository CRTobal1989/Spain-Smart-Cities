# ============================================================
# main.py - Punto de entrada de la API SITEX DATOS
# ============================================================
# Para ejecutar la API, abre una terminal en la RAÍZ del
# proyecto (Spain-smart-cities/) y escribe:
#
#   python -m uvicorn src.api.main:app --reload
#
# Documentación automática:
#   - Swagger UI: http://localhost:8000/docs
#   - ReDoc:      http://localhost:8000/redoc
# ============================================================

from fastapi import FastAPI

# Importamos los routers con rutas absolutas desde la raíz del proyecto
from src.api.routes.municipios import router as municipios_router
from src.api.routes.clima import router as clima_router


# ============================================================
# CREACIÓN DE LA APP
# ============================================================
app = FastAPI(
    title="SITEX DATOS API",
    description="""
    **API de Datos Climáticos de España**

    API REST que proporciona acceso a datos meteorológicos diarios
    de AEMET para municipios españoles (2015-2026).

    **Municipios disponibles:** Córdoba, Sevilla, Málaga, Granada,
    Madrid, Barcelona y Valencia.

    **Fuente de datos:** AEMET OpenData

    Desarrollado por @sitexdatos
    """,
    version="1.0.0",
    openapi_tags=[
        {
            "name": "Municipios",
            "description": "Información de los municipios disponibles."
        },
        {
            "name": "Datos Climáticos",
            "description": "Datos meteorológicos diarios, estadísticas y récords."
        }
    ]
)


# ============================================================
# REGISTRAR ROUTERS
# ============================================================
# Prefijo /api/v1 → buena práctica de versionado.
# Si en el futuro cambias la API, puedes crear /api/v2
# sin romper a quienes ya usen /api/v1.
app.include_router(municipios_router, prefix="/api/v1")
app.include_router(clima_router, prefix="/api/v1")


# ============================================================
# ENDPOINTS GENERALES
# ============================================================
@app.get("/", tags=["General"])
def inicio():
    """Página de inicio con enlaces útiles."""
    return {
        "mensaje": "Bienvenido a SITEX DATOS API",
        "version": "1.0.0",
        "documentacion": {
            "swagger": "http://localhost:8000/docs",
            "redoc": "http://localhost:8000/redoc"
        },
        "endpoints": {
            "municipios": "/api/v1/municipios/",
            "clima": "/api/v1/clima/",
            "estadisticas": "/api/v1/clima/estadisticas/mensuales",
            "records": "/api/v1/clima/records/{municipio}"
        }
    }


@app.get("/health", tags=["General"])
def health_check():
    """Comprueba que la API está activa."""
    return {"status": "ok", "service": "sitex-datos-api"}
