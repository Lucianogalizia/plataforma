import os


class Settings:
    """
    Configuración global del backend.
    Lee variables de entorno (Cloud Run) con valores por defecto.
    """

    # Info básica
    PROJECT_NAME: str = os.getenv("PROJECT_NAME", "DINA Backend")
    VERSION: str = os.getenv("VERSION", "1.0.0")

    # Google Cloud
    GOOGLE_CLOUD_PROJECT: str | None = os.getenv("GOOGLE_CLOUD_PROJECT")
    GCS_BUCKET: str | None = os.getenv("GCS_BUCKET")

    # Puerto (Cloud Run usa PORT=8000)
    PORT: int = int(os.getenv("PORT", 8000))

    # Entorno
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "production")


# Instancia global que importa main.py
settings = Settings()
