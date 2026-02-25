from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    PROJECT_NAME: str = "Plataforma"
    VERSION: str = "1.0.0"

    # Variables opcionales de entorno (Cloud Run)
    GOOGLE_CLOUD_PROJECT: str | None = None
    GCS_BUCKET: str | None = None

    class Config:
        env_file = ".env"


@lru_cache
def get_settings():
    return Settings()


settings = get_settings()
