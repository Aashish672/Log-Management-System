# app/core/config.py
# app/core/config.py

from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    PROJECT_NAME: str = "Cloud Log Management System"
    VERSION: str = "1.2.0"
    LOG_LEVEL: str = "INFO"

    # MongoDB (Atlas or local)
    MONGODB_URI: str = Field(..., env="MONGODB_URI")
    DB_NAME: str = Field(..., env="DB_NAME")

    class Config:
        env_file = ".env"  # Load variables from .env file
        env_file_encoding = "utf-8"

settings = Settings()
