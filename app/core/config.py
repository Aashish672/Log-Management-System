# app/core/config.py

import os

class Settings:
    PROJECT_NAME: str = "Cloud Log Management System"
    VERSION: str = "1.1.0"
    LOG_LEVEL: str = "INFO"
    DATABASE_URL: str = os.getenv("DATABASE_URL", "mongodb://localhost:27017/logs_db")

settings = Settings()
