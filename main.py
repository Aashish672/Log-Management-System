from fastapi import FastAPI
from app.api.endpoints.logs import router as log_router
from app.core.config import settings
from app.core.database import db  # your Motor client

app = FastAPI(
    title="Cloud Log Management System – Ingestion, Template Extraction & Compression",
    version="1.2.0",
    description="""
    Cloud-hosted API that ingests logs, extracts recurring templates, 
    and compresses data blocks using template-based columnar compression.
    """,
)

app.include_router(log_router, prefix="/logs", tags=["Log Pipeline"])


@app.on_event("startup")
async def startup_event():
    # Print MongoDB URI and database
    print("🧠 Connected to:", settings.MONGODB_URI)
    print("🗄️ Using database:", settings.DB_NAME)
    # Test MongoDB connection
    try:
        result = await db.command("ping")
        print("✅ MongoDB Atlas connection successful:", result)
    except Exception as e:
        print("❌ MongoDB connection failed:", e)


@app.get("/")
async def root():
    return {"message": "Log Management Pipeline is running 🚀"}
