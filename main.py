# main.py

from fastapi import FastAPI
from app.api.endpoints.logs import router as log_router

app = FastAPI(
    title="Cloud Log Management System – Ingestion, Template Extraction & Compression",
    version="1.2.0",
    description="""
    Cloud-hosted API that ingests logs, extracts recurring templates, 
    and compresses data blocks using template-based columnar compression.
    """,
)

app.include_router(log_router, prefix="/logs", tags=["Log Pipeline"])

@app.get("/")
async def root():
    return {"message": "Log Management Pipeline is running 🚀"}
