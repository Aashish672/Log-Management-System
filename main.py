from fastapi import FastAPI
from app.api.endpoints import logs

# ----------------------------
# Initialize FastAPI App
# ----------------------------

app=FastAPI(
    title="Cloud Log Management System - Ingestion Layer",
    version="1.0.0",
    description="""
    This API handles real-time and batch log ingestion.
    Logs can be forwarded to message queues, databases, or 
    analytics pipelines for further processing.""",
    )

# ---------------------------
# include Log Router
# ---------------------------

app.include_router(logs.router,prefix="/logs",tags=["Log Ingestion"])

#-----------------------------
# Health check Route
# ----------------------------
@app.get("/")
async def root():
    return {"message":"Log Ingestion Layer is running"}