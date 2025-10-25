# app/api/endpoints/logs.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List
from enum import Enum
import datetime
import logging

# Local imports
from app.modules.template_parser import TemplateParser
from app.modules.compression import CompressionModule

# -----------------------------------------------------------
# Setup Logging
# -----------------------------------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

# -----------------------------------------------------------
# Initialize Parser & Compressor
# -----------------------------------------------------------
parser = TemplateParser()
compressor = CompressionModule()

# -----------------------------------------------------------
# Pydantic Models
# -----------------------------------------------------------
class SeverityLevel(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogEntry(BaseModel):
    service_name: str = Field(..., example="auth-service")
    severity: SeverityLevel = Field(..., example="ERROR")
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    message: str = Field(..., example="User login failed for user 'admin'")


# -----------------------------------------------------------
# Initialize Router
# -----------------------------------------------------------
router = APIRouter()


# -----------------------------------------------------------
# SINGLE LOG INGESTION
# -----------------------------------------------------------
@router.post("/ingest", status_code=202)
async def ingest_log(log_entry: LogEntry):
    """
    Accepts a single log entry, extracts its template, and compresses it.
    """
    try:
        parsed_result = parser.parse(log_entry.message)

        enriched_log = {
            "service_name": log_entry.service_name,
            "severity": log_entry.severity,
            "timestamp": log_entry.timestamp,
            **parsed_result,
        }

        # Compress single log (treated as a batch of one)
        compressed_block = compressor.compress_log_block([enriched_log])

        logger.info(f"✅ Ingested, parsed, and compressed log: {enriched_log}")

        return {
            "status": "success",
            "message": "Log entry accepted, parsed, and compressed",
            "parsed_data": enriched_log,
            "compressed_block": compressed_block,
        }

    except Exception as e:
        logger.error(f"Error ingesting log: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------
# BATCH LOG INGESTION
# -----------------------------------------------------------
@router.post("/ingest/batch", status_code=202)
async def ingest_log_batch(log_entries: List[LogEntry]):
    """
    Accepts a batch of logs, performs template extraction and compression.
    """
    try:
        parsed_batch = []

        for entry in log_entries:
            parsed = parser.parse(entry.message)
            enriched = {
                "service_name": entry.service_name,
                "severity": entry.severity,
                "timestamp": entry.timestamp,
                **parsed,
            }
            parsed_batch.append(enriched)

        # Compress the parsed batch
        compressed_blocks = compressor.compress_log_block(parsed_batch)

        logger.info(f"✅ Processed batch of {len(parsed_batch)} logs and compressed blocks generated.")

        return {
            "status": "success",
            "message": f"{len(parsed_batch)} log entries parsed and compressed",
            "compressed_blocks": compressed_blocks,
        }

    except Exception as e:
        logger.error(f"Error processing batch logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------
# TEMPLATE INSPECTION ENDPOINT
# -----------------------------------------------------------
@router.get("/templates", status_code=200)
async def get_templates():
    """
    Returns all discovered templates and their frequencies.
    """
    templates = parser.get_templates()
    return {"count": len(templates), "templates": templates}
