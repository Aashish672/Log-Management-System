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
from app.core.database import logs_collection, templates_collection, compressed_collection

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
    Accepts a single log entry, extracts its template, compresses it, and stores it in MongoDB Atlas.
    """
    try:
        # Parse template
        parsed_result = parser.parse(log_entry.message)

        # Prepare enriched log
        enriched_log = {
            "service_name": log_entry.service_name,
            "severity": log_entry.severity,
            "timestamp": log_entry.timestamp,
            **parsed_result,
        }

        # Compress single log
        compressed_block = compressor.compress_log_block([enriched_log])

        # -----------------------------
        # Save to MongoDB Atlas
        # -----------------------------
        result = await logs_collection.insert_one(enriched_log)
        enriched_log["_id"] = str(result.inserted_id)  # convert ObjectId to string

        for t_id, block in compressed_block.items():
            await compressed_collection.update_one(
                {"template_id": t_id},
                {"$set": block},
                upsert=True
            )

        await templates_collection.update_one(
            {"template_id": parsed_result["template_id"]},
            {"$set": {"template": parsed_result["template"], "count": parsed_result["template_frequency"]}},
            upsert=True
        )

        logger.info(f"✅ Ingested, parsed, compressed, and stored log: {enriched_log}")

        return {
            "status": "success",
            "message": "Log entry accepted, parsed, compressed, and stored",
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
    Accepts a batch of log entries, extracts templates, compresses groups, and stores them in MongoDB Atlas.
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

        # Store logs
        if parsed_batch:
            result = await logs_collection.insert_many(parsed_batch)
            # convert ObjectIds to string
            for doc, oid in zip(parsed_batch, result.inserted_ids):
                doc["_id"] = str(oid)

        # Store compressed blocks
        for t_id, block in compressed_blocks.items():
            await compressed_collection.update_one(
                {"template_id": t_id},
                {"$set": block},
                upsert=True
            )

        # Update template dictionary
        for parsed in parsed_batch:
            await templates_collection.update_one(
                {"template_id": parsed["template_id"]},
                {"$set": {"template": parsed["template"], "count": parsed["template_frequency"]}},
                upsert=True
            )

        logger.info(f"✅ Processed batch of {len(parsed_batch)} logs and stored them.")

        return {
            "status": "success",
            "message": f"{len(parsed_batch)} log entries parsed, compressed, and stored",
            "parsed_data": parsed_batch,
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
