from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List
from enum import Enum
import datetime
import logging
from pymongo import UpdateOne  # Import UpdateOne for bulk operations

# Local imports
from app.modules.template_parser import TemplateParser
from app.modules.compression import CompressionModule
from app.core.database import logs_collection, templates_collection, compressed_collection

# -----------------------------------------------------------
# Setup
# -----------------------------------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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
    service_name: str
    severity: SeverityLevel
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    message: str

router = APIRouter()

# -----------------------------------------------------------
# SINGLE LOG INGESTION (Corrected)
# -----------------------------------------------------------
@router.post("/ingest", status_code=202)
async def ingest_log(log_entry: LogEntry):
    """
    Accepts a single log entry. This is for live-tailing.
    Compression is a batch-only operation.
    """
    try:
        # 1. Parse
        parsed = parser.parse(log_entry.message)
        enriched_log = {
            "service_name": log_entry.service_name,
            "severity": log_entry.severity,
            "timestamp": log_entry.timestamp,
            **parsed,
        }

        # 2. Store the single, uncompressed log
        await logs_collection.insert_one(enriched_log)

        # 3. Atomically update the template count
        await templates_collection.update_one(
            {"_id": parsed["template_id"]},
            {
                "$set": {"template_string": parsed["template"]},
                "$inc": {"frequency": 1}  # Use $inc for atomic increment
            },
            upsert=True
        )

        logger.info(f"✅ Stored single log with template {parsed['template_id']}")
        return {"status": "success", "message": "Log stored and template updated."}

    except Exception as e:
        logger.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------
# BATCH LOG INGESTION (Corrected for Performance)
# -----------------------------------------------------------
@router.post("/ingest/batch", status_code=202)
async def ingest_log_batch(log_entries: List[LogEntry]):
    """
    Accepts a batch of logs, performs template extraction, compression,
    and efficient bulk-storage.
    """
    try:
        parsed_batch = []
        template_updates = {} # Use a dict to track bulk template updates

        for entry in log_entries:
            parsed = parser.parse(entry.message)
            enriched = {
                "service_name": entry.service_name,
                "severity": entry.severity,
                "timestamp": entry.timestamp,
                **parsed,
            }
            parsed_batch.append(enriched)

            # Track template frequency updates in the dict
            template_id = parsed["template_id"]
            if template_id not in template_updates:
                template_updates[template_id] = {
                    "template_string": parsed["template"], "count": 0
                }
            template_updates[template_id]["count"] += 1

        # 1. Compress entire batch together
        compressed_blocks = compressor.compress_log_block(parsed_batch)

        # 2. Store all parsed raw logs (for live tail)
        if parsed_batch:
            await logs_collection.insert_many(parsed_batch)

        # 3. Store all new compressed blocks
        blocks_to_insert = [block for block in compressed_blocks.values()]
        if blocks_to_insert:
            await compressed_collection.insert_many(blocks_to_insert)

        # 4. Use BulkWrite to update all templates at once
        bulk_operations = []
        for t_id, data in template_updates.items():
            bulk_operations.append(
                UpdateOne(
                    {"_id": t_id},
                    {
                        "$set": {"template_string": data["template_string"]},
                        "$inc": {"frequency": data["count"]}
                    },
                    upsert=True
                )
            )
        
        if bulk_operations:
            await templates_collection.bulk_write(bulk_operations)

        logger.info(f"✅ Stored batch of {len(parsed_batch)} logs and {len(compressed_blocks)} blocks.")

        return {
            "status": "success",
            "message": f"{len(parsed_batch)} log entries parsed, compressed, and stored.",
        }
    except Exception as e:
        logger.error(f"Batch Ingestion Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------
# VIEW ALL KNOWN TEMPLATES (Corrected)
# -----------------------------------------------------------
@router.get("/templates", status_code=200)
async def get_templates():
    """
    Returns all known templates *from the database*.
    """
    templates_cursor = templates_collection.find(
        {}, {"_id": 1, "template_string": 1, "frequency": 1}
    ).sort("frequency", -1) # Sort by frequency
    
    templates = await templates_cursor.to_list(length=1000)
    
    return {"count": len(templates), "templates": templates}