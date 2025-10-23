from fastapi import FastAPI, APIRouter, Body,HTTPException
from pydantic import BaseModel, Field
from typing import List, Dict, Any
from enum import Enum
import logging
import datetime

# -------------------------------------
# Setup Python Logging
# -------------------------------------
logger=logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

# -------------------------------------
# Severity Levels Enum
# -------------------------------------
class SeverityLevel(str,Enum):
    INFO="INFO"
    WARNING="WARNING"
    ERROR="ERROR"
    CRITICAl="CRITICAL"

# -------------------------------------
#Pydantic models to define the structure of incoming logs
# -------------------------------------
class LogEntry(BaseModel):
    service_name: str=Field(...,example="auth-service")
    severity: SeverityLevel=Field(...,example="ERROR")
    timestamp:datetime.datetime=Field(default_factory=datetime.datetime.utcnow)
    message: str=Field(...,example="User login failed for user 'admin'")

# -------------------------------------
#Initialize the router
# -------------------------------------
router=APIRouter()

# -------------------------------------
# Routes
# -------------------------------------

@router.post("/ingest",status_code=202)
async def ingest_log(log_entry: LogEntry):
    """
    Accepts a single log entry.
    """
    # TODO: Here we will send the log to a message queue(like Kafka/Rabbit<MQ)
    # or directly to the next processing module
    try:
        logger.info(f"Received single log: {log_entry.dict()}")
        return {"status":"success","message":"Log entry accepted"}
    except Exception as e:
        logger.error(f"Error ingesting log: {e}")
        raise HTTPException(status_code=500, detail="Failed to ingest log entry")
    

@router.post("/ingest/batch",status_code=202)
async def ingest_log_batch(log_entries: List[LogEntry]):
    """
    Accepts a batch of log entries. Supports real time and batch ingestion
    """
    #TODO: process the batch of logs
    try: 
        logger.info(f"Received batch of {len(log_entries)} logs.")
        for entry in log_entries:
            logger.info(f" - {entry.dict()}")
        return {"status":"success","message":f"{len(log_entries)} log entries accepted"}
    except Exception as e:
        logger.error(f"Error ingesting batch logs: {e}")
        raise HTTPException(status_code=500,detail="Failed to ingest batch logs")
