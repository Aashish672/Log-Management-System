from motor.motor_asyncio import AsyncIOMotorClient
import certifi
from app.core.config import settings

client = AsyncIOMotorClient(
    settings.MONGODB_URI,
    tls=True,
    tlsCAFile=certifi.where(),
)

db = client[settings.DB_NAME]

logs_collection = db["logs"]
templates_collection = db["templates"]
compressed_collection = db["compressed_blocks"]
anomalies_collection = db["anomalies"]
incidents_collection = db["incidents"]

async def create_indexes():
    # Logs
    #await logs_collection.create_index("template_id")
    try:
        await logs_collection.create_index("timestamp")
    except Exception as e:
        print("⚠️ Index creation skipped:", e)
    await logs_collection.create_index(
        [("template_id", 1), ("timestamp", -1)]
    )

    # Templates
    await templates_collection.create_index("frequency")

    # Anomalies
    await anomalies_collection.create_index("severity")
    await anomalies_collection.create_index("last_detected")

    # Incidents
    await incidents_collection.create_index("status")
    await incidents_collection.create_index(
        [("status", 1), ("last_updated", -1)]
    )

    print("✅ MongoDB indexes created")


