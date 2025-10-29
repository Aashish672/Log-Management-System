from motor.motor_asyncio import AsyncIOMotorClient
from app.core.config import settings

client = AsyncIOMotorClient(settings.MONGODB_URI)
db = client[settings.DB_NAME]

logs_collection = db["logs"]
templates_collection = db["templates"]
compressed_collection = db["compressed_blocks"]
print("🧠 Connected to:", settings.MONGODB_URI)
print("🗄️ Using database:", settings.DB_NAME)