from motor.motor_asyncio import AsyncIOMotorClient
import certifi
from app.core.config import settings

# Use certifi CA bundle to fix SSL handshake issues on Windows/Python 3.12
client = AsyncIOMotorClient(
    settings.MONGODB_URI,
    tls=True,
    tlsCAFile=certifi.where(),
)

db = client[settings.DB_NAME]

logs_collection = db["logs"]
templates_collection = db["templates"]
compressed_collection = db["compressed_blocks"]

print("🧠 Connected to:", settings.MONGODB_URI)
print("🗄️ Using database:", settings.DB_NAME)
