import requests
import random
import time
import uuid
from datetime import datetime, timezone

# ================= CONFIG =================
API_URL = "http://127.0.0.1:8000/logs/ingest/batch"
TOTAL_LOGS = 100_000        # Scale for the paper (approx 20-40 seconds to run)
BATCH_SIZE = 500            # Increased batch size for higher throughput
TIMEOUT = 10
# ==========================================

TEMPLATES = [
    "User {} logged in from {}",
    "Payment of {} USD processed for order {}",
    "Failed login attempt for user {}",
    "Service {} responded with status {}",
    "Data processing job {} completed in {} ms",
    "Unauthorized access detected from IP {}"
]

SERVICES = ["auth-service", "payment-service", "data-service", "gateway"]
# Corrected Severity List
SEVERITIES = ["INFO", "WARNING", "ERROR", "CRITICAL"]

def random_ip():
    return f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"

def generate_log():
    template = random.choice(TEMPLATES)
    # Filling parameters
    message = template.format(
        random.randint(1, 9999),
        random_ip() if "{}" in template else random.randint(100, 5000)
    )

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service_name": random.choice(SERVICES),
        "severity": random.choice(SEVERITIES),
        "message": message,
        "host": f"node-{random.randint(1,5)}",
        "process_id": random.randint(1000, 9999),
        "request_id": str(uuid.uuid4())
    }

def send_batch(batch):
    try:
        response = requests.post(API_URL, json=batch, timeout=TIMEOUT)
        return 200 <= response.status_code < 300
    except Exception:
        return False

def main():
    print(f"🚀 Starting PRODUCTION LOAD TEST")
    print(f"Target: {TOTAL_LOGS} logs")
    print(f"Batch Size: {BATCH_SIZE}")
    print("-----------------------------------")

    start_time = time.time()
    sent_count = 0
    failure_count = 0

    # Calculate total batches needed
    total_batches = TOTAL_LOGS // BATCH_SIZE
    
    for i in range(total_batches):
        batch = [generate_log() for _ in range(BATCH_SIZE)]
        success = send_batch(batch)

        if success:
            sent_count += len(batch)
            # Print a dot every batch to show it's alive
            if i % 10 == 0: 
                print(f"[{sent_count}/{TOTAL_LOGS}] logs sent...", end="\r")
        else:
            failure_count += len(batch)
            print("x", end="", flush=True)

    end_time = time.time()
    elapsed = end_time - start_time
    rate = sent_count / elapsed if elapsed > 0 else 0

    print("\n\n✅ ================= RESULTS =================")
    print(f"Total Logs Sent   : {sent_count}")
    print(f"Failed Logs       : {failure_count}")
    print(f"Total Time        : {elapsed:.2f} seconds")
    print(f"Throughput        : {rate:.2f} logs/second")
    print("============================================")

if __name__ == "__main__":
    main()