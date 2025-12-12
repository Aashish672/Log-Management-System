import requests
import random
import uuid
import datetime
import time
import sys

# --- Configuration ---
API_URL = "http://127.0.0.1:8000/logs/ingest/batch"
NUM_LOGS = 2000
BATCH_SIZE = 500  # send in batches
REQUEST_TIMEOUT = 5  # seconds
MAX_RETRIES = 3
RETRY_BACKOFF = 1.0  # base seconds, exponential

# --- Log Content ---
SERVICES = ["auth-service", "payment-service", "web-frontend", "data-pipeline"]
SEVERITIES = ["INFO", "WARNING", "ERROR"]  # note: "CRITICAL" used for anomalies below

def get_random_ip():
    return f"10.0.{random.randint(0,255)}.{random.randint(0,255)}"

NORMAL_TEMPLATES = [
    lambda: f"User login attempt for 'user_{random.randint(1,10)}' from IP {get_random_ip()}",
    lambda: f"Payment processed for order {uuid.uuid4()} amount {random.randint(10,1000)} USD",
    lambda: f"GET /api/v2/user/{random.randint(100,10000)} HTTP/1.1 200 OK",
    lambda: f"Data processing job {uuid.uuid4()} completed in {random.randint(150,500)} ms",
]

ANOMALY_TEMPLATES = [
    lambda: f"CRITICAL: Database connection failed at {get_random_ip()}:5432 timeout",
    lambda: f"Security Alert: Multiple failed login attempts for 'admin' from {get_random_ip()}",
    lambda: f"Payment gateway timeout: 503 Service Unavailable for txn {uuid.uuid4()}",
]

def utc_now_isoz():
    # explicit UTC timestamp with 'Z'
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def generate_batch(size, anomaly_mode=False, anomaly_prob=0.8):
    batch = []
    for _ in range(size):
        if anomaly_mode and random.random() < anomaly_prob:
            msg = random.choice(ANOMALY_TEMPLATES)()
            severity = "CRITICAL"  # keep CRITICAL for anomalies
        else:
            msg = random.choice(NORMAL_TEMPLATES)()
            severity = random.choice(SEVERITIES)

        log_entry = {
            "log_id": str(uuid.uuid4()),
            "service_name": random.choice(SERVICES),
            "severity": severity,
            "timestamp": utc_now_isoz(),
            "message": msg,
            # optional fields helpful for ingestion & debugging:
            "host": f"host-{random.randint(1,10)}",
            "process_id": random.randint(1000,9999),
        }
        batch.append(log_entry)
    return batch

def send_batch(batch):
    headers = {"Content-Type": "application/json"}
    payload = batch
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(API_URL, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
            if 200 <= resp.status_code < 300:
                print(f"Sent {len(batch)} logs. Status: {resp.status_code}")
                return True
            else:
                print(f"Server returned {resp.status_code}: {resp.text}")
        except requests.RequestException as e:
            print(f"Request error (attempt {attempt}): {e}")

        # retry/backoff
        if attempt < MAX_RETRIES:
            backoff = RETRY_BACKOFF * (2 ** (attempt - 1))
            print(f"Retrying in {backoff:.1f}s...")
            time.sleep(backoff)
    print("Failed to send batch after retries.")
    return False

def main():
    print("--- Cloud Log Generator ---")
    print("1. Generate Normal Traffic")
    print("2. Generate ANOMALY Spike")
    choice = input("Select mode (1 or 2): ").strip()

    anomaly_mode = (choice == "2")
    total_logs = NUM_LOGS

    print(f"\nStarting generation... (Anomaly Mode: {anomaly_mode})")
    start_time = time.time()
    sent = 0
    batch_count = 0

    while sent < total_logs:
        current_batch_size = min(BATCH_SIZE, total_logs - sent)
        batch = generate_batch(current_batch_size, anomaly_mode=anomaly_mode)
        ok = send_batch(batch)
        batch_count += 1
        if not ok:
            # If a batch fails after retries, we continue but you may choose to abort.
            print(f"Warning: batch {batch_count} failed to deliver.")
        sent += current_batch_size
        # gentle pacing
        time.sleep(0.5)

    elapsed = time.time() - start_time
    print(f"\nFinished sending ~{sent} logs in {elapsed:.2f}s")

if __name__ == "__main__":
    main()
