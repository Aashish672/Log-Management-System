import requests
import random
import uuid
import datetime
import json
import time

# --- Configuration ---
API_URL = "http://127.0.0.1:8000/logs/ingest/batch"
NUM_LOGS = 2000
BATCH_SIZE = 500  # Send in batches of 500 to not overwhelm a single request

# --- Log Content ---
SERVICES = ["auth-service", "payment-service", "web-frontend", "data-pipeline"]
SEVERITIES = ["INFO", "WARNING", "ERROR"]

# Log templates designed to be caught by your template_parser.py
def get_random_log_message():
    """Generates a random log message based on defined templates."""
    
    # Helper to create a random IP
    def random_ip():
        return f"10.0.{random.randint(0, 255)}.{random.randint(0, 255)}"

    templates = [
        f"User login attempt for 'user_{random.randint(1,10)}' from IP {random_ip()}",
        f"Payment processed for order {uuid.uuid4()} amount {random.randint(10, 1000)} USD",
        f"GET /api/v2/user/{random.randint(100, 10000)} HTTP/1.1 200 OK",
        f"Failed to connect to database at {random_ip()}:5432",
        f"Data processing job {uuid.uuid4()} completed in {random.randint(150, 5000)}ms",
        f"Access denied for user 'guest' to resource /admin/config" # A template with no params
    ]
    return random.choice(templates)

# --- Main Script ---
def generate_and_send_logs():
    """Generates and sends logs in batches to the API."""
    
    print(f"🌍 Sending {NUM_LOGS} logs in batches of {BATCH_SIZE} to {API_URL}...")
    
    total_sent = 0
    start_time = time.time()
    
    while total_sent < NUM_LOGS:
        log_batch = []
        current_batch_size = min(BATCH_SIZE, NUM_LOGS - total_sent)
        
        print(f"\nGenerating batch of {current_batch_size} logs...")
        
        for _ in range(current_batch_size):
            log_entry = {
                "service_name": random.choice(SERVICES),
                "severity": random.choice(SEVERITIES),
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "message": get_random_log_message()
            }
            log_batch.append(log_entry)
            
        # Send the batch
        try:
            print(f"Sending batch... (Total: {total_sent + current_batch_size}/{NUM_LOGS})")
            response = requests.post(API_URL, json=log_batch)
            
            if response.status_code == 202:
                print(f"✅ Batch {total_sent // BATCH_SIZE + 1} accepted by server.")
                print(f"Server response: {response.json().get('message')}")
            else:
                print(f"❌ Error sending batch: {response.status_code}")
                print(f"Response: {response.text}")
                
        except requests.exceptions.ConnectionError as e:
            print(f"❌ Connection Failed: Is the server running at {API_URL}?")
            print(f"Error: {e}")
            return # Stop the script
            
        total_sent += current_batch_size

    end_time = time.time()
    print(f"\n--- Finished ---")
    print(f"Sent {total_sent} logs in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    generate_and_send_logs()
