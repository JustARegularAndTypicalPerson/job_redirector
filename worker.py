import os
import time
import json
import uuid
import datetime
import random
import redis

# --- Configuration ---
REDIS_URL = os.environ.get("REDIS_URL", "redis://34.60.28.143:6379/0")
JOB_QUEUE_KEY = "jobs:queue"
JOB_HASH_PREFIX = "job:"
WORKER_ID = f"worker-{uuid.uuid4()}"
# How long to block waiting for a job. 0 means wait forever.
QUEUE_TIMEOUT = 0

# --- Redis Connection ---
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
    print(f"[{WORKER_ID}] Successfully connected to Redis.")
except redis.exceptions.ConnectionError as e:
    print(f"[{WORKER_ID}] Error connecting to Redis: {e}")
    exit(1)

def execute_job(job_id: str, job_data: dict):
    """
    Simulates the execution of a job.
    In a real-world scenario, this function would contain your actual
    scraping or processing logic.
    """
    print(f"[{WORKER_ID}] Executing job {job_id}: {job_data['scraper']} - {job_data['operation_type']}")

    # Simulate work being done
    time.sleep(random.randint(3, 7))

    # Simulate success or failure
    if random.random() < 0.9:  # 90% success rate
        result = {
            "status": "success",
            "message": f"Scraped data for {job_data['target_id']}",
            "items_found": random.randint(10, 100)
        }
        return json.dumps(result), None
    else:
        error_message = "Failed to connect to target website (simulated error)."
        return None, error_message

def main_loop():
    """
    The main loop for the worker. It listens for jobs and processes them.
    """
    print(f"[{WORKER_ID}] Worker started. Listening for jobs on '{JOB_QUEUE_KEY}'...")
    while True:
        try:
            # Use BRPOP for an efficient blocking pop from the queue.
            # It returns a tuple: (queue_name, job_id) or None on timeout.
            message = redis_client.brpop(JOB_QUEUE_KEY, timeout=QUEUE_TIMEOUT)

            if message is None:
                continue

            _queue_name, job_id = message
            job_hash_key = f"{JOB_HASH_PREFIX}{job_id}"
            print(f"[{WORKER_ID}] Received job {job_id}")

            # --- Update job status to 'running' ---
            job_data = redis_client.hgetall(job_hash_key)
            if not job_data:
                print(f"[{WORKER_ID}] ERROR: Could not find job data for {job_id}. Skipping.")
                continue

            redis_client.hset(job_hash_key, mapping={
                "status": "running",
                "worker_id": WORKER_ID,
                "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })

            # --- Execute the job ---
            result_data, error_message = execute_job(job_id, job_data)

            # --- Update job with final result ---
            completion_payload = {
                "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            if error_message:
                completion_payload.update({"status": "failed", "error_message": error_message})
            else:
                completion_payload.update({"status": "completed", "result_data": result_data})

            redis_client.hset(job_hash_key, mapping=completion_payload)
            print(f"[{WORKER_ID}] Finished job {job_id} with status: {completion_payload['status']}")

        except Exception as e:
            print(f"[{WORKER_ID}] An unexpected error occurred: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main_loop()