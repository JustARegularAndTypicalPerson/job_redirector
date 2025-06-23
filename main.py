# main.py
import os
import time
import json
import uuid
import datetime
import random
import redis
from typing import Tuple, Optional

# --- Configuration ---
# Load configuration from environment variables for production-readiness.
REDIS_URL = os.environ.get("REDIS_URL", "redis://34.60.28.143:6379/0")
JOB_QUEUE_KEY = "jobs:queue"
JOB_HASH_PREFIX = "job:"
DEAD_LETTER_QUEUE_KEY = "jobs:dead-letter"
WORKER_ID = f"worker-{uuid.uuid4()}"
# How long to block waiting for a job. 0 means wait forever.
QUEUE_TIMEOUT = 0

# --- Redis Connection ---
try:
    # decode_responses=True ensures Redis returns strings, not bytes.
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
    print(f"[{WORKER_ID}] Successfully connected to Redis.")
except redis.exceptions.ConnectionError as e:
    print(f"[{WORKER_ID}] CRITICAL: Could not connect to Redis: {e}")
    exit(1)

def execute_job(job_id: str, job_data: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Executes the actual job logic.

    In a real-world scenario, this function would contain your actual
    scraping or processing logic based on the job_data.

    Args:
        job_id: The ID of the job being executed.
        job_data: A dictionary containing the full job payload from Redis.

    Returns:
        A tuple of (result_data, error_message).
        - On success: (json_string_of_results, None)
        - On failure: (None, "A descriptive error message")
    """
    print(f"[{WORKER_ID}] Executing job {job_id}: {job_data.get('scraper')} - {job_data.get('operation_type')}")

    # --- YOUR ACTUAL JOB LOGIC GOES HERE ---
    # Example:
    # scraper_type = job_data.get('scraper')
    # target = job_data.get('target_id')
    # if scraper_type == 'social_media':
    #     from my_scrapers import social_media_scraper
    #     return social_media_scraper.get_posts(target)
    # else:
    #     return None, f"Unknown scraper type: {scraper_type}"
    # -----------------------------------------

    # Simulate work being done
    time.sleep(random.randint(2, 5))

    # Simulate success or failure
    if random.random() < 0.9:  # 90% success rate
        result = {
            "status": "success",
            "message": f"Scraped data for target '{job_data['target_id']}'",
            "items_found": random.randint(10, 100)
        }
        # On success, return a JSON string of the result and None for the error.
        return json.dumps(result), None
    else:
        # On failure, return None for the result and a descriptive error message.
        error_message = "Failed to connect to target website (simulated error)."
        return None, error_message

def main_loop():
    """
    The main loop for the worker. It listens for jobs and processes them.
    """
    print(f"[{WORKER_ID}] Worker started. Listening for jobs on '{JOB_QUEUE_KEY}'...")
    while True:
        job_id = None
        try:
            # Use BRPOP for an efficient blocking pop from the queue.
            # It returns a tuple: (queue_name, job_id) or None on timeout.
            message = redis_client.brpop(JOB_QUEUE_KEY, timeout=QUEUE_TIMEOUT)

            if message is None:
                # This happens if timeout is non-zero and no job arrives.
                continue

            _queue_name, job_id = message
            job_hash_key = f"{JOB_HASH_PREFIX}{job_id}"
            print(f"[{WORKER_ID}] Received job {job_id}")

            # --- 1. Fetch job data and update status to 'running' ---
            job_data = redis_client.hgetall(job_hash_key)
            if not job_data:
                print(f"[{WORKER_ID}] ERROR: Could not find job data for {job_id}. Skipping.")
                continue

            # Safety check: Don't re-process a completed/failed job if it somehow re-appears in the queue.
            if job_data.get("status") not in ["pending", "retrying"]:
                 print(f"[{WORKER_ID}] WARN: Job {job_id} has status '{job_data.get('status')}' but was in queue. Skipping.")
                 continue

            redis_client.hset(job_hash_key, mapping={
                "status": "running",
                "worker_id": WORKER_ID,
                "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })

            # --- 2. Execute the job ---
            result_data, error_message = execute_job(job_id, job_data)

            # --- 3. Update job with final result ---
            completion_payload = {
                "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            if error_message:
                completion_payload.update({"status": "failed", "error_message": error_message})
            else:
                completion_payload.update({"status": "completed", "result_data": result_data, "error_message": ""})

            redis_client.hset(job_hash_key, mapping=completion_payload)
            print(f"[{WORKER_ID}] Finished job {job_id} with status: {completion_payload['status']}")

        except redis.exceptions.RedisError as e:
            print(f"[{WORKER_ID}] Redis error: {e}. Will retry connection in 5 seconds.")
            time.sleep(5)
        except Exception as e:
            # This is a catch-all for unexpected errors in your job logic.
            print(f"[{WORKER_ID}] FATAL: An unhandled exception occurred while processing job {job_id}: {e}")
            if job_id:
                # If a job caused the error, move it to the dead-letter queue to prevent crash loops.
                print(f"[{WORKER_ID}] Moving job {job_id} to dead-letter queue.")
                try:
                    job_hash_key = f"{JOB_HASH_PREFIX}{job_id}"
                    error_payload = {
                        "status": "failed",
                        "error_message": f"Unhandled worker exception: {str(e)}",
                        "failed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    }
                    # Atomically update the hash and move the ID to the DLQ.
                    with redis_client.pipeline() as pipe:
                        pipe.hset(job_hash_key, mapping=error_payload)
                        pipe.lpush(DEAD_LETTER_QUEUE_KEY, job_id)
                        pipe.execute()
                except redis.exceptions.RedisError as redis_err:
                    print(f"[{WORKER_ID}] CRITICAL: Could not move job {job_id} to dead-letter queue: {redis_err}")
            
            # Wait before processing the next job to prevent fast-looping on non-job-related errors.
            time.sleep(5)

if __name__ == "__main__":
    main_loop()
