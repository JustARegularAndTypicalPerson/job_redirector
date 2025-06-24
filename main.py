import os
import time
import json
import uuid
import datetime
import random
import logging
import redis
from typing import Tuple, Optional

# --- Redis Stream Logging Handler ---
class RedisStreamHandler(logging.Handler):
    """
    A logging handler that sends log records to a Redis Stream.
    This allows a central service (like a web dashboard) to consume logs.
    """
    def __init__(self, redis_client, stream_key: str, worker_id: str):
        super().__init__()
        self.redis_client = redis_client
        self.stream_key = stream_key
        self.worker_id = worker_id

    def emit(self, record: logging.LogRecord):
        """Formats the log record into a dictionary and adds it to the Redis Stream."""
        try:
            # This structure is standardized with the API's log_message function.
            log_entry = {
                'message': self.format(record),
                'level': record.levelname,
                'source': 'worker',
                'worker_id': self.worker_id,
                'job_id': getattr(record, 'job_id', 'N/A'),
                'timestamp': datetime.datetime.fromtimestamp(record.created, tz=datetime.timezone.utc).isoformat(),
                'location': f"{record.module}.{record.funcName}:{record.lineno}",
            }
            # Use XADD with MAXLEN to cap the stream size and prevent it from growing indefinitely.
            self.redis_client.xadd(self.stream_key, log_entry, maxlen=5000, approximate=True)
        except Exception:
            self.handleError(record) # Fallback to stderr if Redis logging fails.

# --- Persistent Worker ID ---
WORKER_ID_FILE = ".worker_id"

def get_or_create_worker_id() -> str:
    """
    Retrieves the persistent worker ID from a file, or creates a new one.
    This ensures the worker has a stable identity across restarts.
    """
    if os.path.exists(WORKER_ID_FILE):
        try:
            with open(WORKER_ID_FILE, "r") as f:
                worker_id = f.read().strip()
                if worker_id:
                    print(f"Reusing existing worker ID: {worker_id}")
                    return worker_id
        except IOError as e:
            print(f"WARNING: Could not read worker ID file '{WORKER_ID_FILE}': {e}. A new ID will be generated.")

    # If file doesn't exist, is empty, or couldn't be read, create a new ID.
    new_worker_id = f"worker-{uuid.uuid4()}"
    print(f"Generating new worker ID: {new_worker_id}")
    try:
        with open(WORKER_ID_FILE, "w") as f:
            f.write(new_worker_id)
        print(f"Saved new worker ID to '{WORKER_ID_FILE}' for future runs.")
    except IOError as e:
        print(f"WARNING: Could not save worker ID to file '{WORKER_ID_FILE}': {e}")
        print(f"Using ephemeral worker ID for this session only.")

    return new_worker_id

# --- Configuration ---
# Load configuration from environment variables for production-readiness.
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
JOB_QUEUE_KEY = "jobs:queue"
JOB_HASH_PREFIX = "job:"
DEAD_LETTER_QUEUE_KEY = "jobs:dead-letter"
PROCESSING_QUEUE_PREFIX = "jobs:processing:"
LOG_STREAM_KEY = "logs:stream" # Standardized log stream key
WORKER_ID = get_or_create_worker_id()
# How long to block waiting for a job. 0 means wait forever.
QUEUE_TIMEOUT = 0

# --- Redis Connection ---
redis_client = None
try:
    # decode_responses=True ensures Redis returns strings, not bytes.
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
except redis.exceptions.ConnectionError as e:
    # Use basic logging for critical startup failures before full logger is configured.
    logging.basicConfig()
    logging.critical(f"CRITICAL: Could not connect to Redis: {e}")
    exit(1)

# --- Logging Setup ---
logger = logging.getLogger("worker")
logger.setLevel(logging.INFO)
logger.propagate = False # Prevent root logger from handling messages again

# Clear existing handlers to avoid duplicates during hot-reloads
if logger.hasHandlers():
    logger.handlers.clear()

# 1. Handler for printing to the console (for local development/debugging)
console_handler = logging.StreamHandler()
console_formatter = logging.Formatter(f'%(asctime)s [{WORKER_ID}] %(levelname)s: %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# 2. Custom Handler for sending logs to the Redis Stream
redis_handler = RedisStreamHandler(redis_client, LOG_STREAM_KEY, WORKER_ID)
logger.addHandler(redis_handler)

logger.info("Successfully connected to Redis.")

def recover_interrupted_jobs():
    """
    Moves any jobs from this worker's processing queue back to the main queue.
    This is crucial for recovering jobs that were being processed when a worker
    crashed, preventing them from being stuck in a "pending" or "running" state.
    """
    processing_queue_key = f"{PROCESSING_QUEUE_PREFIX}{WORKER_ID}"

    # Check if there's anything to recover to avoid logging noise
    if redis_client.llen(processing_queue_key) == 0:
        logger.info("No interrupted jobs to recover.")
        return

    logger.warning(f"Found interrupted job(s) in {processing_queue_key}. Re-queueing...")

    # Atomically move all jobs from the processing list back to the main queue.
    # RPOPLPUSH is atomic. If the worker crashes here, it will just resume on the next start.
    while job_id := redis_client.rpoplpush(processing_queue_key, JOB_QUEUE_KEY):
        logger.info(f"Re-queued job {job_id}.", extra={'job_id': job_id})
        # For now, simple re-queueing is sufficient. In a more advanced system,
        # you might check the job's status here before deciding to re-queue.
    logger.warning("Recovery complete.")

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
    logger.info(f"Executing job {job_id}: {job_data.get('scraper')} - {job_data.get('operation_type')}", extra={'job_id': job_id})

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
    processing_queue_key = f"{PROCESSING_QUEUE_PREFIX}{WORKER_ID}"
    logger.info(f"Worker started. Listening for jobs on '{JOB_QUEUE_KEY}'...")
    while True:
        job_id = None
        try:
            # Use BRPOPLPUSH for a reliable queue pattern. It atomically moves a job
            # from the main queue to a worker-specific processing queue.
            # If the worker crashes, the job remains in the processing queue and
            # can be recovered on the next startup.
            job_id = redis_client.brpoplpush(JOB_QUEUE_KEY, processing_queue_key, timeout=QUEUE_TIMEOUT)

            if job_id is None:
                # This happens if timeout is non-zero and no job arrives.
                continue

            job_hash_key = f"{JOB_HASH_PREFIX}{job_id}"
            logger.info(f"Received job {job_id}", extra={'job_id': job_id})

            # --- 1. Fetch job data and update status to 'running' ---
            job_data = redis_client.hgetall(job_hash_key)
            if not job_data:
                logger.error(f"Could not find job data for {job_id}. Skipping.", extra={'job_id': job_id})
                # Make sure to remove the job_id from the processing queue if its hash is missing
                redis_client.lrem(processing_queue_key, 1, job_id)
                continue

            # Safety check: Don't re-process a completed/failed job if it somehow re-appears in the queue.
            if job_data.get("status") not in ["pending"]:
                 logger.warning(f"Job {job_id} has status '{job_data.get('status')}' but was in queue. Skipping.", extra={'job_id': job_id})
                 redis_client.lrem(processing_queue_key, 1, job_id)
                 continue

            redis_client.hset(job_hash_key, mapping={
                "status": "running", # Changed from 'in_progress'
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
            logger.info(f"Finished job {job_id} with status: {completion_payload['status']}", extra={'job_id': job_id})

            # --- 4. Acknowledge completion by removing from processing queue ---
            # This is the final step. If the worker crashes before this,
            # the job ID remains in the processing queue for recovery on next start.
            redis_client.lrem(processing_queue_key, 1, job_id)
            
            recover_interrupted_jobs()
        except redis.exceptions.RedisError as e:
            logger.error(f"Redis error: {e}. Will retry connection in 5 seconds.", exc_info=True)
            time.sleep(5)
        except Exception as e:
            # This is a catch-all for unexpected errors in your job logic.
            logger.critical(f"An unhandled exception occurred while processing job {job_id}: {e}", exc_info=True, extra={'job_id': job_id})
            if job_id:
                # If a job caused the error, move it to the dead-letter queue to prevent crash loops.
                logger.warning(f"Moving job {job_id} to dead-letter queue.", extra={'job_id': job_id})
                try:
                    job_hash_key = f"{JOB_HASH_PREFIX}{job_id}"
                    error_payload = {
                        "status": "failed",
                        "error_message": f"Unhandled worker exception: {str(e)}",
                        "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    }
                    # Atomically update the hash and move the ID to the DLQ.
                    with redis_client.pipeline() as pipe:
                        pipe.hset(job_hash_key, mapping=error_payload)
                        pipe.lpush(DEAD_LETTER_QUEUE_KEY, job_id)
                        pipe.execute()

                    # After moving to DLQ, acknowledge it so it's not re-queued on restart
                    redis_client.lrem(processing_queue_key, 1, job_id)

                except redis.exceptions.RedisError as redis_err:
                    logger.critical(f"Could not move job {job_id} to dead-letter queue: {redis_err}", exc_info=True, extra={'job_id': job_id})
                    # Do NOT remove from processing queue here, as the DLQ move failed.
                    # It will be recovered on restart.

            # Wait before processing the next job to prevent fast-looping on non-job-related errors.
            time.sleep(5)

if __name__ == "__main__":
    recover_interrupted_jobs()
    main_loop()