# main.py
import os
import time
import json
import uuid
import datetime
import random
import logging
import redis
from typing import Tuple, Optional
from job_runner import run_job

# --- BetterStack Logging Handler ---
try:
    from logtail import LogtailHandler

    LOGTAIL_TOKEN = os.environ.get("LOGTAIL_TOKEN")
    if LOGTAIL_TOKEN:
        logtail_handler = LogtailHandler(source_token=LOGTAIL_TOKEN)
        BETTERSTACK_HANDLER = logtail_handler
    else:
        BETTERSTACK_HANDLER = None
except ImportError:
    BETTERSTACK_HANDLER = None

# --- Redis Stream Logging Handler ---
class RedisStreamHandler(logging.Handler):
    def __init__(self, redis_client, stream_key: str, worker_id: str):
        super().__init__()
        self.redis_client = redis_client
        self.stream_key = stream_key
        self.worker_id = worker_id

    def emit(self, record: logging.LogRecord):
        try:
            log_entry = {
                'message': self.format(record),
                'level': record.levelname,
                'source': 'worker',
                'worker_id': self.worker_id,
                'job_id': getattr(record, 'job_id', 'N/A'),
                'timestamp': datetime.datetime.fromtimestamp(record.created, tz=datetime.timezone.utc).isoformat(),
                'location': f"{record.module}.{record.funcName}:{record.lineno}",
            }
            self.redis_client.xadd(self.stream_key, log_entry, maxlen=5000, approximate=True)
        except Exception:
            self.handleError(record)

WORKER_ID_FILE = ".worker_id"
FORBIDDEN_WORKERS_SET = "forbidden:workers"  # <--- Add this constant

def get_or_create_worker_id() -> str:
    if os.path.exists(WORKER_ID_FILE):
        try:
            with open(WORKER_ID_FILE, "r") as f:
                worker_id = f.read().strip()
                if worker_id:
                    print(f"Reusing existing worker ID: {worker_id}")
                    return worker_id
        except IOError as e:
            print(f"WARNING: Could not read worker ID file '{WORKER_ID_FILE}': {e}. A new ID will be generated.")
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

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
JOB_QUEUE_KEY = "jobs:queue"
JOB_HASH_PREFIX = "job:"
DEAD_LETTER_QUEUE_KEY = "jobs:dead-letter"
PROCESSING_QUEUE_PREFIX = "jobs:processing:"
LOG_STREAM_KEY = "logs:stream"
WORKER_ID = get_or_create_worker_id()
QUEUE_TIMEOUT = int(os.environ.get("QUEUE_TIMEOUT", 0))

redis_client = None
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
    print(f"[{WORKER_ID}] Successfully connected to Redis.")
except redis.exceptions.ConnectionError as e:
    logging.basicConfig()
    logging.critical(f"CRITICAL: Could not connect to Redis: {e}")
    exit(1)

logger = logging.getLogger("worker")
logger.setLevel(logging.INFO)
logger.propagate = False
if logger.hasHandlers():
    logger.handlers.clear()
console_handler = logging.StreamHandler()
console_formatter = logging.Formatter(f'%(asctime)s [{WORKER_ID}] %(levelname)s: %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)
redis_handler = RedisStreamHandler(redis_client, LOG_STREAM_KEY, WORKER_ID)
logger.addHandler(redis_handler)
if BETTERSTACK_HANDLER:
    logger.addHandler(BETTERSTACK_HANDLER)
    logger.info("BetterStack logging enabled.")
else:
    logger.info("BetterStack logging not enabled. Set LOGTAIL_TOKEN env var and install logtail.")
logger.info("Successfully connected to Redis.")

def is_worker_forbidden() -> bool:
    """Check if this worker is forbidden from processing jobs."""
    return redis_client.sismember(FORBIDDEN_WORKERS_SET, WORKER_ID)


def recover_interrupted_jobs() -> None:
    processing_queue_key: str = f"{PROCESSING_QUEUE_PREFIX}{WORKER_ID}"
    if redis_client.llen(processing_queue_key) == 0:
        logger.info("No interrupted jobs to recover.")
        return
    logger.warning(f"Found interrupted job(s) in {processing_queue_key}. Re-queueing...")
    while (job_id := redis_client.rpoplpush(processing_queue_key, JOB_QUEUE_KEY)):
        logger.info(f"Re-queued job {job_id}.", extra={'job_id': job_id})
    logger.warning("Recovery complete.")


def execute_job(job_id: str, job_data: dict) -> Tuple[Optional[str], Optional[str]]:
    logger.info(f"Executing job {job_id}: {job_data.get('scraper')} - {job_data.get('operation_type')}", extra={'job_id': job_id})
    try:
        util_result: dict = run_job(job_id, job_data)

        if not util_result:
            raise ValueError("Job execution returned no result.")
        
        result: dict = {
            "status": "success",
            "data": util_result
        }
        
        return json.dumps(result), None
    except Exception as e:
        logger.exception(f"Job {job_id} failed: {e}", extra={'job_id': job_id})
        result: dict = {
            "status": "failed",
            "data": util_result if 'util_result' in locals() else None,
            "error_message": str(e)
        }
        return json.dumps(result), str(e)


def main_loop() -> None:
    processing_queue_key: str = f"{PROCESSING_QUEUE_PREFIX}{WORKER_ID}"
    logger.info(f"Worker started. Listening for jobs on '{JOB_QUEUE_KEY}'...")
    while True:
        # --- Check if worker is forbidden ---
        if is_worker_forbidden():
            logger.warning(f"Worker {WORKER_ID} is forbidden from accepting jobs. Sleeping for 30s.")
            time.sleep(30)
            continue

        job_id = None
        try:
            job_id = redis_client.brpoplpush(JOB_QUEUE_KEY, processing_queue_key, timeout=QUEUE_TIMEOUT)
            if job_id is None:
                continue

            job_hash_key = f"{JOB_HASH_PREFIX}{job_id}"
            logger.info(f"Received job {job_id}", extra={'job_id': job_id})

            job_data = redis_client.hgetall(job_hash_key)
            if not job_data:
                logger.error(f"Could not find job data for {job_id}. Skipping.", extra={'job_id': job_id})
                redis_client.lrem(processing_queue_key, 1, job_id)
                continue

            # --- Check for cancellation before running ---
            if job_data.get("status") == "cancelled":
                logger.info(f"Job {job_id} was cancelled before execution. Marking as cancelled.", extra={'job_id': job_id})
                redis_client.hset(job_hash_key, mapping={
                    "status": "cancelled",
                    "worker_id": WORKER_ID,
                    "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "error_message": "Job was cancelled before execution."
                })
                redis_client.lrem(processing_queue_key, 1, job_id)
                continue

            # Safety check: Don't re-process a completed/failed/cancelled job if it somehow re-appears in the queue.
            if job_data.get("status") in ["completed", "failed", "cancelled"]:
                logger.warning(f"Job {job_id} has status '{job_data.get('status')}' but was in queue. Skipping.", extra={'job_id': job_id})
                redis_client.lrem(processing_queue_key, 1, job_id)
                continue

            redis_client.hset(job_hash_key, mapping={
                "status": "running", # Changed from 'in_progress'
                "worker_id": WORKER_ID,
                "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })

            result_data, error_message = execute_job(job_id, job_data)

            completion_payload = {
                "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            if error_message:
                completion_payload.update({"status": "failed", "error_message": error_message})
            else:
                completion_payload.update({"status": "completed", "result_data": result_data, "error_message": ""})

            redis_client.hset(job_hash_key, mapping=completion_payload)
            logger.info(f"Finished job {job_id} with status: {completion_payload['status']}", extra={'job_id': job_id})
            redis_client.lrem(processing_queue_key, 1, job_id)
            
            recover_interrupted_jobs()
        except redis.exceptions.RedisError as e:
            logger.error(f"Redis error: {e}. Will retry connection in 5 seconds.", exc_info=True)
            time.sleep(5)
        except Exception as e:
            logger.critical(f"An unhandled exception occurred while processing job {job_id}: {e}", exc_info=True, extra={'job_id': job_id})
            if job_id:
                logger.warning(f"Moving job {job_id} to dead-letter queue.", extra={'job_id': job_id})
                try:
                    job_hash_key = f"{JOB_HASH_PREFIX}{job_id}"
                    error_payload = {
                        "status": "failed",
                        "error_message": f"Unhandled worker exception: {str(e)}",
                        "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    }
                    with redis_client.pipeline() as pipe:
                        pipe.hset(job_hash_key, mapping=error_payload)
                        pipe.lpush(DEAD_LETTER_QUEUE_KEY, job_id)
                        pipe.execute()
                    redis_client.lrem(processing_queue_key, 1, job_id)
                except redis.exceptions.RedisError as redis_err:
                    logger.critical(f"Could not move job {job_id} to dead-letter queue: {redis_err}", exc_info=True, extra={'job_id': job_id})
            time.sleep(5)

if __name__ == "__main__":
    recover_interrupted_jobs()
    main_loop()
