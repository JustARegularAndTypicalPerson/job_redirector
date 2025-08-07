# main.py
import os
import sys
import time
import json
import uuid
import datetime
import random
import logging
import redis
import signal
import atexit
import threading
from typing import Tuple, Optional
from job_runner import run_job
from scrapers import gis_scraper

# --- BetterStack Logging Handler ---
try:
    from logtail import LogtailHandler

    LOGTAIL_TOKEN = os.environ.get("LOGTAIL_TOKEN", "9MH3pdR6MXAidoMv7KHJT7b9")
    INGESTION_HOST = os.environ.get("INGESTION_HOST", "https://s1355760.eu-nbg-2.betterstackdata.com")
    if LOGTAIL_TOKEN:
        logtail_handler = LogtailHandler(source_token=LOGTAIL_TOKEN,host=INGESTION_HOST)
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
FORBIDDEN_WORKERS_SET = "forbidden:workers"

# Generate or get worker ID early for logging
if os.path.exists(WORKER_ID_FILE):
    try:
        with open(WORKER_ID_FILE, "r") as f:
            worker_id = f.read().strip()
            if worker_id:
                WORKER_ID = worker_id
            else:
                WORKER_ID = f"worker-{uuid.uuid4()}"
    except IOError:
        WORKER_ID = f"worker-{uuid.uuid4()}"
else:
    WORKER_ID = f"worker-{uuid.uuid4()}"

# Set up root logger and attach handlers
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
if root_logger.hasHandlers():
    root_logger.handlers.clear()
console_handler = logging.StreamHandler()
console_formatter = logging.Formatter(f'%(asctime)s [{WORKER_ID}] %(levelname)s: %(message)s')
console_handler.setFormatter(console_formatter)
root_logger.addHandler(console_handler)

# Use module-level logger for this file
logger = logging.getLogger(__name__)

REDIS_URL = os.environ.get("REDIS_URL", "redis://34.60.28.143:6379/0")
REDIS_PASSWORD = "N7r$5pX@f9vZq2!Lb9#T9iha967aY*&^)@!^"
JOB_QUEUE_KEY = "jobs:queue"
JOB_HASH_PREFIX = "job:"
DEAD_LETTER_QUEUE_KEY = "jobs:dead-letter"
PROCESSING_QUEUE_PREFIX = "jobs:processing:"
LOG_STREAM_KEY = "logs:stream"
QUEUE_TIMEOUT = int(os.environ.get("QUEUE_TIMEOUT", 0))

# After WORKER_ID is set, continue with Redis and other setup
redis_client = None
try:
    redis_client = redis.from_url(
        REDIS_URL,
        password=REDIS_PASSWORD,
        decode_responses=True
    )
    redis_client.ping()
    print(f"[{WORKER_ID}] Successfully connected to Redis.")
except redis.exceptions.ConnectionError as e:
    logging.critical(f"CRITICAL: Could not connect to Redis: {e}")
    sys.exit(1)


# Add Redis and BetterStack handlers after redis_client is available
redis_handler = RedisStreamHandler(redis_client, LOG_STREAM_KEY, WORKER_ID)
root_logger.addHandler(redis_handler)
if BETTERSTACK_HANDLER:
    root_logger.addHandler(BETTERSTACK_HANDLER)
    root_logger.info("BetterStack logging enabled.")
else:
    root_logger.info("BetterStack logging not enabled. Set LOGTAIL_TOKEN env var and install logtail.")
root_logger.info("Successfully connected to Redis.")

# --- Job ID Context Propagation ---
_job_id_local = threading.local()

def set_job_id(job_id: str):
    _job_id_local.job_id = job_id

def get_job_id() -> str:
    return getattr(_job_id_local, 'job_id', None)

class JobIdLogFilter(logging.Filter):
    def filter(self, record):
        # If job_id is not set in the record, try to get it from thread-local
        if not hasattr(record, 'job_id') or record.job_id is None:
            record.job_id = get_job_id() or 'N/A'
        return True

# Add the filter to all handlers
for handler in root_logger.handlers:
    handler.addFilter(JobIdLogFilter())

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
        logger.info(f"Re-queued job {job_id}.")
    logger.warning("Recovery complete.")


def execute_job(job_id: str, job_data: dict) -> dict:
    """
    Executes the job by calling the appropriate redirector.
    Returns the result dictionary from the redirector.
    """
    set_job_id(job_id)  # Set job_id for log context
    logger.info(f"Executing job {job_id}: {job_data.get('scraper_type')} - {job_data.get('operation_type')}")
    try:
        result_dict: dict = run_job(job_id, job_data)

        if not result_dict:
            raise ValueError("Job execution returned no result.")
        
        return result_dict
    except Exception as e:
        logger.exception(f"Job {job_id} failed during execution: {e}")
        return {
            "status": "failed",
            "result": None,
            "error_message": f"Unhandled exception in worker: {str(e)}"
        }
    finally:
        set_job_id(None)  # Clear job_id after job


# --- Queue Selection Helpers ---
def get_allowed_queues(worker_id: str) -> list:
    """Fetch all allowed queue names from Redis set 'queues:all', excluding forbidden ones for this worker."""
    queue_names = redis_client.smembers("queues:all")
    allowed_queues = []
    for name in queue_names:
        if not redis_client.sismember(f"queue:{name}:forbidden_workers", worker_id):
            allowed_queues.append(f"jobs:queue:{name}")
    # Always include the default queue if not forbidden
    if not redis_client.sismember("queue:default:forbidden_workers", worker_id):
        allowed_queues.append("jobs:queue")
    return allowed_queues


def main_loop() -> None:
    processing_queue_key: str = f"{PROCESSING_QUEUE_PREFIX}{WORKER_ID}"
    logger.info(f"Worker started. Listening for jobs on all allowed queues...")
    while True:
        # --- Check if worker is globally forbidden ---
        if is_worker_forbidden():
            logger.warning(f"Worker {WORKER_ID} is forbidden from accepting jobs. Sleeping for 30s.")
            time.sleep(30)
            continue

        allowed_queues = get_allowed_queues(WORKER_ID)
        if not allowed_queues:
            logger.warning(f"Worker {WORKER_ID} is not allowed on any queue. Sleeping for 30s.")
            time.sleep(30)
            continue

        job_id = None
        try:
            # Use BRPOP to atomically pop from all allowed queues
            brpop_result = redis_client.brpop(allowed_queues, timeout=QUEUE_TIMEOUT)
            if not brpop_result:
                continue
            queue_name, job_id = brpop_result
            # Push job_id to processing queue for this worker
            redis_client.lpush(processing_queue_key, job_id)

            job_hash_key = f"{JOB_HASH_PREFIX}{job_id}"
            logger.info(f"Received job {job_id} from {queue_name}")

            job_data = redis_client.hgetall(job_hash_key)
            if not job_data:
                logger.error(f"Could not find job data for {job_id}. Skipping.")
                redis_client.lrem(processing_queue_key, 1, job_id)
                continue

            # --- Check for cancellation before running ---
            if job_data.get("status") == "cancelled":
                logger.info(f"Job {job_id} was cancelled before execution. Marking as cancelled.")
                redis_client.hset(job_hash_key, mapping={
                    "status": "cancelled",
                    "worker_id": WORKER_ID,
                    "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "error_message": "Job was cancelled before execution."
                })
                redis_client.lrem(processing_queue_key, 1, job_id)
                continue

            # Don't re-process completed/failed/cancelled jobs
            if job_data.get("status") in ["completed", "failed", "cancelled"]:
                logger.warning(f"Job {job_id} has status '{job_data.get('status')}' but was in queue. Skipping.")
                redis_client.lrem(processing_queue_key, 1, job_id)
                continue

            # --- Mark job as in_progress and register worker ---
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            redis_client.hset(job_hash_key, mapping={
                "status": "in_progress",
                "worker_id": WORKER_ID,
                "started_at": now_iso
            })
            redis_client.sadd("workers:all", WORKER_ID)

            # Infer scraper_type and operation_type from queue name
            # jobs:queue:yandex:statistics → yandex, statistics
            if queue_name == "jobs:queue" or queue_name.endswith(":default"):
                scraper_type = "gis"
                operation_type = job_data.get("operation_type")
            else:
                parts = queue_name.split(":")
                if len(parts) >= 4:
                    scraper_type = parts[2]
                    operation_type = parts[3]
                else:
                    scraper_type = parts[-1]
                    operation_type = job_data.get("operation_type")
            job_data["scraper_type"] = scraper_type
            job_data["operation_type"] = operation_type

            job_outcome = execute_job(job_id, job_data)

            internal_status = job_outcome.get("status", "failed")
            result_data = job_outcome.get("result")
            error_message = job_outcome.get("error_message", "")

            # Map internal status to final job status in Redis
            if internal_status == "success":
                final_job_status = "completed"
            elif internal_status in ["warning", "captcha_required", "failed"]:
                final_job_status = internal_status
            else:  # any other unknown status from redirector
                final_job_status = "failed"
                if not error_message:
                    error_message = f"Job finished with unhandled internal status: {internal_status}"

            completion_payload = {
                "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "status": final_job_status,
                "result_data": json.dumps(result_data, ensure_ascii=False) if result_data is not None else None,
                "error_message": str(error_message) if error_message is not None else ""
            }

            redis_client.hset(job_hash_key, mapping=completion_payload)
            logger.info(f"Finished job {job_id} with status: {completion_payload['status']}")
            redis_client.lrem(processing_queue_key, 1, job_id)

            recover_interrupted_jobs()
        except redis.exceptions.RedisError as e:
            logger.error(f"Redis error: {e}. Will retry connection in 5 seconds.", exc_info=True)
            time.sleep(5)
        except Exception as e:
            logger.critical(f"An unhandled exception occurred while processing job {job_id}: {e}", exc_info=True)
            if job_id:
                logger.warning(f"Moving job {job_id} to dead-letter queue.")
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
                    logger.critical(f"Could not move job {job_id} to dead-letter queue: {redis_err}", exc_info=True)
            time.sleep(5)

def flush_and_close_log_handlers():
    for handler in root_logger.handlers:
        try:
            handler.flush()
        except Exception:
            pass
        # Always flush, but only close non-console handlers
        if not (isinstance(handler, logging.StreamHandler) and getattr(handler, 'stream', None) in (None, getattr(__import__('sys'), 'stdout', None), getattr(__import__('sys'), 'stderr', None))):
            try:
                handler.close()
            except Exception:
                pass

# Log on normal exit
atexit.register(lambda: root_logger.critical(f"Worker {WORKER_ID} exiting (atexit)."))
atexit.register(flush_and_close_log_handlers)

def handle_sigterm(signum, frame):
    root_logger.critical(f"Worker {WORKER_ID} received signal {signum}, shutting down.")
    flush_and_close_log_handlers()
    exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

if __name__ == "__main__":
    try:
        recover_interrupted_jobs()
        # gis_scraper.get_reviews({"target_id": 70000001040930142})  # Ensure GIS scraper is initialized
        main_loop()
    except Exception as e:
        root_logger.critical(f"Worker {WORKER_ID} crashed: {e}", exc_info=True)
        flush_and_close_log_handlers()
        raise
