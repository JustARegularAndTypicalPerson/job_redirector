import logging
from typing import Dict, Any
from yandex_redirector import run_yandex_operation
from gis_redirector import run_gis_operation

logger = logging.getLogger(__name__)

def run_job(job_id: str, job_data: dict) -> dict:
    job_type = job_data.get("scraper_type")
    operation_type = job_data.get("operation_type")
    if not job_type:
        logger.error("Job data missing 'scraper_type' (inferred from queue)", extra={"job_id": job_id})
        raise ValueError("Job data must contain 'scraper_type' (inferred from queue)")
    if not operation_type:
        logger.error("Job data missing 'operation_type' (inferred from queue)", extra={"job_id": job_id})
        raise ValueError("Job data must contain 'operation_type' (inferred from queue)")
    try:
        if job_type == "yandex":
            logger.info(f"Running Yandex job with id: {job_id}, operation: {operation_type}", extra={"job_id": job_id, "scraper_type": job_type, "operation_type": operation_type})
            return run_yandex_operation(job_id, job_data)
        elif job_type == "gis":
            logger.info(f"Running GIS job with id: {job_id}, operation: {operation_type}", extra={"job_id": job_id, "scraper_type": job_type, "operation_type": operation_type})
            return run_gis_operation(job_id, job_data)
        else:
            logger.error(f"Unknown job type: {job_type}", extra={"job_id": job_id, "scraper_type": job_type, "operation_type": operation_type})
            raise ValueError(f"Unknown job type: {job_type}")
    except Exception as e:
        logger.exception(f"Exception in run_job for job {job_id}", extra={"job_id": job_id, "scraper_type": job_type, "operation_type": operation_type})
        raise e