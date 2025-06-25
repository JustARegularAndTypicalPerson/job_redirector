import logging
from yandex_redirector import run_yandex_operation
from gis_redirector import run_gis_operation

logger = logging.getLogger(__name__)

def run_job(job_id: str, job_data: dict) -> str:
    job_type = job_data.get("scraper_type")
    
    if not job_type:
        raise ValueError("Job data must contain 'scraper_type' key")
    
    if job_type == "yandex":
        logger.info(f"Running Yandex job with id: {job_id}")

        return run_yandex_operation(job_id, job_data)
    elif job_type == "gis":
        logger.info(f"Running GIS job with id: {job_id}")

        return run_gis_operation(job_id, job_data)
    else:
        raise ValueError(f"Unknown job type: {job_type}")