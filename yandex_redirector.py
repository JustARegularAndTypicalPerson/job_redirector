import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

def run_yandex_operation(job_id: str, job_data: dict) -> Dict[str, Any]:
    """
    Handles Yandex job operations based on operation_type.
    Returns a dict with status, result, and error_message fields for consistency.
    """
    operation_type = job_data.get("operation_type")

    if not operation_type:
        logger.error("Job data missing 'operation_type'", extra={"job_id": job_id})

        return {
            "status": "failed",
            "result": None,
            "error_message": "Job data must contain 'operation_type' key"
        }

    try:
        logger.info(f"[Yandex] Running operation '{operation_type}' for job {job_id}", extra={"job_id": job_id, "operation_type": operation_type})
        # Placeholder: Add logic for each operation_type
        
        if operation_type == "statistics":
            from scrapers.yandex_scraper import get_statistics

            result = get_statistics(job_data)

            return {"status": "success", "result": result, "error_message": ""}
        
        elif operation_type == "reviews":
            from scrapers.yandex_scraper import get_reviews

            result = get_reviews(job_data)            

        elif operation_type == "competitors":
            from scrapers.yandex_scraper import get_competitors

            result = get_competitors(job_data)

            return {"status": "success", "result": result, "error_message": ""}
        else:
            logger.error(f"Unknown operation '{operation_type}' for job {job_id}", extra={"job_id": job_id, "operation_type": operation_type})
            
            raise ValueError(f"Unknown operation type: {operation_type}")
    
    except Exception as e:
    
        logger.exception(f"Exception in Yandex operation for job {job_id}", extra={"job_id": job_id, "operation_type": operation_type})
    
        raise e