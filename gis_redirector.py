import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

def run_gis_operation(job_id: str, job_data: dict) -> Dict[str, Any]:
    """
    Handles GIS job operations based on operation_type.
    Returns a dict with status, result, and error_message fields for consistency.
    """
    operation_type = job_data.get("operation_type")
    if not operation_type:
        logger.error("Job data missing 'operation_type' (should be inferred from queue)", extra={"job_id": job_id})
        
        raise ValueError("Job data missing 'operation_type' (should be inferred from queue)")
    
    try:
        logger.info(f"[GIS] Running operation '{operation_type}' for job {job_id}", extra={"job_id": job_id, "operation_type": operation_type})
        result = None
        
        if operation_type == "statistics":
            from scrapers.gis_scraper import get_statistics
            result = get_statistics(job_data)
        elif operation_type == "reviews_data":
            from scrapers.gis_scraper import get_reviews_data
            result = get_reviews_data(job_data)
        elif operation_type == "reviews":
            from scrapers.gis_scraper import get_reviews
            result = get_reviews(job_data)
        elif operation_type == "send_answer":
            from scrapers.gis_scraper import send_answer
            result = send_answer(job_data)
        elif operation_type == "complain_about_a_review":
            from scrapers.gis_scraper import complain_about_a_review
            result = complain_about_a_review(job_data)
        elif operation_type == "mark_as_main":
            from scrapers.gis_scraper import mark_as_main
            result = mark_as_main(job_data)
        else:
            logger.error(f"Unknown operation '{operation_type}' for job {job_id}", extra={"job_id": job_id, "operation_type": operation_type})
            raise ValueError(f"Unknown operation type: {operation_type}")

        is_empty = False
        if result is dict:
            if result.get("result"):
                return {"status": result["result"], "result": result}
        if operation_type == "statistics":
            if result and not result.get("total_displays") and not result.get("daily_statistics"):
                is_empty = True
        elif operation_type in ["reviews_data", "reviews"]:
            if not result:
                is_empty = True

        if is_empty:
            logger.warning(f"GIS operation '{operation_type}' for job {job_id} returned empty/zero result.", extra={"job_id": job_id, "operation_type": operation_type})
            return {"status": "warning", "result": result, "error_message": "Operation completed successfully but returned no data."}

        return {"status": "success", "result": result, "error_message": ""}

    except Exception as e:
        logger.exception(f"Exception in GIS operation for job {job_id}", extra={"job_id": job_id, "operation_type": operation_type})
        return {"status": "failed", "result": None, "error_message": str(e)}