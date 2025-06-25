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
        logger.error("Job data missing 'operation_type'", extra={"job_id": job_id})
    
        return {
            "status": "failed",
            "result": None,
            "error_message": "Job data must contain 'operation_type' key"
        }
    
    try:
        logger.info(f"[GIS] Running operation '{operation_type}' for job {job_id}", extra={"job_id": job_id, "operation_type": operation_type})
        # Placeholder: Add logic for each operation_type
    
        if operation_type == "map":
            # TODO: Implement map operation
            return {"status": "success", "result": "map placeholder", "error_message": ""}
    
        elif operation_type == "analyze":
            # TODO: Implement analyze operation
            return {"status": "success", "result": "analyze placeholder", "error_message": ""}
    
        else:
            logger.error(f"Unknown operation '{operation_type}' for job {job_id}", extra={"job_id": job_id, "operation_type": operation_type})
            return {
                "status": "failed",
                "result": None,
                "error_message": f"GIS unknown operation '{operation_type}' for job {job_id}"
            }
    
    except Exception as e:
        logger.exception(f"Exception in GIS operation for job {job_id}", extra={"job_id": job_id, "operation_type": operation_type})
        return {
            "status": "failed",
            "result": None,
            "error_message": str(e)
        }