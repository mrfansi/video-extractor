from fastapi import APIRouter, Depends
from loguru import logger

from app.models.job import get_stats
from app.schemas.video import HealthResponse
from app.services.metrics_collector import metrics_collector

router = APIRouter()


@router.get(
    "",
    response_model=HealthResponse,
    summary="Health check endpoint",
    description="Returns the health status of the API"
)
async def health_check():
    """
    Basic health check endpoint.
    
    Returns:
        HealthResponse: Health status response
    """
    # Record request metric
    metrics_collector.record_request("/api/health")
    
    # Log health check
    logger.debug("Health check requested")
    
    # Get job stats
    job_stats = get_stats()
    
    # Update job metrics
    metrics_collector.update_jobs_gauge({
        "pending": job_stats["pending"],
        "processing": job_stats["processing"],
        "completed": job_stats["completed"],
        "failed": job_stats["failed"],
    })
    
    return HealthResponse(
        status="ok",
        message="video-extractor API is healthy"
    )