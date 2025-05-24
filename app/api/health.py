from fastapi import APIRouter, Depends, Query
from loguru import logger
from typing import Dict, Any, Optional

from app.models.job import get_stats
from app.schemas.video import HealthResponse
from app.services.metrics_collector import metrics_collector
from app.core.circuit_breaker import CircuitBreaker
from app.services.converter import VideoConverter

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


@router.get(
    "/circuit-breakers",
    response_model=Dict[str, Any],
    summary="Circuit breaker status",
    description="Returns the status of all circuit breakers in the system"
)
async def circuit_breaker_status():
    """
    Get the status of all circuit breakers in the system.
    
    This endpoint provides visibility into the circuit breakers' states,
    which helps diagnose issues with external services like R2 storage.
    
    Returns:
        Dict[str, Any]: Dictionary containing the status of all circuit breakers
    """
    # Record request metric
    metrics_collector.record_request("/api/health/circuit-breakers")
    
    # Log circuit breaker status check
    logger.debug("Circuit breaker status requested")
    
    # Get all circuit breaker states
    circuit_breaker_states = CircuitBreaker.get_all_states()
    
    # If no circuit breakers exist yet, return empty state
    if not circuit_breaker_states:
        return {
            "status": "ok",
            "message": "No circuit breakers initialized yet",
            "circuit_breakers": {}
        }
    
    # Check if any circuit breakers are open
    open_breakers = [
        name for name, state in circuit_breaker_states.items()
        if state["state"] == "OPEN"
    ]
    
    status = "warning" if open_breakers else "ok"
    message = (
        f"{len(open_breakers)} circuit breaker(s) open: {', '.join(open_breakers)}"
        if open_breakers else "All circuit breakers are closed"
    )
    
    return {
        "status": status,
        "message": message,
        "circuit_breakers": circuit_breaker_states
    }


@router.post(
    "/circuit-breakers/reset",
    response_model=Dict[str, Any],
    summary="Reset circuit breakers",
    description="Reset circuit breakers to closed state"
)
async def reset_circuit_breakers(
    service_name: Optional[str] = Query(None, description="Optional name of specific circuit breaker to reset")
):
    """
    Reset circuit breakers to closed state.
    
    This endpoint allows administrators to manually reset circuit breakers
    when external services have recovered from failures.
    
    Args:
        service_name: Optional name of specific circuit breaker to reset.
                     If not provided, all circuit breakers will be reset.
    
    Returns:
        Dict[str, Any]: Dictionary containing the reset status
    """
    # Record request metric
    metrics_collector.record_request("/api/health/circuit-breakers/reset")
    
    # Log circuit breaker reset
    if service_name:
        logger.info(f"Resetting circuit breaker: {service_name}")
    else:
        logger.info("Resetting all circuit breakers")
    
    # Reset circuit breakers
    reset_results = VideoConverter.reset_circuit_breakers(service_name)
    
    if not reset_results:
        return {
            "status": "warning",
            "message": "No circuit breakers found to reset",
            "reset_results": {}
        }
    
    return {
        "status": "ok",
        "message": f"Successfully reset {len(reset_results)} circuit breaker(s)",
        "reset_results": reset_results
    }