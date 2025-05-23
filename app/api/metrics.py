from fastapi import APIRouter, Response
from loguru import logger
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator

from app.core.config import settings
from app.services.metrics_collector import metrics_collector

router = APIRouter()

# Initialize Prometheus instrumentator
instrumentator = Instrumentator().instrument()


def setup_instrumentator(app):
    """Set up the Prometheus instrumentator with the app."""
    if settings.ENABLE_METRICS:
        instrumentator.instrument(app).expose(app, include_in_schema=False)
        logger.info("Prometheus instrumentation enabled")


@router.get(
    "",
    summary="Prometheus metrics endpoint",
    description="Returns Prometheus metrics for monitoring",
    response_description="Prometheus metrics in text format",
    include_in_schema=True
)
async def metrics():
    """
    Prometheus metrics endpoint.
    
    Returns:
        Response: Prometheus metrics in text format
    """
    # Record request metric
    metrics_collector.record_request("/api/metrics")
    
    # Log metrics request
    logger.debug("Metrics requested")
    
    # Generate and return metrics
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )