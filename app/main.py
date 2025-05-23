import os
import shutil
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from loguru import logger

from app.api import health, metrics, video
from app.core.config import settings
from app.core.errors import setup_exception_handlers
from app.core.logging import setup_logging
from app.core.middleware import setup_middlewares
from app.core.patches import apply_patches
from app.api.metrics import setup_instrumentator


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events."""
    # Startup
    logger.info("Application starting up")
    
    # Apply patches to fix deprecation warnings
    apply_patches()
    
    # Ensure temp directory exists
    os.makedirs(settings.TEMP_DIR, exist_ok=True)
    
    # Log configuration
    logger.info(f"Max upload size: {settings.MAX_UPLOAD_SIZE_MB} MB")
    logger.info(f"Max workers: {settings.MAX_WORKERS}")
    logger.info(f"Supported formats: {', '.join(settings.SUPPORTED_FORMATS)}")
    logger.info(f"Metrics enabled: {settings.ENABLE_METRICS}")
    
    yield  # This is where the application runs
    
    # Shutdown
    logger.info("Application shutting down")
    
    # Perform cleanup if needed
    try:
        # Clean up temp directory
        temp_path = Path(settings.TEMP_DIR)
        if temp_path.exists():
            for file_path in temp_path.glob('*'):
                if file_path.is_file():
                    file_path.unlink()
            
            logger.info(f"Temp directory cleaned up: {settings.TEMP_DIR}")
    except Exception as e:
        logger.error(f"Error during shutdown cleanup: {str(e)}")


def create_application() -> FastAPI:
    """
    Create and configure the FastAPI application.
    
    Returns:
        FastAPI: Configured FastAPI application
    """
    # Set up logging
    setup_logging()
    
    # Create FastAPI application
    application = FastAPI(
        title="Video Extractor API",
        description="A FastAPI application for video conversion and optimization",
        version="1.0.0",
        docs_url=f"{settings.API_PREFIX}/docs",
        redoc_url=f"{settings.API_PREFIX}/redoc",
        openapi_url=f"{settings.API_PREFIX}/openapi.json",
        lifespan=lifespan,
    )
    
    # Set up middlewares
    setup_middlewares(application)
    
    # Set up exception handlers
    setup_exception_handlers(application)
    
    # Set up Prometheus instrumentation
    if settings.ENABLE_METRICS:
        setup_instrumentator(application)
    
    # Include API routers
    application.include_router(
        video.router,
        prefix=f"{settings.API_PREFIX}/convert",
        tags=["Video Conversion"],
    )
    
    application.include_router(
        health.router,
        prefix=f"{settings.API_PREFIX}/health",
        tags=["Health"],
    )
    
    application.include_router(
        metrics.router,
        prefix=f"{settings.API_PREFIX}/metrics",
        tags=["Metrics"],
    )
    
    # Log application startup
    logger.info(f"Application initialized with API prefix: {settings.API_PREFIX}")
    
    return application


app = create_application()