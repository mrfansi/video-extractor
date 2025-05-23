import time
from typing import Callable

from fastapi import FastAPI, Request, Response
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from app.core.config import settings


class ProcessTimeMiddleware(BaseHTTPMiddleware):
    """Middleware for logging request processing time."""
    
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        start_time = time.time()
        
        # Process the request
        response = await call_next(request)
        
        # Calculate processing time
        process_time = time.time() - start_time
        
        # Log request details and processing time
        logger.info(
            f"{request.method} {request.url.path} - "
            f"Status: {response.status_code} - "
            f"Process time: {process_time:.4f}s"
        )
        
        # Add processing time to response headers
        response.headers["X-Process-Time"] = str(process_time)
        
        return response


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Middleware for setting request ID in response headers."""
    
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        # Get request ID from request state if available
        request_id = request.state.request_id if hasattr(request.state, "request_id") else None
        
        # Process the request
        response = await call_next(request)
        
        # Add request ID to response headers if available
        if request_id:
            response.headers["X-Request-ID"] = str(request_id)
        
        return response


def setup_middlewares(app: FastAPI) -> None:
    """Configure middlewares for the FastAPI application."""
    
    # Add middleware for logging request processing time
    app.add_middleware(ProcessTimeMiddleware)
    
    # Add middleware for setting request ID in response headers
    app.add_middleware(RequestIDMiddleware)
    
    # Add CORS middleware if needed
    from fastapi.middleware.cors import CORSMiddleware
    
    # Configure CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Allows all origins for now, adjust for production
        allow_credentials=True,
        allow_methods=["*"],  # Allows all methods
        allow_headers=["*"],  # Allows all headers
    )
    
    # Add any additional middleware here
    
    logger.info("Middleware configuration completed")