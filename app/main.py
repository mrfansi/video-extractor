from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
import time
import json
from app.api.endpoints import router as api_router
from app.core.config import settings
from app.core.logging import logger, RequestLoggingMiddleware

# Create FastAPI app
app = FastAPI(
    title="Video Extractor API",
    description="""
    # Video Extractor API
    
    A Python-based API that accepts video files and converts them into multiple optimized formats
    while maintaining original resolution and visual quality but reducing file size.
    
    ## Features
    
    - Convert videos to multiple formats (.mp4, .webm, .mov)
    - Optimize videos using efficient codecs (H.264, VP9)
    - Maintain original resolution and visual quality
    - Reduce file size through advanced compression techniques
    - Upload optimized videos to Cloudflare R2 bucket
    - Store files in format-specific subdirectories
    - Support concurrent processing
    - Return JSON response with URLs of all uploaded videos
    
    ## Authentication
    
    This API currently does not require authentication.
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    contact={
        "name": "Muhammad Irfan",
        "email": "mrfansi@outlook.com",
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    }
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add request logging middleware
app.add_middleware(RequestLoggingMiddleware)

# Log application startup
logger.info(
    json.dumps({
        "event": "application_startup",
        "version": app.version,
        "environment": os.getenv("ENVIRONMENT", "development"),
        "host": settings.api_host,
        "port": settings.api_port,
        "workers": settings.api_workers,
        "api_prefix": settings.api_prefix
    })
)

# Exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "message": str(exc)
        }
    )

# Include API router
app.include_router(api_router, prefix=settings.api_prefix)

# Root endpoint
@app.get("/")
async def root():
    return {
        "name": "Video Extractor API",
        "version": "1.0.0",
        "description": "API for converting videos to optimized formats and uploading to Cloudflare R2"
    }

# Health check endpoint
@app.get("/health")
async def health():
    return {"status": "ok"}

# Create necessary directories
os.makedirs(settings.temp_dir, exist_ok=True)

# Run the application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        workers=settings.api_workers,
        reload=True
    )
