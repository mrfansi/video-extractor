from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
import time
from app.api.endpoints import router as api_router
from app.core.config import settings

# Create FastAPI app
app = FastAPI(
    title="Video Extractor API",
    description="API for converting videos to optimized formats and uploading to Cloudflare R2",
    version="0.1.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add request ID middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

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
        "version": "0.1.0",
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
