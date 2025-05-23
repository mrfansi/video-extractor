import os
import sys
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union

from loguru import logger
from pythonjsonlogger.jsonlogger import JsonFormatter
from app.core.config import settings

# Create logs directory if it doesn't exist
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# Configure log file paths
access_log_file = logs_dir / "access.log"
error_log_file = logs_dir / "error.log"
app_log_file = logs_dir / "app.log"

# Define log levels
LOG_LEVEL_MAP = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}

# Get log level from environment or default to INFO
log_level = os.getenv("LOG_LEVEL", "info").lower()
LOG_LEVEL = LOG_LEVEL_MAP.get(log_level, logging.INFO)

# Custom JSON formatter for structured logging
class CustomJsonFormatter(JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        
        # Add timestamp in ISO format
        log_record["timestamp"] = datetime.utcnow().isoformat()
        
        # Add log level
        log_record["level"] = record.levelname
        
        # Add source location
        log_record["module"] = record.module
        log_record["function"] = record.funcName
        log_record["line"] = record.lineno
        
        # Add process and thread info
        log_record["process_id"] = record.process
        log_record["thread_id"] = record.thread
        
        # Add environment info
        log_record["environment"] = os.getenv("ENVIRONMENT", "development")

# Configure loguru logger
def configure_logger():
    # Remove default handler
    logger.remove()
    
    # Add console handler with color formatting
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=LOG_LEVEL,
        colorize=True,
    )
    
    # Add file handler for all logs with JSON formatting
    logger.add(
        app_log_file,
        format="{message}",
        level=LOG_LEVEL,
        rotation="10 MB",  # Rotate when file reaches 10MB
        retention="1 week",  # Keep logs for 1 week
        compression="gz",  # Compress rotated logs
        serialize=True,  # Enable JSON serialization
    )
    
    # Add file handler for error logs
    logger.add(
        error_log_file,
        format="{message}",
        level="ERROR",
        rotation="10 MB",
        retention="1 month",  # Keep error logs longer
        compression="gz",
        serialize=True,
    )
    
    # Add file handler for access logs
    logger.add(
        access_log_file,
        format="{message}",
        level="INFO",
        rotation="10 MB",
        retention="1 week",
        compression="gz",
        serialize=True,
        filter=lambda record: "access" in record["extra"],
    )
    
    return logger

# Configure standard logging for libraries that use it
def configure_standard_logging():
    # Create JSON formatter
    formatter = CustomJsonFormatter(
        "%(timestamp)s %(level)s %(name)s %(module)s %(funcName)s %(lineno)d %(message)s"
    )
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(LOG_LEVEL)
    
    # Remove existing handlers
    for handler in root_logger.handlers:
        root_logger.removeHandler(handler)
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(LOG_LEVEL)
    root_logger.addHandler(console_handler)
    
    # Add file handler
    file_handler = logging.FileHandler(app_log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(LOG_LEVEL)
    root_logger.addHandler(file_handler)
    
    # Configure uvicorn access logger
    access_logger = logging.getLogger("uvicorn.access")
    access_logger.setLevel(logging.INFO)
    
    # Add access log file handler
    access_handler = logging.FileHandler(access_log_file)
    access_handler.setFormatter(formatter)
    access_logger.addHandler(access_handler)
    
    # Configure uvicorn error logger
    error_logger = logging.getLogger("uvicorn.error")
    error_logger.setLevel(logging.ERROR)
    
    # Add error log file handler
    error_handler = logging.FileHandler(error_log_file)
    error_handler.setFormatter(formatter)
    error_logger.addHandler(error_handler)

# Initialize loggers
logger = configure_logger()
configure_standard_logging()

# Request logging middleware
class RequestLoggingMiddleware:
    def __init__(self, app):
        self.app = app
    
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            # If not an HTTP request, just pass through
            await self.app(scope, receive, send)
            return
        
        # Record start time
        start_time = time.time()
        
        # Generate request ID
        request_id = f"{int(time.time() * 1000)}-{os.getpid()}"
        
        # Extract request details from scope
        method = scope.get("method", "")
        path = scope.get("path", "")
        client = scope.get("client", ("", 0))
        client_ip = client[0] if client else "unknown"
        
        # Get headers
        headers = dict(scope.get("headers", []))
        user_agent = headers.get(b"user-agent", b"").decode("utf-8", errors="replace")
        
        # Log request
        logger.bind(access=True).info(
            json.dumps({
                "type": "request",
                "request_id": request_id,
                "method": method,
                "path": path,
                "client_ip": client_ip,
                "user_agent": user_agent,
            })
        )
        
        # Create a wrapper for send to intercept the response status
        response_status = [200]  # Default status code
        
        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                # Capture the status code
                response_status[0] = message["status"]
                
                # Add custom headers
                process_time = time.time() - start_time
                message["headers"].append((b"X-Process-Time", str(process_time).encode()))
                message["headers"].append((b"X-Request-ID", request_id.encode()))
            
            await send(message)
        
        # Process request
        try:
            await self.app(scope, receive, send_wrapper)
            
            # Calculate processing time
            process_time = time.time() - start_time
            
            # Log response
            logger.bind(access=True).info(
                json.dumps({
                    "type": "response",
                    "request_id": request_id,
                    "status_code": response_status[0],
                    "process_time_ms": round(process_time * 1000, 2),
                    "path": path,
                    "method": method,
                })
            )
        except Exception as e:
            # Log exception
            logger.error(
                f"Error processing request: {str(e)}",
                exc_info=True,
            )
            # Re-raise the exception for FastAPI to handle
            raise

# Helper functions for logging
def log_video_conversion(video_file: str, output_formats: list, job_id: str):
    """Log video conversion start"""
    logger.info(
        json.dumps({
            "event": "video_conversion_start",
            "job_id": job_id,
            "video_file": video_file,
            "output_formats": output_formats,
            "timestamp": datetime.utcnow().isoformat(),
        })
    )

def log_conversion_complete(job_id: str, formats: Dict[str, Any], original_size: int, total_output_size: int):
    """Log video conversion completion"""
    logger.info(
        json.dumps({
            "event": "video_conversion_complete",
            "job_id": job_id,
            "formats": list(formats.keys()),
            "original_size": original_size,
            "total_output_size": total_output_size,
            "compression_ratio": round(original_size / total_output_size, 2) if total_output_size > 0 else 1,
            "timestamp": datetime.utcnow().isoformat(),
        })
    )

def log_upload_start(job_id: str, bucket: str, formats: Dict[str, Any]):
    """Log upload start"""
    logger.info(
        json.dumps({
            "event": "upload_start",
            "job_id": job_id,
            "bucket": bucket,
            "formats": list(formats.keys()),
            "timestamp": datetime.utcnow().isoformat(),
        })
    )

def log_upload_complete(job_id: str, bucket: str, formats: Dict[str, Any]):
    """Log upload completion"""
    logger.info(
        json.dumps({
            "event": "upload_complete",
            "job_id": job_id,
            "bucket": bucket,
            "formats": list(formats.keys()),
            "timestamp": datetime.utcnow().isoformat(),
        })
    )

def log_error(error_type: str, message: str, context: Optional[Dict[str, Any]] = None):
    """Log error with context"""
    error_data = {
        "error_type": error_type,
        "message": message,
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    if context:
        error_data.update(context)
    
    logger.error(json.dumps(error_data))

def log_performance(operation: str, duration_ms: float, context: Optional[Dict[str, Any]] = None):
    """Log performance metrics"""
    perf_data = {
        "event": "performance",
        "operation": operation,
        "duration_ms": duration_ms,
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    if context:
        perf_data.update(context)
    
    logger.info(json.dumps(perf_data))
