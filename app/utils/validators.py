from fastapi import HTTPException, UploadFile
from typing import List, Optional
import os
import magic
import time
from app.core.logging import logger, log_error, log_performance

# List of allowed video MIME types
ALLOWED_VIDEO_TYPES = [
    "video/mp4",
    "video/webm",
    "video/quicktime",  # MOV
    "video/x-msvideo",  # AVI
    "video/x-matroska",  # MKV
    "video/mpeg",
    "video/ogg"
]

async def validate_video_file(file: UploadFile) -> None:
    """Validate that the uploaded file is a valid video file
    
    Args:
        file: The uploaded file to validate
        
    Raises:
        HTTPException: If the file is not a valid video file
    """
    logger.info(f"Validating uploaded file: {file.filename if file else 'None'}")
    validation_start = time.time()
    
    # Check if file was provided
    if not file:
        error_message = "No file provided"
        log_error("validation_error", error_message, {
            "error_type": "missing_file"
        })
        logger.error(error_message)
        raise HTTPException(status_code=400, detail=error_message)
    
    # Check file extension
    file_ext = os.path.splitext(file.filename)[1].lower()
    logger.debug(f"Checking file extension: {file_ext}")
    
    if file_ext not in [".mp4", ".webm", ".mov", ".avi", ".mkv", ".mpeg", ".ogg"]:
        error_message = f"Unsupported file extension: {file_ext}. Supported extensions: .mp4, .webm, .mov, .avi, .mkv, .mpeg, .ogg"
        log_error("validation_error", error_message, {
            "error_type": "invalid_extension",
            "filename": file.filename,
            "extension": file_ext
        })
        logger.error(error_message)
        raise HTTPException(status_code=400, detail=error_message)
    
    # Read a small chunk of the file to detect MIME type
    logger.debug(f"Reading file chunk to detect MIME type")
    mime_start = time.time()
    content = await file.read(2048)
    mime_type = magic.from_buffer(content, mime=True)
    mime_time = time.time() - mime_start
    
    # Log MIME detection performance
    log_performance("mime_detection", mime_time * 1000, {
        "filename": file.filename,
        "mime_type": mime_type
    })
    
    logger.debug(f"Detected MIME type: {mime_type} for file {file.filename}")
    
    # Reset file position after reading
    await file.seek(0)
    
    # Check MIME type
    if mime_type not in ALLOWED_VIDEO_TYPES:
        error_message = f"Unsupported file type: {mime_type}. File must be a valid video."
        log_error("validation_error", error_message, {
            "error_type": "invalid_mime_type",
            "filename": file.filename,
            "mime_type": mime_type,
            "expected": ALLOWED_VIDEO_TYPES
        })
        logger.error(error_message)
        raise HTTPException(status_code=400, detail=error_message)
    
    # Calculate total validation time
    validation_time = time.time() - validation_start
    
    # Log successful validation
    log_performance("file_validation", validation_time * 1000, {
        "filename": file.filename,
        "extension": file_ext,
        "mime_type": mime_type
    })
    
    logger.info(f"Successfully validated file {file.filename} as {mime_type} in {validation_time:.2f} seconds")
