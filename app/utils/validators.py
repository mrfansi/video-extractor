from fastapi import HTTPException, UploadFile
from typing import List, Optional
import os
import magic

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
    # Check if file was provided
    if not file:
        raise HTTPException(status_code=400, detail="No file provided")
    
    # Check file extension
    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in [".mp4", ".webm", ".mov", ".avi", ".mkv", ".mpeg", ".ogg"]:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file extension: {file_ext}. Supported extensions: .mp4, .webm, .mov, .avi, .mkv, .mpeg, .ogg"
        )
    
    # Read a small chunk of the file to detect MIME type
    content = await file.read(2048)
    mime_type = magic.from_buffer(content, mime=True)
    
    # Reset file position after reading
    await file.seek(0)
    
    # Check MIME type
    if mime_type not in ALLOWED_VIDEO_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {mime_type}. File must be a valid video."
        )
