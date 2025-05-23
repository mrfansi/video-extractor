import os
import shutil
import uuid
from pathlib import Path
from typing import Optional, Tuple
from fastapi import UploadFile
from app.core.config import settings

async def save_upload_file(upload_file: UploadFile) -> Tuple[str, str]:
    """Save an uploaded file to a temporary location
    
    Args:
        upload_file: The uploaded file
        
    Returns:
        Tuple containing the file path and job directory
    """
    # Create a unique job ID
    job_id = str(uuid.uuid4())
    job_dir = os.path.join(settings.temp_dir, job_id)
    os.makedirs(job_dir, exist_ok=True)
    
    # Create file path
    file_path = os.path.join(job_dir, upload_file.filename)
    
    # Save the file
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(upload_file.file, buffer)
    
    return file_path, job_dir

def get_file_size(file_path: str) -> int:
    """Get the size of a file in bytes
    
    Args:
        file_path: Path to the file
        
    Returns:
        Size of the file in bytes
    """
    return os.path.getsize(file_path)

def get_file_extension(filename: str) -> str:
    """Get the extension of a file
    
    Args:
        filename: Name of the file
        
    Returns:
        File extension (lowercase, with dot)
    """
    return os.path.splitext(filename)[1].lower()

def cleanup_temp_files(job_dir: str) -> None:
    """Clean up temporary files
    
    Args:
        job_dir: Path to the job directory
    """
    try:
        shutil.rmtree(job_dir)
    except Exception as e:
        print(f"Error cleaning up temporary files: {str(e)}")
