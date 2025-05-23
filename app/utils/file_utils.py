import os
import shutil
import uuid
import time
from pathlib import Path
from typing import Optional, Tuple
from fastapi import UploadFile
from app.core.config import settings
from app.core.logging import logger, log_error, log_performance

async def save_upload_file(upload_file: UploadFile) -> Tuple[str, str]:
    """Save an uploaded file to a temporary location
    
    Args:
        upload_file: The uploaded file
        
    Returns:
        Tuple containing the file path and job directory
    """
    logger.info(f"Saving uploaded file: {upload_file.filename}")
    save_start_time = time.time()
    
    # Create a unique job ID
    job_id = str(uuid.uuid4())
    job_dir = os.path.join(settings.temp_dir, job_id)
    
    try:
        # Create directory if it doesn't exist
        dir_start_time = time.time()
        os.makedirs(job_dir, exist_ok=True)
        dir_time = time.time() - dir_start_time
        log_performance("create_temp_dir", dir_time * 1000, {
            "job_dir": job_dir
        })
        
        # Create file path
        file_path = os.path.join(job_dir, upload_file.filename)
        logger.debug(f"Saving file to: {file_path}")
        
        # Save the file
        write_start_time = time.time()
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(upload_file.file, buffer)
        write_time = time.time() - write_start_time
        
        # Get file size for logging
        file_size = os.path.getsize(file_path)
        
        # Log performance metrics
        log_performance("file_write", write_time * 1000, {
            "file_path": file_path,
            "file_size": file_size,
            "write_speed_bytes_per_sec": file_size / write_time if write_time > 0 else 0
        })
        
        # Log total save time
        total_save_time = time.time() - save_start_time
        logger.info(f"Successfully saved file {upload_file.filename} ({file_size} bytes) to {file_path} in {total_save_time:.2f} seconds")
        
        return file_path, job_dir
    except Exception as e:
        error_message = f"Error saving uploaded file: {str(e)}"
        log_error("file_save_error", error_message, {
            "filename": upload_file.filename,
            "job_dir": job_dir,
            "error_type": type(e).__name__
        })
        logger.error(error_message)
        raise

def get_file_size(file_path: str) -> int:
    """Get the size of a file in bytes
    
    Args:
        file_path: Path to the file
        
    Returns:
        Size of the file in bytes
    """
    try:
        size = os.path.getsize(file_path)
        logger.debug(f"File size for {file_path}: {size} bytes ({size / (1024 * 1024):.2f} MB)")
        return size
    except Exception as e:
        error_message = f"Error getting file size for {file_path}: {str(e)}"
        log_error("file_size_error", error_message, {
            "file_path": file_path,
            "error_type": type(e).__name__
        })
        logger.error(error_message)
        raise

def get_file_extension(filename: str) -> str:
    """Get the extension of a file
    
    Args:
        filename: Name of the file
        
    Returns:
        File extension (lowercase, with dot)
    """
    extension = os.path.splitext(filename)[1].lower()
    logger.debug(f"File extension for {filename}: {extension}")
    return extension

def cleanup_temp_files(job_dir: str) -> None:
    """Clean up temporary files
    
    Args:
        job_dir: Path to the job directory
    """
    logger.info(f"Cleaning up temporary directory: {job_dir}")
    cleanup_start = time.time()
    
    try:
        # Get directory size before deletion for logging
        dir_size = sum(os.path.getsize(os.path.join(dirpath, filename)) 
                      for dirpath, _, filenames in os.walk(job_dir) 
                      for filename in filenames)
        
        # Count files before deletion
        file_count = sum(len(files) for _, _, files in os.walk(job_dir))
        
        # Delete the directory
        shutil.rmtree(job_dir)
        
        # Calculate cleanup time
        cleanup_time = time.time() - cleanup_start
        
        # Log performance
        log_performance("cleanup_temp_files", cleanup_time * 1000, {
            "job_dir": job_dir,
            "dir_size_bytes": dir_size,
            "file_count": file_count
        })
        
        logger.info(f"Successfully cleaned up {file_count} files ({dir_size} bytes) from {job_dir} in {cleanup_time:.2f} seconds")
    except Exception as e:
        error_message = f"Error cleaning up temporary files: {str(e)}"
        log_error("cleanup_error", error_message, {
            "job_dir": job_dir,
            "error_type": type(e).__name__
        })
        logger.error(error_message)
