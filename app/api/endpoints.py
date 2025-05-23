from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Depends, status
from fastapi.responses import JSONResponse
from typing import Dict, List, Optional
import os
import shutil
import asyncio
import time
import uuid
from app.services.video_converter import VideoConverter
from app.services.storage import R2Storage
from app.core.config import settings
from app.utils.validators import validate_video_file
from app.utils.file_utils import save_upload_file, cleanup_temp_files, get_file_size
from app.core.logging import logger, log_video_conversion, log_conversion_complete, log_upload_start, log_upload_complete, log_error, log_performance

router = APIRouter(tags=["video"])

@router.post("/convert", 
          status_code=status.HTTP_200_OK, 
          summary="Convert video to optimized formats", 
          description="Upload a video file and convert it to multiple optimized formats (.mp4, .webm, .mov) while maintaining original resolution and visual quality. The optimized videos are uploaded to Cloudflare R2 and the response includes URLs for all formats.",
          responses={
              200: {
                  "description": "Video successfully converted and uploaded",
                  "content": {
                      "application/json": {
                          "example": {
                              "status": "success",
                              "message": "Video processed successfully",
                              "data": {
                                  "original": {
                                      "filename": "input.mp4",
                                      "size": 10485760,
                                      "resolution": "1920x1080"
                                  },
                                  "formats": {
                                      "mp4": {
                                          "filename": "output.mp4",
                                          "size": 5242880,
                                          "resolution": "1920x1080",
                                          "url": "https://example.com/mp4/output.mp4"
                                      },
                                      "webm": {
                                          "filename": "output.webm",
                                          "size": 4194304,
                                          "resolution": "1920x1080",
                                          "url": "https://example.com/webm/output.webm"
                                      },
                                      "mov": {
                                          "filename": "output.mov",
                                          "size": 6291456,
                                          "resolution": "1920x1080",
                                          "url": "https://example.com/mov/output.mov"
                                      }
                                  }
                              },
                              "metadata": {
                                  "original_size": 10485760,
                                  "total_output_size": 15728640,
                                  "compression_ratio": 0.67,
                                  "formats_count": 3
                              }
                          }
                      }
                  }
              },
              400: {
                  "description": "Bad request, invalid file",
                  "content": {
                      "application/json": {
                          "example": {
                              "detail": "Unsupported file type. File must be a valid video."
                          }
                      }
                  }
              },
              413: {
                  "description": "File too large",
                  "content": {
                      "application/json": {
                          "example": {
                              "detail": "File too large. Maximum size is 500MB"
                          }
                      }
                  }
              },
              500: {
                  "description": "Internal server error",
                  "content": {
                      "application/json": {
                          "example": {
                              "detail": "Error processing video: [error details]"
                          }
                      }
                  }
              }
          })
async def convert_video(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
):
    """Convert a video file to multiple optimized formats and upload to R2
    
    Args:
        background_tasks: FastAPI background tasks
        file: Uploaded video file
        
    Returns:
        JSON response with URLs of the uploaded videos
    """
    # Generate a unique job ID for tracking
    job_id = str(uuid.uuid4())
    start_time = time.time()
    
    logger.info(f"Starting video conversion job {job_id} for file {file.filename}")
    
    try:
        # Validate the uploaded file
        validation_start = time.time()
        await validate_video_file(file)
        validation_time = time.time() - validation_start
        log_performance("file_validation", validation_time * 1000, {"job_id": job_id, "filename": file.filename})
        
        # Check file size
        max_size = settings.max_upload_size_mb * 1024 * 1024  # Convert MB to bytes
        
        # Save the uploaded file to a temporary location
        save_start = time.time()
        temp_file, job_dir = await save_upload_file(file)
        save_time = time.time() - save_start
        log_performance("file_save", save_time * 1000, {"job_id": job_id, "filename": file.filename})
        
        # Check file size after saving
        file_size = get_file_size(temp_file)
        logger.info(f"File size for job {job_id}: {file_size} bytes ({file_size / (1024 * 1024):.2f} MB)")
        
        if file_size > max_size:
            # Clean up the file
            os.remove(temp_file)
            log_error("file_too_large", f"File too large. Maximum size is {settings.max_upload_size_mb}MB", {
                "job_id": job_id,
                "filename": file.filename,
                "file_size": file_size,
                "max_size": max_size
            })
            raise HTTPException(
                status_code=413,
                detail=f"File too large. Maximum size is {settings.max_upload_size_mb}MB"
            )
        
        # Initialize services
        converter = VideoConverter()
        storage = R2Storage()
        
        # Log conversion start
        log_video_conversion(temp_file, ["mp4", "webm", "mov"], job_id)
        
        # Ensure R2 directories exist
        dir_start = time.time()
        await storage.ensure_directories_exist()
        dir_time = time.time() - dir_start
        log_performance("ensure_directories", dir_time * 1000, {"job_id": job_id})
        
        # Process the video
        conversion_start = time.time()
        files_info, _ = await converter.process_video(temp_file)
        conversion_time = time.time() - conversion_start
        log_performance("video_conversion", conversion_time * 1000, {
            "job_id": job_id,
            "formats": list(files_info["formats"].keys())
        })
        
        # Log conversion complete
        total_output_size = sum(format_info["size"] for format_info in files_info["formats"].values())
        log_conversion_complete(job_id, files_info["formats"], file_size, total_output_size)
        
        # Upload the processed files to R2
        upload_start_time = time.time()
        log_upload_start(job_id, settings.r2_bucket_name, files_info["formats"])
        result = await storage.upload_files(files_info)
        upload_time = time.time() - upload_start_time
        log_performance("r2_upload", upload_time * 1000, {
            "job_id": job_id,
            "formats": list(files_info["formats"].keys())
        })
        log_upload_complete(job_id, settings.r2_bucket_name, files_info["formats"])
        
        # Add original file size to result
        result["original"]["size"] = file_size
        
        # Clean up temporary files in the background
        background_tasks.add_task(cleanup_temp_files, job_dir)
        
        # Calculate total processing time
        total_processing_time = time.time() - start_time
        log_performance("total_processing", total_processing_time * 1000, {
            "job_id": job_id,
            "filename": file.filename,
            "original_size": file_size,
            "formats_count": len(result["formats"])
        })
        
        # Calculate metadata
        total_output_size = sum(format_info["size"] for format_info in result["formats"].values())
        compression_ratio = round(file_size / total_output_size, 2) if total_output_size > 0 else 1
        
        # Log successful completion
        logger.info(f"Video conversion job {job_id} completed successfully in {total_processing_time:.2f} seconds")
        logger.info(f"Compression ratio for job {job_id}: {compression_ratio}")
        
        # Return the result with additional metadata
        return {
            "status": "success",
            "message": "Video processed successfully",
            "data": result,
            "metadata": {
                "job_id": job_id,
                "original_size": file_size,
                "total_output_size": total_output_size,
                "compression_ratio": compression_ratio,
                "formats_count": len(result["formats"]),
                "processing_time_seconds": round(total_processing_time, 2)
            }
        }
    except HTTPException as e:
        # Log HTTP exceptions
        log_error("http_exception", str(e), {
            "job_id": job_id,
            "status_code": e.status_code,
            "filename": file.filename if file else "unknown"
        })
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        # Log unexpected exceptions
        log_error("unexpected_error", str(e), {
            "job_id": job_id,
            "filename": file.filename if file else "unknown",
            "error_type": type(e).__name__
        })
        
        # Clean up temporary files in case of error
        if 'job_dir' in locals():
            cleanup_temp_files(job_dir)
        
        # Log full exception details
        logger.exception(f"Unhandled exception in job {job_id}: {str(e)}")
        
        # Raise HTTP exception
        raise HTTPException(
            status_code=500,
            detail=f"Error processing video: {str(e)}"
        )

@router.get("/health", 
          status_code=status.HTTP_200_OK, 
          summary="Health check", 
          description="Check if the API is running properly.",
          responses={
              200: {
                  "description": "API is healthy",
                  "content": {
                      "application/json": {
                          "example": {
                              "status": "ok"
                          }
                      }
                  }
              }
          })
async def health_check():
    """Health check endpoint"""
    return {"status": "ok"}

@router.get("/formats", 
          status_code=status.HTTP_200_OK, 
          summary="Get supported formats", 
          description="Get a list of supported input and output video formats.",
          responses={
              200: {
                  "description": "List of supported formats",
                  "content": {
                      "application/json": {
                          "example": {
                              "status": "success",
                              "data": {
                                  "input_formats": [".mp4", ".webm", ".mov", ".avi", ".mkv", ".mpeg", ".ogg"],
                                  "output_formats": [".mp4", ".webm", ".mov"]
                              }
                          }
                      }
                  }
              }
          })
async def supported_formats():
    """Get supported video formats"""
    return {
        "status": "success",
        "data": {
            "input_formats": [".mp4", ".webm", ".mov", ".avi", ".mkv", ".mpeg", ".ogg"],
            "output_formats": [".mp4", ".webm", ".mov"]
        }
    }
