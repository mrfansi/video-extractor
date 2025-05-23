from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Depends, status
from fastapi.responses import JSONResponse
from typing import Dict, List, Optional
import os
import shutil
import asyncio
from app.services.video_converter import VideoConverter
from app.services.storage import R2Storage
from app.core.config import settings
from app.utils.validators import validate_video_file
from app.utils.file_utils import save_upload_file, cleanup_temp_files, get_file_size

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
    try:
        # Validate the uploaded file
        await validate_video_file(file)
        
        # Check file size
        max_size = settings.max_upload_size_mb * 1024 * 1024  # Convert MB to bytes
        
        # Save the uploaded file to a temporary location
        temp_file, job_dir = await save_upload_file(file)
        
        # Check file size after saving
        file_size = get_file_size(temp_file)
        if file_size > max_size:
            # Clean up the file
            os.remove(temp_file)
            raise HTTPException(
                status_code=413,
                detail=f"File too large. Maximum size is {settings.max_upload_size_mb}MB"
            )
        
        # Initialize services
        converter = VideoConverter()
        storage = R2Storage()
        
        # Ensure R2 directories exist
        await storage.ensure_directories_exist()
        
        # Process the video
        files_info, _ = await converter.process_video(temp_file)
        
        # Upload the processed files to R2
        result = await storage.upload_files(files_info)
        
        # Add original file size to result
        result["original"]["size"] = file_size
        
        # Clean up temporary files in the background
        background_tasks.add_task(cleanup_temp_files, job_dir)
        
        # Return the result with additional metadata
        return {
            "status": "success",
            "message": "Video processed successfully",
            "data": result,
            "metadata": {
                "original_size": file_size,
                "total_output_size": sum(format_info["size"] for format_info in result["formats"].values()),
                "compression_ratio": round(file_size / sum(format_info["size"] for format_info in result["formats"].values()), 2) if sum(format_info["size"] for format_info in result["formats"].values()) > 0 else 1,
                "formats_count": len(result["formats"])
            }
        }
    except HTTPException as e:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        # Clean up temporary files in case of error
        if 'job_dir' in locals():
            cleanup_temp_files(job_dir)
        
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
