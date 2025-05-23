import os
import time
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile, status
from loguru import logger

from app.core.config import settings
from app.core.errors import FileUploadError, RequestNotFoundError, VideoProcessingError
from app.models.job import ConversionJob, JobStatus, create_job, get_job
from app.schemas.video import (
    ConversionCompletedResponse,
    ConversionErrorResponse,
    ConversionProcessingResponse,
    ConversionRequestResponse,
    ConversionStatusBase,
    FileMetadata,
    OptimizationLevel,
)
from app.services.converter import video_converter
from app.services.metrics_collector import metrics_collector

router = APIRouter()


@router.post(
    "",
    response_model=ConversionRequestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start video conversion",
    description="Upload a video file and start an asynchronous conversion process",
    response_description="Request accepted response with a unique request ID"
)
async def start_conversion(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    formats: Optional[str] = Form("mp4"),
    preserve_audio: Optional[bool] = Form(True),
    optimize_level: OptimizationLevel = Form(OptimizationLevel.BALANCED),
):
    """
    Upload a video file and start an asynchronous conversion process.
    
    Args:
        background_tasks: FastAPI BackgroundTasks
        file: Video file to convert
        formats: Comma-separated list of output formats (default: mp4)
        preserve_audio: Whether to preserve audio in the output (default: True)
        optimize_level: Level of optimization to apply (default: balanced)
        
    Returns:
        ConversionRequestResponse: Request accepted response with a unique request ID
    """
    # Record request metric
    metrics_collector.record_request("/api/convert")
    
    # Validate file size
    if file.size > settings.MAX_UPLOAD_SIZE_MB * 1024 * 1024:
        error_msg = (
            f"File size exceeds the maximum allowed size of "
            f"{settings.MAX_UPLOAD_SIZE_MB} MB"
        )
        logger.warning(f"File upload rejected: {error_msg}")
        raise FileUploadError(error_msg)
    
    # Validate file type
    content_type = file.content_type or ""
    if not content_type.startswith("video/"):
        error_msg = f"Unsupported file type: {content_type}"
        logger.warning(f"File upload rejected: {error_msg}")
        raise FileUploadError(error_msg)
    
    try:
        # Save the uploaded file to a temporary location
        temp_file_path, original_filename = await video_converter.save_upload_file(file)
        
        # Parse formats
        format_list = [fmt.strip().lower() for fmt in formats.split(",")]
        
        # Create a new conversion job
        job = ConversionJob(
            original_filename=original_filename,
            temp_file_path=temp_file_path,
            formats=format_list,
            preserve_audio=preserve_audio,
            optimize_level=optimize_level,
        )
        
        # Store the job
        create_job(job)
        
        # Schedule the job for processing in the background
        background_tasks.add_task(video_converter.process_job, job)
        
        # Log successful request
        logger.info(
            f"Conversion request accepted: job_id={job.id}, "
            f"formats={format_list}, optimize_level={optimize_level.value}"
        )
        
        # Return response with job ID
        return ConversionRequestResponse(
            status="processing",
            request_id=job.id,
            message="Conversion started. Monitor at /api/convert/{request_id}",
        )
        
    except Exception as e:
        # Log error
        logger.error(f"Error starting conversion: {str(e)}")
        
        # Clean up any temporary files
        if "temp_file_path" in locals() and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        
        # Raise exception for error handler
        raise VideoProcessingError(f"Failed to start conversion: {str(e)}")


@router.get(
    "/{request_id}",
    response_model=ConversionStatusBase,
    summary="Get conversion status",
    description="Check the status of a conversion request",
    response_description="Status response, which could be processing, completed, or error",
    responses={
        200: {
            "model": ConversionCompletedResponse,
            "description": "Conversion completed successfully",
        },
        202: {
            "model": ConversionProcessingResponse,
            "description": "Conversion is still processing",
        },
        404: {
            "model": ConversionErrorResponse,
            "description": "Request ID not found",
        },
        500: {
            "model": ConversionErrorResponse,
            "description": "Server error during conversion",
        },
    },
)
async def get_conversion_status(request_id: UUID):
    """
    Check the status of a conversion request.
    
    Args:
        request_id: Unique request ID
        
    Returns:
        ConversionStatusBase: Status response, which could be processing, completed, or error
    """
    # Record request metric
    metrics_collector.record_request(f"/api/convert/{request_id}")
    
    # Get job by ID
    job = get_job(str(request_id))
    
    # Check if job exists
    if not job:
        logger.warning(f"Request ID not found: {request_id}")
        raise RequestNotFoundError(str(request_id))
    
    # Check job status
    if job.status == JobStatus.PENDING or job.status == JobStatus.PROCESSING:
        # Job is still processing
        logger.info(f"Job {request_id} is still processing")
        return ConversionProcessingResponse(
            status="processing",
            message="Video is being processed.",
        )
    
    elif job.status == JobStatus.COMPLETED:
        # Job is completed
        logger.info(f"Job {request_id} is completed")
        
        # Record metrics
        for fmt, url in job.converted_files.items():
            # Record completion
            metrics_collector.record_completion(fmt)
            
            # Record processing time
            if job.get_processing_time():
                metrics_collector.record_processing_time(
                    fmt, job.get_processing_time()
                )
            
            # Record file sizes
            if job.original_size_mb:
                metrics_collector.record_file_size(
                    "original", fmt, job.original_size_mb * 1024 * 1024
                )
            
            if fmt in job.converted_sizes_mb:
                metrics_collector.record_file_size(
                    "converted", fmt, job.converted_sizes_mb[fmt] * 1024 * 1024
                )
                
                # Record compression ratio
                if job.original_size_mb and job.converted_sizes_mb[fmt] > 0:
                    ratio = job.converted_sizes_mb[fmt] / job.original_size_mb
                    metrics_collector.record_compression_ratio(fmt, ratio)
        
        # Return completed response
        return ConversionCompletedResponse(
            status="completed",
            converted_files=job.converted_files,
            metadata=FileMetadata(
                original_size_mb=job.original_size_mb,
                converted_sizes_mb=job.converted_sizes_mb,
                compression_ratio=job.compression_ratios,
            ),
        )
    
    elif job.status == JobStatus.FAILED:
        # Job failed
        logger.error(f"Job {request_id} failed: {job.error_message}")
        
        # Record failure metric
        metrics_collector.record_failure()
        
        # Return error response
        return ConversionErrorResponse(
            status="error",
            message=job.error_message or "Conversion failed",
        )
    
    else:
        # Unknown status
        logger.error(f"Job {request_id} has unknown status: {job.status}")
        return ConversionErrorResponse(
            status="error",
            message="Unknown job status",
        )


@router.get(
    "/{request_id}/logs",
    response_model=dict,
    summary="Get conversion logs",
    description="Get processing logs for a conversion request",
    response_description="Processing logs for the conversion",
    responses={
        404: {
            "model": ConversionErrorResponse,
            "description": "Request ID not found",
        },
    },
)
async def get_conversion_logs(request_id: UUID):
    """
    Get processing logs for a conversion request.
    
    Args:
        request_id: Unique request ID
        
    Returns:
        dict: Processing logs and other information
    """
    # Record request metric
    metrics_collector.record_request(f"/api/convert/{request_id}/logs")
    
    # Get job by ID
    job = get_job(str(request_id))
    
    # Check if job exists
    if not job:
        logger.warning(f"Request ID not found for logs: {request_id}")
        raise RequestNotFoundError(str(request_id))
    
    # Return job details
    return {
        "job_info": job.to_dict(),
        # In a real implementation, you would retrieve logs from a log store
        "logs": [
            f"Job created at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(job.created_at))}",
            f"Status: {job.status.value}",
            f"Processing time: {job.get_processing_time() or 'N/A'} seconds",
        ],
    }