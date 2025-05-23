import os
import shutil
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import ffmpeg
from loguru import logger

from app.core.config import settings
from app.core.errors import VideoProcessingError
from app.models.job import ConversionJob, JobStatus
from app.schemas.video import OptimizationLevel
from app.services.r2_uploader import r2_uploader


class VideoConverter:
    """Service for converting videos using FFmpeg."""
    
    def __init__(self):
        """Initialize the video converter."""
        self.temp_dir = Path(settings.TEMP_DIR)
        self.max_workers = settings.MAX_WORKERS
        
        # Ensure temp directory exists
        if not self.temp_dir.exists():
            self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Worker pool for concurrent processing
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        
        # Thread lock for thread-safe operations
        self.lock = threading.Lock()
        
        logger.info(f"VideoConverter initialized with {self.max_workers} workers")
    
    def save_upload_file(self, file) -> Tuple[str, str]:
        """
        Save an uploaded file to a temporary location.
        
        Args:
            file: FastAPI UploadFile object
            
        Returns:
            Tuple containing the temporary file path and original filename
        """
        try:
            # Create a unique filename
            unique_id = uuid.uuid4()
            original_filename = file.filename
            
            # Extract file extension
            _, file_extension = os.path.splitext(original_filename)
            
            # Create temporary file
            temp_file_path = os.path.join(
                self.temp_dir, f"{unique_id}{file_extension}"
            )
            
            # Save uploaded file to temporary location
            with open(temp_file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            
            logger.info(f"Saved uploaded file to {temp_file_path}")
            
            return temp_file_path, original_filename
        
        except Exception as e:
            logger.error(f"Failed to save uploaded file: {str(e)}")
            raise VideoProcessingError(f"Failed to save uploaded file: {str(e)}")
    
    def get_file_size_mb(self, file_path: str) -> float:
        """Get file size in megabytes."""
        return os.path.getsize(file_path) / (1024 * 1024)
    
    def validate_file(self, file_path: str) -> dict:
        """
        Validate a video file and get its metadata.
        
        Args:
            file_path: Path to the video file
            
        Returns:
            Dictionary containing video metadata
        """
        try:
            probe = ffmpeg.probe(file_path)
            video_info = next(
                (stream for stream in probe["streams"] if stream["codec_type"] == "video"),
                None,
            )
            
            if not video_info:
                raise VideoProcessingError("No video stream found in the file")
            
            # Extract basic metadata
            metadata = {
                "format": probe["format"]["format_name"],
                "duration": float(probe["format"].get("duration", 0)),
                "size_mb": float(probe["format"].get("size", 0)) / (1024 * 1024),
                "video_codec": video_info.get("codec_name", "unknown"),
                "width": int(video_info.get("width", 0)),
                "height": int(video_info.get("height", 0)),
            }
            
            # Check for audio stream
            audio_info = next(
                (stream for stream in probe["streams"] if stream["codec_type"] == "audio"),
                None,
            )
            
            metadata["has_audio"] = audio_info is not None
            
            if audio_info:
                metadata["audio_codec"] = audio_info.get("codec_name", "unknown")
            
            return metadata
        
        except ffmpeg.Error as e:
            logger.error(f"Failed to probe video file: {str(e)}")
            raise VideoProcessingError(f"Failed to probe video file: {str(e)}")
        
        except Exception as e:
            logger.error(f"Failed to validate video file: {str(e)}")
            raise VideoProcessingError(f"Failed to validate video file: {str(e)}")
    
    def get_ffmpeg_options(
        self, optimize_level: OptimizationLevel, preserve_audio: bool
    ) -> Dict[str, Dict]:
        """
        Get FFmpeg options based on optimization level and audio preference.
        
        Args:
            optimize_level: Optimization level (fast, balanced, max)
            preserve_audio: Whether to preserve audio in the output
            
        Returns:
            Dictionary containing FFmpeg options for each format
        """
        # Base options for all formats
        options = {
            "mp4": {
                "video_codec": "libx264",
                "audio_codec": "aac" if preserve_audio else None,
                "options": {},
            },
            "webm": {
                "video_codec": "libvpx-vp9",
                "audio_codec": "libopus" if preserve_audio else None,
                "options": {},
            },
            "mov": {
                "video_codec": "libx264",
                "audio_codec": "aac" if preserve_audio else None,
                "options": {},
            },
        }
        
        # Optimization level specific options
        if optimize_level == OptimizationLevel.FAST:
            # Fast encoding, lower quality
            options["mp4"]["options"] = {
                "preset": "veryfast",
                "crf": "28",
                "tune": "fastdecode",
            }
            options["webm"]["options"] = {
                "deadline": "realtime",
                "cpu-used": "8",
                "crf": "35",
            }
            options["mov"]["options"] = {
                "preset": "veryfast",
                "crf": "28",
                "tune": "fastdecode",
            }
        
        elif optimize_level == OptimizationLevel.BALANCED:
            # Balanced encoding, good quality
            options["mp4"]["options"] = {
                "preset": "medium",
                "crf": "23",
                "tune": "film",
            }
            options["webm"]["options"] = {
                "deadline": "good",
                "cpu-used": "4",
                "crf": "30",
            }
            options["mov"]["options"] = {
                "preset": "medium",
                "crf": "23",
                "tune": "film",
            }
        
        elif optimize_level == OptimizationLevel.MAX:
            # Maximum quality, slower encoding
            options["mp4"]["options"] = {
                "preset": "slow",
                "crf": "18",
                "tune": "film",
            }
            options["webm"]["options"] = {
                "deadline": "best",
                "cpu-used": "0",
                "crf": "24",
            }
            options["mov"]["options"] = {
                "preset": "slow",
                "crf": "18",
                "tune": "film",
            }
        
        return options
    
    def convert_video(
        self,
        input_file: str,
        output_format: str,
        options: Dict,
        preserve_audio: bool,
    ) -> str:
        """
        Convert a video file to a specific format using FFmpeg.
        
        Args:
            input_file: Path to the input video file
            output_format: Output format (mp4, webm, mov)
            options: FFmpeg options for the conversion
            preserve_audio: Whether to preserve audio in the output
            
        Returns:
            Path to the converted video file
        """
        try:
            # Create output file path
            input_path = Path(input_file)
            output_file = str(
                input_path.parent / f"{input_path.stem}.{output_format}"
            )
            
            # Start with input file
            stream = ffmpeg.input(input_file)
            
            # Configure video stream
            video_stream = ffmpeg.filter(
                stream.video, 
                'fps', 
                fps=30
            )
            
            # Apply video codec and options
            video_codec = options["video_codec"]
            video_options = options["options"]
            
            # Prepare video stream with codec and options
            video_args = {
                **video_options,
            }
            
            video_output = ffmpeg.output(
                video_stream, 
                output_file,
                **video_args,
                vcodec=video_codec
            )
            
            # Configure audio if needed
            if preserve_audio and options["audio_codec"]:
                audio_codec = options["audio_codec"]
                
                # Add audio to output
                video_output = ffmpeg.output(
                    video_stream,
                    stream.audio,
                    output_file,
                    **video_args,
                    vcodec=video_codec,
                    acodec=audio_codec
                )
            
            # Run FFmpeg conversion
            logger.info(f"Converting {input_file} to {output_format}")
            ffmpeg.run(
                video_output,
                overwrite_output=True,
                quiet=True
            )
            
            logger.info(f"Conversion to {output_format} completed: {output_file}")
            
            return output_file
            
        except ffmpeg.Error as e:
            error_message = f"FFmpeg error during conversion to {output_format}: {str(e)}"
            logger.error(error_message)
            
            if os.path.exists(output_file):
                os.remove(output_file)
                
            raise VideoProcessingError(error_message)
            
        except Exception as e:
            error_message = f"Error during conversion to {output_format}: {str(e)}"
            logger.error(error_message)
            
            if os.path.exists(output_file):
                os.remove(output_file)
                
            raise VideoProcessingError(error_message)
    
    async def process_job(self, job: ConversionJob) -> None:
        """
        Process a video conversion job asynchronously.
        
        Args:
            job: ConversionJob object
        """
        try:
            # Update job status to processing
            job.update_status(JobStatus.PROCESSING)
            logger.info(f"Processing job {job.id}")
            
            # Get original file size
            original_size = self.get_file_size_mb(job.temp_file_path)
            job.original_size_mb = original_size
            
            # Get FFmpeg options based on optimization level
            ffmpeg_options = self.get_ffmpeg_options(
                job.optimize_level, job.preserve_audio
            )
            
            # Process each requested format
            conversion_results = {}
            futures = {}
            
            with self.executor as executor:
                # Submit conversion tasks for each format
                for fmt in job.formats:
                    logger.info(f"Submitting format {fmt} for job {job.id}")
                    
                    format_options = ffmpeg_options.get(fmt, {})
                    if not format_options:
                        logger.warning(f"No options found for format {fmt}, skipping")
                        continue
                    
                    future = executor.submit(
                        self.convert_video,
                        job.temp_file_path,
                        fmt,
                        format_options,
                        job.preserve_audio,
                    )
                    futures[future] = fmt
                
                # Wait for all conversions to complete
                for future in as_completed(futures):
                    fmt = futures[future]
                    try:
                        converted_file_path = future.result()
                        conversion_results[fmt] = converted_file_path
                        logger.info(f"Conversion for format {fmt} completed")
                    except Exception as e:
                        logger.error(f"Conversion for format {fmt} failed: {str(e)}")
            
            # Upload converted files to R2
            for fmt, file_path in conversion_results.items():
                try:
                    # Generate object key for R2
                    object_key = f"{fmt}/{Path(file_path).stem}.{fmt}"
                    
                    # Upload to R2
                    public_url, size_mb = r2_uploader.upload_file(file_path, object_key)
                    
                    # Add converted file to job
                    job.add_converted_file(fmt, public_url, size_mb)
                    
                    # Remove temporary converted file
                    os.remove(file_path)
                    
                except Exception as e:
                    logger.error(f"Failed to upload {fmt} file: {str(e)}")
            
            # Remove original temporary file
            if os.path.exists(job.temp_file_path):
                os.remove(job.temp_file_path)
            
            # Update job status to completed
            job.update_status(JobStatus.COMPLETED)
            logger.info(f"Job {job.id} completed")
            
        except Exception as e:
            error_message = f"Error processing job {job.id}: {str(e)}"
            logger.error(error_message)
            
            # Update job status to failed
            job.update_status(JobStatus.FAILED, error=error_message)
            
            # Clean up temporary files
            try:
                # Remove original file
                if os.path.exists(job.temp_file_path):
                    os.remove(job.temp_file_path)
                
                # Remove any partially converted files
                for fmt in job.formats:
                    output_file = str(
                        Path(job.temp_file_path).parent
                        / f"{Path(job.temp_file_path).stem}.{fmt}"
                    )
                    if os.path.exists(output_file):
                        os.remove(output_file)
            except Exception as cleanup_error:
                logger.error(f"Error during cleanup: {str(cleanup_error)}")


# Singleton instance
video_converter = VideoConverter()