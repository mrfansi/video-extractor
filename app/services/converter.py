import os
import shutil
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Optional
from pathlib import Path

import ffmpeg
import psutil
from loguru import logger

from app.core.config import settings
from app.core.errors import VideoProcessingError
from app.models.job import ConversionJob, JobStatus
from app.schemas.video import OptimizationLevel
from app.services.r2_uploader import r2_uploader


class VideoConverter:
    """
    Video converter service with adaptive thread allocation and optimized FFmpeg parameters.
    Based on performance profiling results, this implementation optimizes for:
    1. Thread count: 4 threads optimal for most conversions
    2. Format-specific settings: MP4 is 5x faster than WebM
    3. Adaptive parameters based on video characteristics
    """

    def __init__(self, max_workers: int = settings.MAX_WORKERS):
        """
        Initialize the video converter service with adaptive thread allocation.
        
        Args:
            max_workers: Maximum number of worker threads (default from settings)
        """
        # Create thread pool with adaptive sizing based on system resources
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.temp_dir = Path(settings.TEMP_DIR)
        
        # Ensure temp directory exists
        if not self.temp_dir.exists():
            self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Thread lock for thread-safe operations
        self.lock = threading.Lock()
        
        # Log initialization
        cpu_count = psutil.cpu_count(logical=True)
        logger.info(f"Initializing VideoConverter with max_workers={max_workers}, available CPUs={cpu_count}")

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
        self, optimize_level: OptimizationLevel, preserve_audio: bool, video_info: Dict = None
    ) -> Dict[str, Dict]:
        """
        Get optimized FFmpeg options based on optimization level, audio preference, and video characteristics.
        
        Args:
            optimize_level: Optimization level (fast, balanced, max)
            preserve_audio: Whether to preserve audio in the output
            video_info: Optional dictionary with video metadata for adaptive optimization
        
        Returns:
            Dictionary containing FFmpeg options for each format
        """
        # Base options for all formats
        options = {
            "mp4": {
                "video_codec": "libx264",
                "audio_codec": "aac" if preserve_audio else None,
                "options": {},
                "threads": self._get_optimal_thread_count("mp4"),
            },
            "webm": {
                "video_codec": "libvpx-vp9",
                "audio_codec": "libopus" if preserve_audio else None,
                "options": {},
                "threads": self._get_optimal_thread_count("webm"),
            },
            "mov": {
                "video_codec": "libx264",
                "audio_codec": "aac" if preserve_audio else None,
                "options": {},
                "threads": self._get_optimal_thread_count("mov"),
            },
        }
        
        # Optimization level specific options
        if optimize_level == OptimizationLevel.FAST:
            # Fast encoding, lower quality
            options["mp4"]["options"] = {
                "preset": "veryfast",
                "crf": "28",
                "tune": "fastdecode",
                "movflags": "+faststart",  # Optimize for web streaming
            }
            options["webm"]["options"] = {
                "deadline": "realtime",
                "cpu-used": "8",
                "crf": "35",
                "row-mt": "1",  # Enable row-based multithreading
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
                "movflags": "+faststart",  # Optimize for web streaming
            }
            options["webm"]["options"] = {
                "deadline": "good",
                "cpu-used": "4",
                "crf": "30",
                "row-mt": "1",  # Enable row-based multithreading
            }
            options["mov"]["options"] = {
                "preset": "medium",
                "crf": "23",
                "tune": "film",
            }
        
        elif optimize_level == OptimizationLevel.MAX:
            # Maximum quality, slower encoding
            options["mp4"]["options"] = {
                "preset": "slow",  # Using 'slow' instead of 'slower' for better speed/quality balance
                "crf": "18",
                "tune": "film",
                "movflags": "+faststart",  # Optimize for web streaming
            }
            options["webm"]["options"] = {
                "deadline": "good",  # Using 'good' instead of 'best' for better performance
                "cpu-used": "2",    # Less aggressive CPU optimization for better quality
                "crf": "24",
                "row-mt": "1",      # Enable row-based multithreading
            }
            options["mov"]["options"] = {
                "preset": "slow",
                "crf": "18",
                "tune": "film",
            }
        
        # Apply adaptive optimizations if video info is provided
        if video_info:
            self._apply_adaptive_optimizations(options, video_info)
        
        return options

    def _get_optimal_thread_count(self, format: str) -> int:
        """
        Calculate optimal thread count based on format and system resources.
        
        Args:
            format: The output format (mp4, webm, mov)
            
        Returns:
            Optimal thread count for the conversion
        """
        import psutil
        
        # Get available CPU cores
        cpu_count = psutil.cpu_count(logical=True)
        
        # Based on our performance profiling, 4 threads is optimal for most conversions
        # with diminishing returns beyond that
        base_thread_count = min(4, cpu_count)
        
        # Format-specific adjustments based on profiling results
        if format == "webm":
            # WebM encoding is more CPU-intensive, can benefit from more threads
            # but still with diminishing returns
            return min(base_thread_count + 1, cpu_count)
        elif format == "mp4" or format == "mov":
            # MP4/MOV encoding is efficient with 4 threads
            return base_thread_count
        else:
            # Default fallback
            return base_thread_count

    def _apply_adaptive_optimizations(self, options: Dict, video_info: Dict) -> None:
        """
        Apply adaptive optimizations based on video characteristics.
        
        Args:
            options: FFmpeg options dictionary to modify
            video_info: Dictionary with video metadata
        """
        # Extract relevant video information
        width = video_info.get("width", 0)
        height = video_info.get("height", 0)
        bitrate = video_info.get("bitrate", 0)
        duration = video_info.get("duration", 0)
        
        # Adjust parameters based on resolution
        if width and height:
            resolution = width * height
            
            # High resolution videos (4K or higher)
            if resolution >= 3840 * 2160:
                # For high-res videos, adjust parameters for better efficiency
                for fmt in options:
                    if fmt == "mp4" or fmt == "mov":
                        # Use faster preset for high-res videos to improve performance
                        if options[fmt]["options"].get("preset") == "slow":
                            options[fmt]["options"]["preset"] = "medium"
                        
                        # Increase thread count for high-res videos
                        options[fmt]["threads"] = min(options[fmt]["threads"] + 2, 8)
                    
                    elif fmt == "webm":
                        # Adjust CPU usage for high-res WebM videos
                        cpu_used = int(options[fmt]["options"].get("cpu-used", "4"))
                        options[fmt]["options"]["cpu-used"] = str(min(cpu_used + 2, 8))
            
            # Low resolution videos
            elif resolution <= 640 * 480:
                # For low-res videos, we can use higher quality settings
                for fmt in options:
                    if fmt == "mp4" or fmt == "mov":
                        # Can use slower preset for better quality on low-res
                        if options[fmt]["options"].get("preset") == "medium":
                            options[fmt]["options"]["preset"] = "slow"
                    
                    elif fmt == "webm":
                        # Lower CPU usage for better quality on low-res
                        cpu_used = int(options[fmt]["options"].get("cpu-used", "4"))
                        options[fmt]["options"]["cpu-used"] = str(max(cpu_used - 2, 0))
        
        # Adjust parameters based on video duration
        if duration:
            # For very short videos, we can use higher quality settings
            if duration < 30:  # Less than 30 seconds
                for fmt in options:
                    if fmt == "mp4" or fmt == "mov":
                        # Reduce CRF for better quality on short videos
                        crf = int(options[fmt]["options"].get("crf", "23"))
                        options[fmt]["options"]["crf"] = str(max(crf - 2, 18))
                    
                    elif fmt == "webm":
                        # Reduce CRF for better quality on short videos
                        crf = int(options[fmt]["options"].get("crf", "30"))
                        options[fmt]["options"]["crf"] = str(max(crf - 2, 24))
        
        # Log the adaptive optimizations
        logger.info(f"Applied adaptive optimizations based on video characteristics: {width}x{height}, {duration}s")

    def _get_video_info(self, video_path: str) -> Dict:
        """
        Get video information using FFprobe.
        
        Args:
            video_path: Path to the video file
            
        Returns:
            Dictionary with video information
        """
        try:
            probe = ffmpeg.probe(video_path)
            video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
            
            if video_stream:
                return {
                    "width": int(video_stream.get('width', 0)),
                    "height": int(video_stream.get('height', 0)),
                    "duration": float(probe.get('format', {}).get('duration', 0)),
                    "bitrate": int(video_stream.get('bit_rate', 0)),
                    "codec": video_stream.get('codec_name', ''),
                    "format": probe.get('format', {}).get('format_name', ''),
                }
            return {}
        except Exception as e:
            logger.warning(f"Failed to get video info: {str(e)}")
            return {}

    def convert_video(
        self,
        input_file: str,
        output_format: str,
        options: Dict,
        preserve_audio: bool,
    ) -> str:
        """
        Convert a video file to a specific format using FFmpeg with optimized parameters.
        
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
            
            # Get video information for adaptive optimizations
            video_info = self._get_video_info(input_file)
            
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
            thread_count = options.get("threads", 4)  # Get thread count from options
            
            # Prepare video stream with codec and options
            video_args = {
                **video_options,
                "threads": str(thread_count),  # Apply thread count
            }
            
            # Log conversion parameters
            logger.info(f"Converting with {thread_count} threads, codec: {video_codec}, options: {video_options}")
            
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
            
            # Run FFmpeg conversion with progress monitoring
            start_time = time.time()
            logger.info(f"Starting conversion of {input_file} to {output_format}")
            
            ffmpeg.run(
                video_output,
                overwrite_output=True,
                quiet=True
            )
            
            # Calculate conversion time
            conversion_time = time.time() - start_time
            
            # Get file sizes for logging
            original_size = self.get_file_size_mb(input_file)
            converted_size = self.get_file_size_mb(output_file)
            compression_ratio = (converted_size / original_size) * 100 if original_size > 0 else 0
            
            logger.info(
                f"Conversion to {output_format} completed in {conversion_time:.2f}s: "
                f"{output_file}, Size: {converted_size:.2f}MB, "
                f"Compression: {compression_ratio:.2f}%"
            )
            
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
        Process a video conversion job asynchronously with adaptive thread allocation
        and optimized FFmpeg parameters.
        
        Args:
            job: ConversionJob object
        """
        start_time = time.time()
        
        try:
            # Initialize job processing
            self._initialize_job(job)
            
            # Get video metadata and prepare conversion parameters
            original_size, video_info, ffmpeg_options = self._prepare_conversion_parameters(job)
            
            # Execute conversions with optimized thread allocation
            conversion_results, failed_formats = self._execute_conversions(
                job, original_size, ffmpeg_options
            )
            
            # Upload results to storage
            upload_results = self._upload_converted_files(job, conversion_results)
            
            # Clean up temporary files
            self._cleanup_temp_files(job, conversion_results)
            
            # Finalize job status
            self._finalize_job_status(job, failed_formats, upload_results)
            
            # Log completion metrics
            total_time = time.time() - start_time
            logger.info(
                f"Job {job.id} completed in {total_time:.2f}s. "
                f"Original size: {original_size:.2f}MB, "
                f"Formats: {list(conversion_results.keys())}"
            )
            
        except Exception as e:
            self._handle_job_failure(job, e, start_time)
    
    def _initialize_job(self, job: ConversionJob) -> None:
        """Initialize job processing and update status."""
        job.update_status(JobStatus.PROCESSING)
        logger.info(f"Processing job {job.id}")
    
    def _prepare_conversion_parameters(self, job: ConversionJob) -> Tuple[float, Optional[Dict], Dict]:
        """
        Prepare parameters for video conversion including file size, video info, and FFmpeg options.
        
        Returns:
            Tuple of (original_size, video_info, ffmpeg_options)
        """
        # Get original file size
        original_size = self.get_file_size_mb(job.temp_file_path)
        job.original_size_mb = original_size
        
        # Get video information for adaptive optimizations
        video_info = None
        try:
            video_info = self._get_video_info(job.temp_file_path)
            logger.info(f"Video info for job {job.id}: {video_info}")
        except Exception as e:
            logger.warning(f"Failed to get video info for job {job.id}: {str(e)}")
        
        # Get FFmpeg options based on optimization level and video characteristics
        ffmpeg_options = self.get_ffmpeg_options(
            job.optimize_level, job.preserve_audio, video_info
        )
        
        return original_size, video_info, ffmpeg_options
    
    def _execute_conversions(
        self, job: ConversionJob, original_size: float, ffmpeg_options: Dict
    ) -> Tuple[Dict[str, str], List[str]]:
        """
        Execute video conversions for all requested formats with optimized thread allocation.
        
        Returns:
            Tuple of (conversion_results, failed_formats)
        """
        # Calculate optimal worker count based on system resources and job requirements
        optimal_workers = self._calculate_optimal_workers(job.formats)
        logger.info(f"Using {optimal_workers} worker threads for job {job.id}")
        
        # Process each requested format
        conversion_results = {}
        futures = {}
        failed_formats = []
        
        # Create a thread pool with optimal worker count
        with ThreadPoolExecutor(max_workers=optimal_workers) as executor:
            # Submit conversion tasks for each format
            for fmt in job.formats:
                self._submit_conversion_task(
                    executor, job, fmt, ffmpeg_options, futures
                )
            
            # Wait for all conversions to complete with timeout handling
            for future in as_completed(futures):
                fmt = futures[future]
                result = self._process_conversion_result(
                    future, fmt, original_size, conversion_results, failed_formats
                )
        
        # Check if all conversions failed
        if not conversion_results and job.formats:
            raise VideoProcessingError(f"All conversions failed for job {job.id}")
        
        return conversion_results, failed_formats
    
    def _submit_conversion_task(
        self, executor: ThreadPoolExecutor, job: ConversionJob, fmt: str, 
        ffmpeg_options: Dict, futures: Dict
    ) -> None:
        """Submit a single format conversion task to the thread pool."""
        logger.info(f"Submitting format {fmt} for job {job.id}")
        
        format_options = ffmpeg_options.get(fmt, {})
        if not format_options:
            logger.warning(f"No options found for format {fmt}, skipping")
            return
        
        future = executor.submit(
            self.convert_video,
            job.temp_file_path,
            fmt,
            format_options,
            job.preserve_audio,
        )
        futures[future] = fmt
    
    def _process_conversion_result(
        self, future, fmt: str, original_size: float, 
        conversion_results: Dict[str, str], failed_formats: List[str]
    ) -> None:
        """Process the result of a conversion task."""
        try:
            # Set a reasonable timeout based on video size and format
            timeout = self._calculate_timeout(original_size, fmt)
            converted_file_path = future.result(timeout=timeout)
            conversion_results[fmt] = converted_file_path
            logger.info(f"Conversion for format {fmt} completed successfully")
        except TimeoutError:
            logger.error(f"Conversion for format {fmt} timed out after {timeout} seconds")
            failed_formats.append(fmt)
        except Exception as e:
            logger.error(f"Conversion for format {fmt} failed: {str(e)}")
            failed_formats.append(fmt)
    
    def _upload_converted_files(
        self, job: ConversionJob, conversion_results: Dict[str, str]
    ) -> Dict[str, bool]:
        """
        Upload converted files to R2 storage.
        
        Returns:
            Dictionary mapping formats to upload success status
        """
        upload_results = {}
        for fmt, file_path in conversion_results.items():
            try:
                # Generate object key for R2
                object_key = f"{fmt}/{Path(file_path).stem}.{fmt}"
                
                # Upload to R2 with retry logic
                public_url, size_mb = self._upload_with_retry(file_path, object_key)
                
                # Add converted file to job
                job.add_converted_file(fmt, public_url, size_mb)
                upload_results[fmt] = True
                
            except Exception as e:
                upload_results[fmt] = False
                logger.error(f"Failed to upload {fmt} file: {str(e)}")
        
        return upload_results
    
    def _cleanup_temp_files(self, job: ConversionJob, conversion_results: Dict[str, str]) -> None:
        """Clean up temporary files after processing."""
        # Remove temporary converted files
        for file_path in conversion_results.values():
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception as e:
                logger.warning(f"Failed to remove temporary file {file_path}: {str(e)}")
        
        # Remove original temporary file
        try:
            if os.path.exists(job.temp_file_path):
                os.remove(job.temp_file_path)
        except Exception as e:
            logger.warning(f"Failed to remove original file {job.temp_file_path}: {str(e)}")
    
    def _finalize_job_status(
        self, job: ConversionJob, failed_formats: List[str], upload_results: Dict[str, bool]
    ) -> None:
        """Determine and update final job status based on results."""
        if failed_formats or not all(upload_results.values()):
            # Some formats failed but others succeeded
            job.update_status(
                JobStatus.PARTIALLY_COMPLETED,
                error=f"Failed formats: {', '.join(failed_formats)}"
            )
            logger.warning(
                f"Job {job.id} partially completed. "
                f"Failed formats: {failed_formats}"
            )
        else:
            # All formats completed successfully
            job.update_status(JobStatus.COMPLETED)
            logger.info(f"Job {job.id} completed successfully")
    
    def _handle_job_failure(self, job: ConversionJob, exception: Exception, start_time: float) -> None:
        """Handle job failure with proper error logging and cleanup."""
        total_time = time.time() - start_time
        error_message = f"Error processing job {job.id}: {str(exception)}"
        logger.error(f"{error_message} (after {total_time:.2f}s)")
        
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
    
    def _calculate_optimal_workers(self, formats: List[str]) -> int:
        """
        Calculate optimal number of worker threads based on formats and system resources.
        
        Args:
            formats: List of output formats
            
        Returns:
            Optimal number of worker threads
        """
        # Get available CPU cores
        cpu_count = psutil.cpu_count(logical=True)
        
        # Get system memory information
        memory = psutil.virtual_memory()
        available_memory_gb = memory.available / (1024 * 1024 * 1024)
        
        # Base worker count on available CPUs, but cap at 4 based on profiling results
        # which showed diminishing returns beyond 4 threads
        base_workers = min(4, cpu_count)
        
        # Adjust based on number of formats
        format_count = len(formats)
        
        # Check for memory constraints (each worker might need ~500MB)
        memory_based_workers = max(1, int(available_memory_gb / 0.5))
        
        # If we have multiple formats, we need at least that many workers
        # but still respect our upper limits based on profiling results and memory
        if format_count > 1:
            # Consider both CPU and memory constraints
            return min(max(format_count, base_workers), memory_based_workers, 8)
        
        # For single format, respect memory constraints
        return min(base_workers, memory_based_workers)
    
    def _calculate_timeout(self, file_size_mb: float, format: str) -> int:
        """
        Calculate appropriate timeout for conversion based on file size and format.
        
        Args:
            file_size_mb: Size of the input file in MB
            format: Output format
            
        Returns:
            Timeout in seconds
        """
        # Base timeout on file size
        base_timeout = max(60, int(file_size_mb * 2))  # At least 60 seconds
        
        # Format-specific adjustments based on profiling results
        if format == "webm":
            # WebM encoding is significantly slower (5x based on profiling)
            return base_timeout * 5
        
        return base_timeout
    
    def _upload_with_retry(self, file_path: str, object_key: str, max_retries: int = 3) -> Tuple[str, float]:
        """
        Upload a file to R2 with retry logic.
        
        Args:
            file_path: Path to the file to upload
            object_key: Object key for R2
            max_retries: Maximum number of retry attempts
            
        Returns:
            Tuple of (public_url, size_mb)
        """
        retries = 0
        last_error = None
        
        while retries < max_retries:
            try:
                # Upload to R2
                public_url, size_mb = r2_uploader.upload_file(file_path, object_key)
                return public_url, size_mb
            except Exception as e:
                retries += 1
                last_error = e
                logger.warning(f"Upload attempt {retries} failed: {str(e)}. Retrying...")
                time.sleep(1)  # Wait before retrying
        
        # If we get here, all retries failed
        raise Exception(f"Failed to upload after {max_retries} attempts: {str(last_error)}")


# Singleton instance
converter = VideoConverter()