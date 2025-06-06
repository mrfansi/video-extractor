import os
import psutil
import random
import shutil
import tempfile
import threading
import time
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Optional, Union
from pathlib import Path

import ffmpeg
from loguru import logger

from app.core.config import settings
from app.core.errors import StorageError, VideoProcessingError, CircuitBreakerError
from app.services.r2_uploader import r2_uploader
from app.core.circuit_breaker import CircuitBreaker, circuit_breaker
from app.models.job import ConversionJob, JobStatus
from app.schemas.video import OptimizationLevel


class VideoConverter:
    """
    Video converter service with adaptive thread allocation and optimized FFmpeg parameters.
    Based on performance profiling results, this implementation optimizes for:
    1. Thread count: 4 threads optimal for most conversions
    2. Format-specific settings: MP4 is 5x faster than WebM
    3. Adaptive parameters based on video characteristics
    """
    
    @staticmethod
    def reset_circuit_breakers(service_name: str = None) -> Dict[str, bool]:
        """
        Reset circuit breakers for better resilience.
        
        Args:
            service_name: Optional name of the specific circuit breaker to reset.
                         If None, all circuit breakers will be reset.
                         
        Returns:
            Dictionary mapping circuit breaker names to reset status (True = reset successful)
        """
        results = {}
        
        if service_name:
            # Reset a specific circuit breaker
            breaker = CircuitBreaker.get_instance(service_name)
            breaker.reset()
            results[service_name] = True
            logger.info(f"Circuit breaker for {service_name} manually reset")
        else:
            # Reset all circuit breakers
            for name, instance in CircuitBreaker._instances.items():
                instance.reset()
                results[name] = True
                logger.info(f"Circuit breaker for {name} manually reset")
                
        return results

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

    async def save_upload_file(self, file) -> Tuple[str, str]:
        """
        Save an uploaded file to a temporary location asynchronously.
        
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
            
            # Save uploaded file to temporary location using aiofiles for async I/O
            import aiofiles
            
            # Read the file content
            content = await file.read()
            
            # Write to the temporary file asynchronously
            async with aiofiles.open(temp_file_path, "wb") as buffer:
                await buffer.write(content)
            
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
        # Determine content type from video info if available
        content_type = self._detect_content_type(video_info) if video_info else "general"
        logger.info(f"Detected content type: {content_type}")
        
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
                "pix_fmt": "yuv420p",  # Ensure compatibility across devices
            }
            options["webm"]["options"] = {
                "deadline": "realtime",
                "cpu-used": "8",
                "crf": "35",
                "row-mt": "1",  # Enable row-based multithreading
                "auto-alt-ref": "1",  # Enable automatic alternate reference frames
                "lag-in-frames": "0",  # No look-ahead for faster encoding
            }
            options["mov"]["options"] = {
                "preset": "veryfast",
                "crf": "28",
                "tune": "fastdecode",
                "pix_fmt": "yuv420p",  # Ensure compatibility across devices
            }
        
        elif optimize_level == OptimizationLevel.BALANCED:
            # Balanced encoding, good quality
            options["mp4"]["options"] = {
                "preset": "medium",
                "crf": "23",
                "tune": "film",
                "movflags": "+faststart",  # Optimize for web streaming
                "pix_fmt": "yuv420p",  # Ensure compatibility across devices
                "profile:v": "high",  # Use high profile for better quality
                "level": "4.1",  # Compatible with most devices
            }
            options["webm"]["options"] = {
                "deadline": "good",
                "cpu-used": "4",
                "crf": "30",
                "row-mt": "1",  # Enable row-based multithreading
                "auto-alt-ref": "1",  # Enable automatic alternate reference frames
                "lag-in-frames": "25",  # Look-ahead for better quality
                "arnr-maxframes": "7",  # Temporal filter strength
                "arnr-strength": "4",  # Temporal filter strength
            }
            options["mov"]["options"] = {
                "preset": "medium",
                "crf": "23",
                "tune": "film",
                "pix_fmt": "yuv420p",  # Ensure compatibility across devices
                "profile:v": "high",  # Use high profile for better quality
                "level": "4.1",  # Compatible with most devices
            }
        
        elif optimize_level == OptimizationLevel.MAX:
            # Maximum quality, slower encoding
            options["mp4"]["options"] = {
                "preset": "slow",  # Using 'slow' instead of 'slower' for better speed/quality balance
                "crf": "18",
                "tune": "film",
                "movflags": "+faststart",  # Optimize for web streaming
                "pix_fmt": "yuv420p",  # Ensure compatibility across devices
                "profile:v": "high",  # Use high profile for better quality
                "level": "4.2",  # Higher level for better quality
                "x264-params": "ref=6:me=umh:subme=8:trellis=2:deblock=-1,-1",  # Advanced x264 params
            }
            options["webm"]["options"] = {
                "deadline": "good",  # Using 'good' instead of 'best' for better performance
                "cpu-used": "2",    # Less aggressive CPU optimization for better quality
                "crf": "24",
                "row-mt": "1",      # Enable row-based multithreading
                "auto-alt-ref": "1",  # Enable automatic alternate reference frames
                "lag-in-frames": "25",  # Look-ahead for better quality
                "arnr-maxframes": "15",  # Temporal filter strength
                "arnr-strength": "6",  # Temporal filter strength
                "aq-mode": "2",  # Adaptive quantization mode
            }
            options["mov"]["options"] = {
                "preset": "slow",
                "crf": "18",
                "tune": "film",
                "pix_fmt": "yuv420p",  # Ensure compatibility across devices
                "profile:v": "high",  # Use high profile for better quality
                "level": "4.2",  # Higher level for better quality
                "x264-params": "ref=6:me=umh:subme=8:trellis=2:deblock=-1,-1",  # Advanced x264 params
            }
        
        # Apply content-type specific optimizations
        self._apply_content_specific_options(options, content_type)
        
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
        fps = video_info.get("fps", 0)
        codec = video_info.get("codec_name", "")

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
                        
                        # Add scaling for 4K videos to improve performance
                        if resolution >= 4096 * 2160 and "scale" not in options[fmt]["options"]:
                            options[fmt]["options"]["scale"] = "3840:2160"
                    
                    elif fmt == "webm":
                        # Adjust CPU usage for high-res WebM videos
                        cpu_used = int(options[fmt]["options"].get("cpu-used", "4"))
                        options[fmt]["options"]["cpu-used"] = str(min(cpu_used + 2, 8))
                        
                        # Add scaling for 4K videos to improve performance
                        if resolution >= 4096 * 2160 and "scale" not in options[fmt]["options"]:
                            options[fmt]["options"]["scale"] = "3840:2160"
                        
                        # Add tiling for high-res WebM videos
                        options[fmt]["options"]["tile-columns"] = "4"
                        options[fmt]["options"]["tile-rows"] = "2"
            
            # Low resolution videos
            elif resolution <= 640 * 480:
                # For low-res videos, we can use higher quality settings
                for fmt in options:
                    if fmt == "mp4" or fmt == "mov":
                        # Can use slower preset for better quality on low-res
                        if options[fmt]["options"].get("preset") == "medium":
                            options[fmt]["options"]["preset"] = "slow"
                        
                        # Reduce CRF for better quality on low-res
                        crf = int(options[fmt]["options"].get("crf", "23"))
                        options[fmt]["options"]["crf"] = str(max(crf - 2, 18))
                    
                    elif fmt == "webm":
                        # Lower CPU usage for better quality on low-res
                        cpu_used = int(options[fmt]["options"].get("cpu-used", "4"))
                        options[fmt]["options"]["cpu-used"] = str(max(cpu_used - 2, 0))
                        
                        # Reduce CRF for better quality on low-res
                        crf = int(options[fmt]["options"].get("crf", "30"))
                        options[fmt]["options"]["crf"] = str(max(crf - 3, 22))
        
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
        
            # For long videos, optimize for file size
            elif duration > 600:  # More than 10 minutes
                for fmt in options:
                    if fmt == "mp4" or fmt == "mov":
                        # Increase CRF slightly for long videos to reduce file size
                        crf = int(options[fmt]["options"].get("crf", "23"))
                        options[fmt]["options"]["crf"] = str(min(crf + 2, 28))
                    
                    elif fmt == "webm":
                        # Increase CRF slightly for long videos to reduce file size
                        crf = int(options[fmt]["options"].get("crf", "30"))
                        options[fmt]["options"]["crf"] = str(min(crf + 2, 34))
        
        # Adjust parameters based on bitrate
        if bitrate:
            # For high bitrate videos, we can be more aggressive with compression
            if bitrate > 10000000:  # More than 10 Mbps
                for fmt in options:
                    if fmt == "mp4" or fmt == "mov":
                        # Add maxrate and bufsize for high bitrate videos
                        if "maxrate" not in options[fmt]["options"]:
                            options[fmt]["options"]["maxrate"] = "8M"
                            options[fmt]["options"]["bufsize"] = "16M"
                    
                    elif fmt == "webm":
                        # Add maxrate and bufsize for high bitrate videos
                        if "maxrate" not in options[fmt]["options"]:
                            options[fmt]["options"]["maxrate"] = "6M"
                            options[fmt]["options"]["bufsize"] = "12M"
        
        # Adjust parameters based on frame rate
        if fps:
            # For high frame rate videos, adjust settings
            if fps > 30:
                for fmt in options:
                    if fmt == "mp4" or fmt == "mov":
                        # Add frame rate control for high fps videos
                        if "r" not in options[fmt]["options"]:
                            options[fmt]["options"]["r"] = "30"
                    
                    elif fmt == "webm":
                        # Add frame rate control for high fps videos
                        if "r" not in options[fmt]["options"]:
                            options[fmt]["options"]["r"] = "30"
        
        # Log the adaptive optimizations
        logger.info(f"Applied adaptive optimizations based on video characteristics: {width}x{height}, {duration}s, {bitrate/1000:.2f}kbps, {fps}fps")

    def _detect_content_type(self, video_info: Dict) -> str:
        """
        Detect the content type of the video based on its characteristics.
        
        Args:
            video_info: Dictionary with video metadata
        
        Returns:
            Content type: 'animation', 'film', 'screencast', 'gaming', or 'general'
        """
        if not video_info:
            return "general"
        
        # Extract relevant video information
        width = video_info.get("width", 0)
        height = video_info.get("height", 0)
        fps = video_info.get("fps", 0)
        duration = video_info.get("duration", 0)
        bitrate = video_info.get("bitrate", 0)
        codec = video_info.get("codec_name", "")
        
        # Heuristics for content type detection
        # Animation: Often has flat color areas, lower frame rates, and specific resolutions
        if (fps and fps < 24) and (width == 1920 and height == 1080 or width == 1280 and height == 720):
            return "animation"
        
        # Screencast: Often has specific resolutions and lower frame rates
        if (width == 1920 and height == 1080 or width == 1366 and height == 768) and (fps and fps <= 30):
            return "screencast"
        
        # Gaming: Often has high frame rates and specific resolutions
        if (fps and fps >= 60) and (width == 1920 and height == 1080 or width == 2560 and height == 1440):
            return "gaming"
        
        # Film: Often has specific frame rates and higher bitrates
        if (fps and (fps == 24 or fps == 25 or fps == 30)) and (bitrate and bitrate > 5000000):
            return "film"
        
        # Default to general if no specific type is detected
        return "general"

    def _apply_content_specific_options(self, options: Dict, content_type: str) -> None:
        """
        Apply content-specific optimizations to FFmpeg options.
        
        Args:
            options: FFmpeg options dictionary to modify
            content_type: Type of content ('animation', 'film', 'screencast', 'gaming', or 'general')
        """
        if content_type == "animation":
            # Animation: Focus on preserving flat colors and sharp edges
            for fmt in options:
                if fmt == "mp4" or fmt == "mov":
                    options[fmt]["options"]["tune"] = "animation"
                    # Reduce CRF for better quality with animations
                    crf = int(options[fmt]["options"].get("crf", "23"))
                    options[fmt]["options"]["crf"] = str(max(crf - 2, 18))
                
                elif fmt == "webm":
                    # Reduce CRF for better quality with animations
                    crf = int(options[fmt]["options"].get("crf", "30"))
                    options[fmt]["options"]["crf"] = str(max(crf - 4, 20))
                    # Add specific VP9 settings for animation
                    options[fmt]["options"]["min-q"] = "0"
                    options[fmt]["options"]["max-q"] = "50"
        
        elif content_type == "film":
            # Film: Focus on preserving grain and texture
            for fmt in options:
                if fmt == "mp4" or fmt == "mov":
                    options[fmt]["options"]["tune"] = "film"
                    # Add film-specific settings
                    if "x264-params" in options[fmt]["options"]:
                        options[fmt]["options"]["x264-params"] += ":psy-rd=1.0:psy-rdoq=2.0"
                    else:
                        options[fmt]["options"]["x264-params"] = "psy-rd=1.0:psy-rdoq=2.0"
                
                elif fmt == "webm":
                    # Add film-specific VP9 settings
                    options[fmt]["options"]["arnr-maxframes"] = "15"
                    options[fmt]["options"]["arnr-strength"] = "6"
        
        elif content_type == "screencast":
            # Screencast: Focus on text clarity and low motion
            for fmt in options:
                if fmt == "mp4" or fmt == "mov":
                    options[fmt]["options"]["tune"] = "stillimage"
                    # Increase CRF for screencasts (text remains sharp)
                    crf = int(options[fmt]["options"].get("crf", "23"))
                    options[fmt]["options"]["crf"] = str(min(crf + 4, 28))
                
                elif fmt == "webm":
                    # Adjust settings for screencasts
                    options[fmt]["options"]["sharpness"] = "2"
                    options[fmt]["options"]["static-thresh"] = "100"
        
        elif content_type == "gaming":
            # Gaming: Focus on motion and detail
            for fmt in options:
                if fmt == "mp4" or fmt == "mov":
                    options[fmt]["options"]["tune"] = "zerolatency"
                    # Add gaming-specific settings
                    if "x264-params" in options[fmt]["options"]:
                        options[fmt]["options"]["x264-params"] += ":rc-lookahead=10:deblock=1,1"
                    else:
                        options[fmt]["options"]["x264-params"] = "rc-lookahead=10:deblock=1,1"
                
                elif fmt == "webm":
                    # Add gaming-specific VP9 settings
                    options[fmt]["options"]["lag-in-frames"] = "10"
                    options[fmt]["options"]["error-resilient"] = "1"
        
        # For general content, we keep the default settings
        logger.info(f"Applied content-specific optimizations for content type: {content_type}")

    def _optimize_format_specific(self, fmt: str, options: Dict, video_info: Dict):
        """
        Apply format-specific optimizations with content-aware settings.
        
        Args:
            fmt: Format (mp4, webm, mov)
            options: FFmpeg options dictionary to modify
            video_info: Dictionary with video metadata
        """
        # Get basic video properties
        width = video_info.get('width', 0)
        height = video_info.get('height', 0)
        bitrate = video_info.get('bit_rate', 0)
        fps_str = video_info.get('r_frame_rate', '')
        
        # Calculate frame rate
        fps = 0
        if fps_str and '/' in fps_str:
            try:
                num, den = map(int, fps_str.split('/'))
                fps = num / den if den != 0 else 0
            except (ValueError, ZeroDivisionError):
                pass
                
        # Detect content type for specialized optimizations
        content_type = self._detect_content_type(video_info)
        logger.debug(f"Optimizing {fmt} format for content type: {content_type}")
        
        # MP4 optimizations
        if fmt.lower() == 'mp4':
            # Always use faststart for web streaming
            options['movflags'] = '+faststart'
            
            # Use appropriate pixel format
            options['pix_fmt'] = 'yuv420p'
            
            # Resolution-based optimizations
            if width and height:
                resolution = width * height
                if resolution > 3840 * 2160:  # 8K
                    options['preset'] = 'slower'
                    options['crf'] = '20'
                elif resolution > 1920 * 1080:  # 4K
                    options['preset'] = 'slow'
                    options['crf'] = '22'
                elif resolution > 1280 * 720:  # 1080p
                    options['preset'] = 'medium'
                    options['crf'] = '23'
                else:  # 720p or lower
                    options['preset'] = 'fast'
                    options['crf'] = '25'
            
            # Content-specific optimizations for MP4
            if content_type == 'animation':
                options['tune'] = 'animation'
                # Reduce CRF for better quality with animation
                current_crf = int(options.get('crf', '23'))
                options['crf'] = str(max(18, current_crf - 2))
            elif content_type == 'film':
                options['tune'] = 'film'
                # Slightly reduce CRF for film content
                current_crf = int(options.get('crf', '23'))
                options['crf'] = str(max(20, current_crf - 1))
            elif content_type == 'gaming':
                # For gaming, prioritize speed and handle high motion
                if fps >= 60:
                    options['vsync'] = '1'  # Better frame syncing for high fps
                    options['preset'] = 'faster'  # Faster encoding for high fps
                else:
                    # Use zerolatency tune for better motion handling
                    options['tune'] = 'zerolatency'
            elif content_type == 'screencast':
                # For screencasts, prioritize text clarity
                options['tune'] = 'stillimage'
                current_crf = int(options.get('crf', '23'))
                options['crf'] = str(max(18, current_crf - 3))  # Much better quality for text
        
        # WebM optimizations
        elif fmt.lower() == 'webm':
            # Use VP9 codec for better quality/size ratio
            options['c:v'] = 'libvpx-vp9'
            options['c:a'] = 'libopus'
            options['b:a'] = '96k'  # Lower bitrate for opus is fine
            
            # Resolution-based optimizations
            if width and height:
                resolution = width * height
                if resolution > 3840 * 2160:  # 8K
                    options['deadline'] = 'good'
                    options['cpu-used'] = '1'
                    options['crf'] = '28'
                elif resolution > 1920 * 1080:  # 4K
                    options['deadline'] = 'good'
                    options['cpu-used'] = '2'
                    options['crf'] = '30'
                elif resolution > 1280 * 720:  # 1080p
                    options['deadline'] = 'good'
                    options['cpu-used'] = '3'
                    options['crf'] = '32'
                else:  # 720p or lower
                    options['deadline'] = 'realtime'
                    options['cpu-used'] = '4'
                    options['crf'] = '34'
            
            # Content-specific optimizations for WebM
            if content_type == 'animation':
                # Better quality for animation
                options['deadline'] = 'good'
                current_crf = int(options.get('crf', '32'))
                options['crf'] = str(max(26, current_crf - 4))
                options['cpu-used'] = '2'  # Better quality
            elif content_type == 'film':
                # Better quality for film
                options['deadline'] = 'good'
                current_crf = int(options.get('crf', '32'))
                options['crf'] = str(max(28, current_crf - 2))
            elif content_type == 'gaming':
                # For gaming, prioritize speed
                options['deadline'] = 'realtime'
                options['cpu-used'] = '4'  # Faster encoding
                if fps >= 60:
                    # For high fps content, adjust tile-columns and frame-parallel
                    options['tile-columns'] = '2'
                    options['frame-parallel'] = '1'
            elif content_type == 'screencast':
                # For screencasts, prioritize text clarity
                options['deadline'] = 'good'
                options['cpu-used'] = '0'  # Best quality for text
                current_crf = int(options.get('crf', '32'))
                options['crf'] = str(max(24, current_crf - 6))  # Much better quality for text
                
        # GIF optimizations
        elif fmt.lower() == 'gif':
            # Content-specific optimizations for GIF
            if content_type == 'animation':
                options['fps'] = '12'  # Higher framerate for animation
            elif content_type == 'gaming':
                options['fps'] = '15'  # Higher framerate for gaming
            elif content_type == 'screencast':
                options['fps'] = '8'  # Lower framerate for screencasts
            else:
                options['fps'] = '10'  # Default
                
        # MOV optimizations
        elif fmt.lower() == 'mov':
            # Similar to MP4 but with some MOV-specific adjustments
            options['pix_fmt'] = 'yuv420p'
            
            # Content-specific optimizations similar to MP4
            if content_type == 'animation':
                options['tune'] = 'animation'
            elif content_type == 'film':
                options['tune'] = 'film'
            elif content_type == 'screencast':
                options['tune'] = 'stillimage'
                
        # Log the optimized options
        logger.debug(f"Format-specific optimizations for {fmt} ({content_type}): {options}")

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
        # Initialize output_file variable for exception handling
        output_file = ""
        start_time = time.time()
        
        try:
            # Create output file path with a unique suffix to avoid in-place editing
            input_path = Path(input_file)
            output_file = str(
                input_path.parent / f"{input_path.stem}_converted.{output_format}"
            )
            
            # Get video information for adaptive optimizations
            video_info = self._get_video_info(input_file)
            
            # Start with input file
            stream = ffmpeg.input(input_file)
            
            # Get video stream - no need to apply fps filter by default
            # as it can cause compatibility issues with some videos
            video_stream = stream.video
            
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
            
            # Check if the input file has an audio stream
            has_audio = False
            try:
                # Probe the input file to check for audio streams
                probe = ffmpeg.probe(input_file)
                audio_streams = [stream for stream in probe['streams'] if stream['codec_type'] == 'audio']
                has_audio = len(audio_streams) > 0
                logger.info(f"Input file has audio: {has_audio}")
            except Exception as e:
                logger.warning(f"Failed to probe for audio streams: {str(e)}")
            
            # Configure output with or without audio
            if preserve_audio and has_audio and "audio_codec" in options and options["audio_codec"]:
                audio_codec = options["audio_codec"]
                
                # Add audio to output (combining video and audio streams)
                video_output = ffmpeg.output(
                    video_stream,
                    stream.audio,
                    output_file,
                    **video_args,
                    vcodec=video_codec,
                    acodec=audio_codec
                )
            else:
                # Video only output
                video_output = ffmpeg.output(
                    video_stream, 
                    output_file,
                    **video_args,
                    vcodec=video_codec
                )
            
            # Run FFmpeg conversion with progress monitoring
            logger.info(f"Starting conversion of {input_file} to {output_format}")
            
            try:
                # Run FFmpeg with stderr capture for better debugging
                ffmpeg.run(
                    video_output,
                    overwrite_output=True,
                    quiet=False,  # Show FFmpeg output for debugging
                    capture_stdout=True,
                    capture_stderr=True
                )
            except ffmpeg.Error as e:
                # Log the stderr output from FFmpeg for better debugging
                stderr = e.stderr.decode('utf-8') if e.stderr else "No stderr output"
                logger.error(f"FFmpeg stderr: {stderr}")
                raise
            
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
            # Get detailed error information from FFmpeg
            stderr = e.stderr.decode('utf-8') if hasattr(e, 'stderr') and e.stderr else "No stderr output"
            error_message = f"FFmpeg error during conversion to {output_format}: {str(e)}"
            logger.error(f"{error_message}\nFFmpeg stderr: {stderr}")
            
            if output_file and os.path.exists(output_file):
                os.remove(output_file)
                
            raise VideoProcessingError(f"{error_message}\nFFmpeg stderr: {stderr}")
            
        except Exception as e:
            error_message = f"Error during conversion to {output_format}: {str(e)}"
            logger.error(error_message)
            
            if output_file and os.path.exists(output_file):
                os.remove(output_file)
                
            raise VideoProcessingError(error_message)

    async def process_job(self, job: ConversionJob) -> None:
        """
        Process a video conversion job asynchronously with enhanced adaptive thread allocation
        based on video characteristics and system resources.
        
        Args:
            job: ConversionJob object
        """
        start_time = time.time()
        video_info = None
        
        try:
            # Initialize job processing
            self._initialize_job(job)
            
            # Get video metadata and prepare conversion parameters
            original_size, video_info, ffmpeg_options = self._prepare_conversion_parameters(job)
            
            # Log video characteristics for better observability
            if video_info:
                logger.info(
                    f"Video characteristics for job {job.id}: "
                    f"Resolution: {video_info.get('width', 'unknown')}x{video_info.get('height', 'unknown')}, "
                    f"Duration: {video_info.get('duration', 'unknown')}s, "
                    f"Codec: {video_info.get('codec_name', 'unknown')}, "
                    f"Bitrate: {video_info.get('bit_rate', 'unknown')} bps"
                )
                
                # Calculate video complexity factor for better resource allocation
                complexity_factor = self._calculate_video_complexity_factor(video_info)
                logger.info(f"Video complexity factor for job {job.id}: {complexity_factor:.2f}")
            
            # Execute conversions with enhanced adaptive thread allocation
            conversion_results, failed_formats = self._execute_conversions(
                job, original_size, ffmpeg_options, video_info
            )
            
            # Upload results to storage with circuit breaker protection
            upload_results = self._upload_converted_files(job, conversion_results)
            
            # Clean up temporary files
            self._cleanup_temp_files(job, conversion_results)
            
            # Finalize job status with enhanced error reporting
            self._finalize_job_status(job, failed_formats, upload_results)
            
            # Log completion metrics with detailed performance information
            total_time = time.time() - start_time
            logger.info(
                f"Job {job.id} completed in {total_time:.2f}s. "
                f"Original size: {original_size:.2f}MB, "
                f"Formats: {list(conversion_results.keys())}, "
                f"Performance: {original_size / total_time:.2f}MB/s"
            )
            
        except Exception as e:
            self._handle_job_failure(job, e, start_time, video_info)
    
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
        self, job: ConversionJob, original_size: float, ffmpeg_options: Dict, 
        video_info: Optional[Dict] = None
    ) -> Tuple[Dict[str, str], List[str]]:
        """
        Execute video conversions for all requested formats with enhanced adaptive thread allocation
        based on video characteristics and system resources.
        
        Args:
            job: The conversion job
            original_size: Size of the original file in MB
            ffmpeg_options: FFmpeg conversion options for each format
            video_info: Optional video metadata for better resource allocation
            
        Returns:
            Tuple of (conversion_results, failed_formats)
        """
        # Calculate optimal worker count based on system resources, video characteristics, and job requirements
        optimal_workers = self._calculate_optimal_workers(job.formats, video_info)
        logger.info(f"Using {optimal_workers} worker threads for job {job.id}")
        
        # Process each requested format
        conversion_results = {}
        futures = {}
        failed_formats = []
        
        # Prioritize formats based on complexity (WebM is more complex than MP4)
        prioritized_formats = self._prioritize_formats(job.formats)
        logger.info(f"Format execution order for job {job.id}: {prioritized_formats}")
        
        # Create a thread pool with optimal worker count
        with ThreadPoolExecutor(max_workers=optimal_workers) as executor:
            # Submit conversion tasks for each format in priority order
            for fmt in prioritized_formats:
                self._submit_conversion_task(
                    executor, job, fmt, ffmpeg_options, futures, video_info
                )
            
            # Monitor system resources during conversion with dynamic worker adjustment
            monitor_thread = threading.Thread(
                target=self._monitor_system_resources,
                args=(job.id, optimal_workers, executor)
            )
            monitor_thread.daemon = True  # Daemon thread will exit when main thread exits
            monitor_thread.start()
            
            # Wait for all conversions to complete with adaptive timeout handling
            for future in as_completed(futures):
                fmt = futures[future]
                timeout = self._calculate_timeout(original_size, fmt, video_info)
                result = self._process_conversion_result(
                    future, fmt, original_size, conversion_results, failed_formats, timeout
                )
        
        # Check if all conversions failed
        if not conversion_results and job.formats:
            raise VideoProcessingError(f"All conversions failed for job {job.id}")
        
        return conversion_results, failed_formats
        
    def _prioritize_formats(self, formats: List[str]) -> List[str]:
        """
        Prioritize formats based on complexity to optimize resource utilization.
        More complex formats (WebM) are started first to maximize parallelism.
        
        Args:
            formats: List of formats to prioritize
            
        Returns:
            Prioritized list of formats
        """
        # Define format complexity (higher value = higher priority)
        format_priority = {
            'webm': 3,  # Most complex, start first
            'mov': 2,
            'mp4': 1   # Least complex, start last
        }
        
        # Sort formats by priority (descending)
        return sorted(formats, key=lambda fmt: format_priority.get(fmt, 0), reverse=True)
        
    def _monitor_system_resources(self, job_id: str, initial_workers: int, executor: Optional[ThreadPoolExecutor] = None) -> None:
        """
        Monitor system resources during conversion and dynamically adjust worker count if needed.
        
        Args:
            job_id: ID of the job being processed
            initial_workers: Initial number of worker threads
            executor: Optional ThreadPoolExecutor to adjust worker count dynamically
        """
        check_interval = 3  # Check every 3 seconds for more responsive adjustments
        warning_threshold = 0.80  # 80% utilization triggers adjustment
        critical_threshold = 0.90  # 90% utilization triggers more aggressive adjustment
        adjustment_cooldown = 0  # Cooldown counter to prevent too frequent adjustments
        cooldown_period = 2  # Wait for 2 checks before adjusting again
        
        # Store the current worker count
        current_workers = initial_workers
        
        # Track resource history for trend analysis
        cpu_history = []
        memory_history = []
        history_size = 3  # Number of data points to keep for trend analysis
        
        try:
            while True:
                # Get current CPU and memory utilization
                cpu_percent = psutil.cpu_percent(interval=1) / 100  # 0.0 to 1.0
                memory = psutil.virtual_memory()
                memory_percent = memory.percent / 100  # 0.0 to 1.0
                
                # Update history for trend analysis
                cpu_history.append(cpu_percent)
                memory_history.append(memory_percent)
                if len(cpu_history) > history_size:
                    cpu_history.pop(0)
                    memory_history.pop(0)
                
                # Calculate trends (increasing or decreasing)
                cpu_trend = sum(cpu_history) / len(cpu_history) if cpu_history else cpu_percent
                memory_trend = sum(memory_history) / len(memory_history) if memory_history else memory_percent
                
                # Log resource utilization periodically
                logger.debug(
                    f"Resource utilization for job {job_id}: "
                    f"CPU: {cpu_percent:.2%}, Memory: {memory_percent:.2%}, "
                    f"Workers: {current_workers}"
                )
                
                # Check if we need to adjust worker count and if we're not in cooldown period
                if adjustment_cooldown <= 0 and executor is not None:
                    if cpu_percent > critical_threshold or memory_percent > critical_threshold:
                        # Critical load - reduce workers more aggressively
                        new_workers = max(1, int(current_workers * 0.6))  # Reduce by 40%
                        if new_workers < current_workers:
                            logger.warning(
                                f"Critical resource utilization during job {job_id}: "
                                f"CPU: {cpu_percent:.2%}, Memory: {memory_percent:.2%}. "
                                f"Reducing worker threads from {current_workers} to {new_workers}"
                            )
                            self._adjust_thread_pool_size(executor, new_workers)
                            current_workers = new_workers
                            adjustment_cooldown = cooldown_period
                    elif cpu_percent > warning_threshold or memory_percent > warning_threshold:
                        # High load - reduce workers moderately
                        new_workers = max(1, int(current_workers * 0.8))  # Reduce by 20%
                        if new_workers < current_workers:
                            logger.warning(
                                f"High resource utilization during job {job_id}: "
                                f"CPU: {cpu_percent:.2%}, Memory: {memory_percent:.2%}. "
                                f"Reducing worker threads from {current_workers} to {new_workers}"
                            )
                            self._adjust_thread_pool_size(executor, new_workers)
                            current_workers = new_workers
                            adjustment_cooldown = cooldown_period
                    elif cpu_trend < 0.5 and memory_trend < 0.6 and current_workers < initial_workers:
                        # Low load and we previously reduced workers - try to increase back up to initial
                        new_workers = min(initial_workers, current_workers + 1)
                        if new_workers > current_workers:
                            logger.info(
                                f"Resource utilization is low for job {job_id}: "
                                f"CPU: {cpu_percent:.2%}, Memory: {memory_percent:.2%}. "
                                f"Increasing worker threads from {current_workers} to {new_workers}"
                            )
                            self._adjust_thread_pool_size(executor, new_workers)
                            current_workers = new_workers
                            adjustment_cooldown = cooldown_period
                
                # Decrement cooldown counter if needed
                if adjustment_cooldown > 0:
                    adjustment_cooldown -= 1
                
                # Sleep before next check
                time.sleep(check_interval)
        except Exception as e:
            # Non-critical monitoring thread, just log errors
            logger.error(f"Error in resource monitoring thread: {str(e)}")
        finally:
            logger.debug(f"Resource monitoring for job {job_id} completed")
    
    def _adjust_thread_pool_size(self, executor: ThreadPoolExecutor, new_worker_count: int) -> None:
        """
        Dynamically adjust the thread pool size during processing.
        
        Args:
            executor: The ThreadPoolExecutor to adjust
            new_worker_count: The new number of worker threads
        """
        try:
            # This is a bit of a hack since ThreadPoolExecutor doesn't officially support
            # dynamic resizing, but we can adjust the max_workers attribute
            if hasattr(executor, '_max_workers'):
                # Access the private attribute (not ideal but necessary)
                executor._max_workers = new_worker_count
                logger.debug(f"Adjusted thread pool size to {new_worker_count} workers")
            else:
                logger.warning("Could not adjust thread pool size: _max_workers attribute not found")
        except Exception as e:
            logger.error(f"Error adjusting thread pool size: {str(e)}")
    
    def _submit_conversion_task(
        self, executor: ThreadPoolExecutor, job: ConversionJob, fmt: str, 
        ffmpeg_options: Dict, futures: Dict, video_info: Optional[Dict] = None
    ) -> None:
        """
        Submit a single format conversion task to the thread pool with format-specific optimizations.
        
        Args:
            executor: ThreadPoolExecutor instance
            job: ConversionJob to process
            fmt: Format to convert to (mp4, webm, etc.)
            ffmpeg_options: FFmpeg conversion options
            futures: Dictionary to store futures for tracking
            video_info: Optional video metadata for optimizations
        """
        logger.info(f"Submitting format {fmt} for job {job.id}")
        
        # Get format-specific options
        format_options = ffmpeg_options.get(fmt, {})
        if not format_options:
            logger.warning(f"No options found for format {fmt}, skipping")
            return
        
        # Apply format-specific optimizations based on video characteristics
        if video_info:
            optimized_options = self._optimize_format_options(fmt, format_options, video_info)
            logger.debug(f"Optimized options for {fmt}: {optimized_options}")
        else:
            optimized_options = format_options
        
        # Submit the conversion task to the thread pool
        future = executor.submit(
            self.convert_video,
            job.temp_file_path,
            fmt,
            optimized_options,
            job.preserve_audio,
        )
        futures[future] = fmt
    
    def _process_conversion_result(
        self, future, fmt: str, original_size: float, 
        conversion_results: Dict[str, str], failed_formats: List[str],
        timeout: Optional[int] = None
    ) -> None:
        """
        Process the result of a conversion task with enhanced error handling.
        
        Args:
            future: Future object from the thread pool
            fmt: Format being converted
            original_size: Size of the original file in MB
            conversion_results: Dictionary to store successful conversion results
            failed_formats: List to store failed formats
            timeout: Optional timeout in seconds (if None, will be calculated)
        """
        try:
            # Set a reasonable timeout based on video size and format if not provided
            if timeout is None:
                timeout = self._calculate_timeout(original_size, fmt)
                
            # Wait for the conversion to complete with timeout
            start_time = time.time()
            converted_file_path = future.result(timeout=timeout)
            conversion_time = time.time() - start_time
            
            # Store the result and log success
            conversion_results[fmt] = converted_file_path
            logger.info(
                f"Conversion for format {fmt} completed successfully in {conversion_time:.2f}s "
                f"(Throughput: {original_size / conversion_time:.2f}MB/s)"
            )
        except TimeoutError:
            logger.error(f"Conversion for format {fmt} timed out after {timeout} seconds")
            failed_formats.append(fmt)
        except Exception as e:
            logger.error(f"Conversion for format {fmt} failed: {str(e)}")
            failed_formats.append(fmt)
            
    def _optimize_format_options(self, fmt: str, base_options: Dict, video_info: Dict) -> Dict:
        """
        Optimize FFmpeg options for a specific format based on video characteristics.
        
        Args:
            fmt: Format to optimize for (mp4, webm, etc.)
            base_options: Base FFmpeg options
            video_info: Video metadata
            
        Returns:
            Optimized FFmpeg options
        """
        # Start with the base options
        optimized_options = base_options.copy()
        
        # Get video characteristics
        width = video_info.get('width', 0)
        height = video_info.get('height', 0)
        bitrate = video_info.get('bit_rate', 0)
        duration = video_info.get('duration', 0)
        codec = video_info.get('codec_name', '').lower()
        
        # Format-specific optimizations
        if fmt == 'webm':
            # WebM (VP9) optimizations
            # For high-resolution videos, use 2-pass encoding for better quality/size ratio
            if width * height > 1280 * 720:
                optimized_options.setdefault('vcodec', 'libvpx-vp9')
                # Use constrained quality mode for better performance
                optimized_options.setdefault('crf', 31)  # Lower is better quality
                optimized_options.setdefault('b:v', '0')  # Let CRF control bitrate
                
                # CPU-specific optimizations
                cpu_count = psutil.cpu_count(logical=True)
                if cpu_count >= 4:
                    # Use row-multithreading for better parallelism
                    optimized_options.setdefault('row-mt', 1)
                    optimized_options.setdefault('tile-columns', 2)
                    optimized_options.setdefault('threads', min(4, cpu_count))
            
        elif fmt == 'mp4':
            # MP4 (H.264) optimizations
            optimized_options.setdefault('vcodec', 'libx264')
            
            # For shorter videos, prioritize quality
            if duration and duration < 300:  # Less than 5 minutes
                optimized_options.setdefault('crf', 23)  # Better quality for short videos
            else:
                optimized_options.setdefault('crf', 26)  # More compression for longer videos
                
            # Use faster preset for larger videos to improve performance
            if width * height > 1920 * 1080:
                optimized_options.setdefault('preset', 'faster')
            elif width * height > 1280 * 720:
                optimized_options.setdefault('preset', 'medium')
            else:
                optimized_options.setdefault('preset', 'slow')  # Better quality for smaller videos
                
        elif fmt == 'mov':
            # MOV (H.264) optimizations similar to MP4 but with different container
            optimized_options.setdefault('vcodec', 'libx264')
            optimized_options.setdefault('crf', 23)  # Better quality for MOV
            
            # Use faststart for better streaming
            optimized_options.setdefault('movflags', '+faststart')
            
        # Common optimizations for all formats
        # Adjust audio quality based on video quality
        if bitrate:
            bitrate_mbps = bitrate / 1000000
            if bitrate_mbps > 10:
                # High quality video, use better audio
                optimized_options.setdefault('b:a', '192k')
            elif bitrate_mbps > 5:
                # Medium quality video
                optimized_options.setdefault('b:a', '128k')
            else:
                # Lower quality video
                optimized_options.setdefault('b:a', '96k')
                
        return optimized_options
    
    def _upload_converted_files(
        self, job: ConversionJob, conversion_results: Dict[str, str]
    ) -> Dict[str, bool]:
        """
        Upload converted files to R2 storage with circuit breaker awareness.
        
        Returns:
            Dictionary mapping formats to upload success status
        """
        upload_results = {}
        circuit_breaker_open = False
        
        for fmt, file_path in conversion_results.items():
            # Skip remaining uploads if circuit breaker is open
            if circuit_breaker_open:
                upload_results[fmt] = False
                logger.warning(f"Skipping upload of {fmt} file due to open circuit breaker")
                continue
                
            try:
                # Generate object key for R2
                object_key = f"{fmt}/{Path(file_path).stem}.{fmt}"
                
                # Upload to R2 with retry logic
                public_url, size_mb = self._upload_with_retry(file_path, object_key)
                
                # Add converted file to job
                job.add_converted_file(fmt, public_url, size_mb)
                upload_results[fmt] = True
                
            except CircuitBreakerError as e:
                # Circuit breaker is open, mark this and all remaining formats as failed
                circuit_breaker_open = True
                upload_results[fmt] = False
                
                # Log the circuit breaker error
                logger.error(f"Circuit breaker open during upload of {fmt} file: {str(e)}")
                
                # Update job status to reflect the circuit breaker issue
                job.add_error(f"R2 storage service is currently unavailable: {str(e)}")
                
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
    
    def _handle_job_failure(self, job: ConversionJob, exception: Exception, start_time: float, video_info: Optional[Dict] = None) -> None:
        """
        Handle job failure with enhanced error reporting and cleanup.
        
        Args:
            job: The conversion job that failed
            exception: The exception that caused the failure
            start_time: The time when the job started
            video_info: Optional video metadata for better error reporting
        """
        # Log the error with detailed information
        error_context = self._get_error_context(exception, video_info)
        logger.error(
            f"Job {job.id} failed: {str(exception)}. "
            f"Error context: {error_context}"
        )
        
        # Update job status with detailed error information
        job.status = JobStatus.FAILED
        job.error_message = str(exception)
        
        # Add detailed error context if available
        if error_context:
            job.add_error_detail(error_context)
            
        job.completed_at = datetime.now()
        
        # Log failure metrics with detailed timing information
        total_time = time.time() - start_time
        logger.info(
            f"Job {job.id} failed after {total_time:.2f}s. "
            f"Video info available: {bool(video_info)}"
        )
        
        # Clean up temporary files and any partial results
        try:
            # Clean up the original temporary file
            if job.temp_file_path and os.path.exists(job.temp_file_path):
                os.remove(job.temp_file_path)
                logger.info(f"Cleaned up temporary file for failed job {job.id}")
                
            # Clean up any partial output files in the temp directory
            self._cleanup_partial_outputs(job)
                
        except Exception as cleanup_error:
            logger.error(f"Error during cleanup: {str(cleanup_error)}")
            
    def _get_error_context(self, exception: Exception, video_info: Optional[Dict]) -> Dict:
        """
        Get detailed context information for an error to aid in debugging.
        
        Args:
            exception: The exception that occurred
            video_info: Optional video metadata
            
        Returns:
            Dictionary with error context information
        """
        context = {
            'error_type': type(exception).__name__,
            'error_message': str(exception),
            'system_info': {
                'cpu_count': psutil.cpu_count(logical=True),
                'memory_available_gb': psutil.virtual_memory().available / (1024 * 1024 * 1024),
                'system_load': psutil.cpu_percent(interval=0.1),
            }
        }
        
        # Add video information if available
        if video_info:
            context['video_info'] = {
                'resolution': f"{video_info.get('width', 'unknown')}x{video_info.get('height', 'unknown')}",
                'duration': video_info.get('duration', 'unknown'),
                'codec': video_info.get('codec_name', 'unknown'),
                'bitrate': video_info.get('bit_rate', 'unknown'),
            }
            
        # Add specific error context based on exception type
        if isinstance(exception, VideoProcessingError):
            context['error_category'] = 'video_processing'
        elif isinstance(exception, StorageError):
            context['error_category'] = 'storage'
        elif isinstance(exception, CircuitBreakerError):
            context['error_category'] = 'circuit_breaker'
            context['service_name'] = getattr(exception, 'service_name', 'unknown')
            context['open_until'] = getattr(exception, 'open_until', 'unknown')
        elif isinstance(exception, TimeoutError):
            context['error_category'] = 'timeout'
        else:
            context['error_category'] = 'general'
            
        return context
        
    def _cleanup_partial_outputs(self, job: ConversionJob) -> None:
        """
        Clean up any partial output files that may have been created during a failed job.
        
        Args:
            job: The failed conversion job
        """
        # Get the base name of the input file without extension
        if not job.temp_file_path:
            return
            
        base_name = Path(job.temp_file_path).stem
        temp_dir = self.temp_dir
        
        # Look for any files in the temp directory that match the base name
        try:
            for fmt in ['mp4', 'webm', 'mov']:
                pattern = f"{base_name}*.{fmt}"
                for file_path in temp_dir.glob(pattern):
                    try:
                        os.remove(file_path)
                        logger.info(f"Cleaned up partial output file: {file_path}")
                    except Exception as e:
                        logger.warning(f"Failed to remove partial output file {file_path}: {str(e)}")
        except Exception as e:
            logger.error(f"Error searching for partial output files: {str(e)}")
    
    def _calculate_optimal_workers(self, formats: List[str], video_info: Optional[Dict] = None) -> int:
        """
        Calculate the optimal number of worker threads based on formats, video characteristics,
        content type, and real-time system resource availability.
        
        Args:
            formats: List of output formats
            video_info: Optional dictionary with video metadata
            
        Returns:
            Optimal number of worker threads
        """
        # Base worker count - start with CPU count
        cpu_count = psutil.cpu_count(logical=True)
        base_workers = min(cpu_count, settings.MAX_WORKERS)
        
        # Adjust for current system load with more granular scaling
        cpu_percent = psutil.cpu_percent() / 100  # Convert to 0-1 range
        memory = psutil.virtual_memory()
        memory_percent = memory.percent / 100  # Convert to 0-1 range
        
        # Calculate load factor with more granular scaling
        if cpu_percent > 0.9 or memory_percent > 0.9:
            # System under critical load - drastically reduce workers
            load_factor = 0.4
            logger.warning(f"System under critical load: CPU {cpu_percent:.2%}, Memory {memory_percent:.2%}")
        elif cpu_percent > 0.8 or memory_percent > 0.8:
            # System under heavy load - reduce workers significantly
            load_factor = 0.6
            logger.warning(f"System under heavy load: CPU {cpu_percent:.2%}, Memory {memory_percent:.2%}")
        elif cpu_percent > 0.7 or memory_percent > 0.7:
            # System under substantial load - reduce workers moderately
            load_factor = 0.7
            logger.info(f"System under substantial load: CPU {cpu_percent:.2%}, Memory {memory_percent:.2%}")
        elif cpu_percent > 0.6 or memory_percent > 0.6:
            # System under moderate load - slightly reduce workers
            load_factor = 0.8
            logger.info(f"System under moderate load: CPU {cpu_percent:.2%}, Memory {memory_percent:.2%}")
        else:
            # System under light load - use full capacity
            load_factor = 1.0
            logger.debug(f"System under light load: CPU {cpu_percent:.2%}, Memory {memory_percent:.2%}")
            
        # Adjust for number of formats with more sophisticated scaling
        format_count = len(formats)
        if format_count >= 4:
            # Many formats - need more parallelization but avoid overloading
            format_factor = 1.1  # Slight boost for many formats
        elif format_count == 3:
            format_factor = 1.0  # Full capacity for 3 formats
        elif format_count == 2:
            format_factor = 0.9  # Slightly reduced for 2 formats
        else:  # 1 format
            format_factor = 0.8  # More reduced for single format
        
        # Detect content type if video info is available
        content_type = 'general'
        if video_info:
            content_type = self._detect_content_type(video_info)
            
        # Content type specific adjustments
        content_type_factor = 1.0
        if content_type == 'animation':
            # Animation typically compresses well and benefits from more threads
            content_type_factor = 1.2
        elif content_type == 'gaming':
            # Gaming content has high motion and benefits from more threads
            content_type_factor = 1.1
        elif content_type == 'screencast':
            # Screencasts are often simpler and need fewer threads
            content_type_factor = 0.9
        elif content_type == 'film':
            # Film content is complex but benefits from focused processing
            content_type_factor = 1.0
            
        # Apply video complexity factor if available
        complexity_factor = 1.0
        if video_info:
            complexity_factor = self._calculate_video_complexity_factor(video_info)
            
        # Format-specific adjustments
        format_specific_factor = 1.0
        if 'webm' in formats:
            # WebM encoding is more CPU intensive
            format_specific_factor *= 1.2
        if 'gif' in formats:
            # GIF encoding is less CPU intensive
            format_specific_factor *= 0.9
            
        # Calculate adjusted worker count
        adjusted_workers = max(1, int(base_workers * load_factor * format_factor * 
                                     complexity_factor * content_type_factor * format_specific_factor))
        
        # Ensure we don't exceed system memory constraints with more accurate estimation
        available_memory_mb = memory.available / (1024 * 1024)  # Convert to MB
        
        # Estimate memory usage per worker based on video resolution and format
        memory_per_worker_mb = 500  # Default estimate
        if video_info and 'width' in video_info and 'height' in video_info:
            width = video_info.get('width', 0)
            height = video_info.get('height', 0)
            resolution = width * height
            
            # Scale memory requirements based on resolution
            if resolution > 3840 * 2160:  # 8K
                memory_per_worker_mb = 1500
            elif resolution > 1920 * 1080:  # 4K
                memory_per_worker_mb = 1000
            elif resolution > 1280 * 720:  # 1080p
                memory_per_worker_mb = 750
            else:  # 720p or lower
                memory_per_worker_mb = 500
                
            # Adjust for formats
            if 'webm' in formats:
                memory_per_worker_mb *= 1.2  # WebM needs more memory
                
        # Calculate memory-based worker limit
        memory_based_workers = max(1, int(available_memory_mb / memory_per_worker_mb))
        
        # Reserve some memory for the system (at least 1GB or 10% of total, whichever is greater)
        total_memory_gb = memory.total / (1024 * 1024 * 1024)  # Convert to GB
        min_reserve_gb = max(1, total_memory_gb * 0.1)  # At least 1GB or 10% of total
        min_reserve_mb = min_reserve_gb * 1024
        
        # Adjust memory-based workers to account for reserved memory
        safe_memory_mb = max(0, available_memory_mb - min_reserve_mb)
        safe_memory_workers = max(1, int(safe_memory_mb / memory_per_worker_mb))
        
        # Final worker count is the minimum of adjusted workers and safe memory workers
        final_workers = min(adjusted_workers, safe_memory_workers)
        
        # Log the calculation factors
        logger.debug(
            f"Worker calculation for {content_type} content: base={base_workers}, "
            f"load_factor={load_factor:.2f}, format_factor={format_factor:.2f}, "
            f"complexity_factor={complexity_factor:.2f}, content_type_factor={content_type_factor:.2f}, "
            f"format_specific_factor={format_specific_factor:.2f}, "
            f"memory_per_worker={memory_per_worker_mb}MB, memory_based={safe_memory_workers}, "
            f"final={final_workers}"
        )
        
        return final_workers
    
    def _detect_content_type(self, video_info: Dict) -> str:
        """
        Detect the type of content in a video based on its characteristics.
        
        Args:
            video_info: Dictionary containing video metadata
            
        Returns:
            Content type: 'animation', 'gaming', 'screencast', 'film', or 'general'
        """
        # Default to 'general' if we can't determine content type
        if not video_info:
            return 'general'
            
        # Extract relevant metadata
        codec = video_info.get('codec_name', '').lower()
        width = video_info.get('width', 0)
        height = video_info.get('height', 0)
        fps_str = video_info.get('r_frame_rate', '')
        bitrate = video_info.get('bit_rate', 0)
        audio_codec = video_info.get('audio_codec_name', '').lower()
        tags = video_info.get('tags', {})
        
        # Calculate frame rate
        fps = 0
        if fps_str and '/' in fps_str:
            try:
                num, den = map(int, fps_str.split('/'))
                fps = num / den if den != 0 else 0
            except (ValueError, ZeroDivisionError):
                pass
                
        # Look for specific indicators in metadata
        
        # Animation indicators
        animation_indicators = 0
        
        # Animations often have specific aspect ratios
        if width and height:
            aspect_ratio = width / height if height != 0 else 0
            if abs(aspect_ratio - 16/9) < 0.1 or abs(aspect_ratio - 4/3) < 0.1:
                animation_indicators += 1
                
        # Animations often have specific frame rates (24fps for traditional, 12-15fps for limited)
        if 22 <= fps <= 26 or 12 <= fps <= 15:
            animation_indicators += 1
            
        # Animations often have specific tags
        title = tags.get('title', '').lower()
        genre = tags.get('genre', '').lower()
        if (any(term in title for term in ['anime', 'animation', 'cartoon', 'animated']) or 
            any(term in genre for term in ['anime', 'animation', 'cartoon'])):
            animation_indicators += 2
            
        # Gaming indicators
        gaming_indicators = 0
        
        # Gaming videos often have specific resolutions
        if ((width == 1920 and height == 1080) or 
            (width == 2560 and height == 1440) or 
            (width == 3840 and height == 2160)):
            gaming_indicators += 1
            
        # Gaming videos often have high frame rates
        if fps >= 60:
            gaming_indicators += 2
            
        # Gaming videos often have specific tags
        if (any(term in title for term in ['gameplay', 'gaming', 'playthrough', 'game']) or 
            any(term in genre for term in ['game', 'gaming'])):
            gaming_indicators += 2
            
        # Screencast indicators
        screencast_indicators = 0
        
        # Screencasts often have specific frame rates (usually 30fps or lower)
        if 0 < fps <= 30:
            screencast_indicators += 1
            
        # Screencasts often have specific aspect ratios (typically 16:9 or 16:10)
        if width and height:
            aspect_ratio = width / height if height != 0 else 0
            if abs(aspect_ratio - 16/9) < 0.1 or abs(aspect_ratio - 16/10) < 0.1:
                screencast_indicators += 1
                
        # Screencasts often have specific tags
        if (any(term in title for term in ['tutorial', 'screencast', 'screen recording', 'demo']) or 
            any(term in genre for term in ['tutorial', 'educational', 'demonstration'])):
            screencast_indicators += 2
            
        # Film indicators
        film_indicators = 0
        
        # Films often have specific frame rates (typically 24fps)
        if 23 <= fps <= 25:
            film_indicators += 2
            
        # Films often have high bitrates
        if bitrate > 8000000:  # > 8 Mbps
            film_indicators += 1
            
        # Films often have specific audio codecs
        if audio_codec in ['aac', 'ac3', 'eac3', 'dts']:
            film_indicators += 1
            
        # Films often have specific tags
        if (any(term in title for term in ['movie', 'film', 'cinema', 'trailer']) or 
            any(term in genre for term in ['movie', 'film', 'drama', 'action', 'comedy', 'thriller'])):
            film_indicators += 2
            
        # Determine content type based on indicators
        scores = {
            'animation': animation_indicators,
            'gaming': gaming_indicators,
            'screencast': screencast_indicators,
            'film': film_indicators,
            'general': 0  # Default score for general content
        }
        
        # Get the content type with the highest score
        content_type = max(scores.items(), key=lambda x: x[1])[0]
        
        # If the highest score is 0, default to 'general'
        if scores[content_type] == 0:
            content_type = 'general'
            
        logger.debug(f"Detected content type: {content_type} (scores: {scores})")
        return content_type
        
    def _calculate_video_complexity_factor(self, video_info: Dict) -> float:
        """
        Calculate a complexity factor for a video based on its characteristics and content type.
        This factor is used to adjust thread allocation based on video complexity.
        
        Args:
            video_info: Dictionary containing video metadata
            
        Returns:
            Complexity factor (1.0 is baseline, higher means more complex)
        """
        # Default to 1.0 if we can't determine complexity
        if not video_info:
            return 1.0
            
        # Resolution factor - higher resolution videos are more complex
        width = video_info.get('width', 0)
        height = video_info.get('height', 0)
        resolution = width * height if width and height else 0
        
        if resolution > 3840 * 2160:  # 8K
            resolution_factor = 2.0
        elif resolution > 1920 * 1080:  # 4K
            resolution_factor = 1.5
        elif resolution > 1280 * 720:  # 1080p
            resolution_factor = 1.2
        elif resolution > 640 * 480:  # 720p
            resolution_factor = 1.0
        else:  # SD or lower
            resolution_factor = 0.8
            
        # Bitrate factor - higher bitrate videos are more complex
        bitrate = video_info.get('bit_rate', 0)
        if bitrate:
            bitrate_mbps = bitrate / 1000000  # Convert to Mbps
            if bitrate_mbps > 50:  # Ultra high bitrate
                bitrate_factor = 1.5
            elif bitrate_mbps > 20:  # Very high bitrate
                bitrate_factor = 1.3
            elif bitrate_mbps > 10:  # High bitrate
                bitrate_factor = 1.1
            elif bitrate_mbps > 5:  # Medium bitrate
                bitrate_factor = 1.0
            else:  # Low bitrate
                bitrate_factor = 0.9
        else:
            bitrate_factor = 1.0
            
        # Duration factor - longer videos may need more resources
        duration = video_info.get('duration', 0)
        if duration:
            if duration > 3600:  # > 60 minutes
                duration_factor = 1.3
            elif duration > 1800:  # > 30 minutes
                duration_factor = 1.2
            elif duration > 600:  # > 10 minutes
                duration_factor = 1.1
            elif duration > 300:  # > 5 minutes
                duration_factor = 1.0
            else:  # Short video
                duration_factor = 0.9
        else:
            duration_factor = 1.0
            
        # Codec factor - some codecs are more complex to decode/encode
        codec = video_info.get('codec_name', '').lower()
        if codec in ['h265', 'hevc', 'vp9', 'av1']:
            codec_factor = 1.3  # Modern codecs are more CPU intensive
        elif codec in ['h264', 'avc']:
            codec_factor = 1.0  # Standard codec
        elif codec in ['mpeg4', 'mpeg2video']:
            codec_factor = 0.9  # Older codecs are simpler
        else:
            codec_factor = 1.1  # Other codecs
        
        # Frame rate factor - higher frame rates need more processing power
        fps = video_info.get('r_frame_rate', '')
        if fps and '/' in fps:
            try:
                num, den = map(int, fps.split('/'))
                frame_rate = num / den if den != 0 else 0
                
                if frame_rate > 60:
                    fps_factor = 1.3  # High frame rate (60+ fps)
                elif frame_rate > 30:
                    fps_factor = 1.1  # Medium frame rate (30-60 fps)
                else:
                    fps_factor = 1.0  # Standard frame rate (<=30 fps)
            except (ValueError, ZeroDivisionError):
                fps_factor = 1.0
        else:
            fps_factor = 1.0
        
        # Content type factor - different content types have different complexity
        content_type = self._detect_content_type(video_info)
        if content_type == 'animation':
            content_type_factor = 0.9  # Animation is usually simpler to encode
        elif content_type == 'gaming':
            content_type_factor = 1.2  # Gaming has high motion and detail
        elif content_type == 'screencast':
            content_type_factor = 0.8  # Screencasts have low motion
        elif content_type == 'film':
            content_type_factor = 1.1  # Film has grain and texture
        else:  # 'general'
            content_type_factor = 1.0
        
        # Combine all factors with appropriate weights
        complexity_factor = (
            resolution_factor * 0.3 +    # Resolution is important
            bitrate_factor * 0.2 +      # Bitrate is important
            codec_factor * 0.15 +       # Codec affects complexity
            fps_factor * 0.15 +         # Frame rate affects complexity
            content_type_factor * 0.1 + # Content type affects complexity
            duration_factor * 0.1       # Duration has some impact
        )
        
        # Log the complexity calculation for debugging
        logger.debug(
            f"Video complexity calculation: resolution={resolution_factor:.2f}, "
            f"bitrate={bitrate_factor:.2f}, codec={codec_factor:.2f}, "
            f"fps={fps_factor:.2f}, content={content_type_factor:.2f}, "
            f"duration={duration_factor:.2f}, final={complexity_factor:.2f}"
        )
        
        return complexity_factor
        
    def _calculate_timeout(self, file_size_mb: float, format: str, video_info: Optional[Dict] = None) -> int:
        """
        Calculate an appropriate timeout for a conversion task based on file size, format,
        and video characteristics.
        
        Args:
            file_size_mb: Size of the original file in MB
            format: Output format
            video_info: Optional dictionary containing video metadata
            
        Returns:
            Timeout in seconds
        """
        # Base timeout calculation - 1 second per MB with a minimum of 30 seconds
        base_timeout = max(30, int(file_size_mb))
        
        # Apply video complexity factor if available
        if video_info:
            complexity_factor = self._calculate_video_complexity_factor(video_info)
            base_timeout = int(base_timeout * complexity_factor)
        
        # Format-specific adjustments
        if format == 'webm':
            # WebM encoding is slower, allow more time
            return base_timeout * 3
        elif format == 'mov':
            # MOV is slightly slower than MP4
            return base_timeout * 2
        else:  # mp4 and others
            return base_timeout
    
    def _upload_with_retry(self, file_path: str, object_key: str, max_retries: int = 5, base_delay: float = 0.5, max_delay: float = 30.0) -> Tuple[str, float]:
        """
        Upload a file to R2 with exponential backoff retry logic and circuit breaker awareness.
        
        Args:
            file_path: Path to the file to upload
            object_key: Object key for R2
            max_retries: Maximum number of retry attempts
            base_delay: Initial delay between retries in seconds
            max_delay: Maximum delay between retries in seconds
            
        Returns:
            Tuple of (public_url, size_mb)
            
        Raises:
            StorageError: If all upload attempts fail
            CircuitBreakerError: If the circuit breaker is open due to persistent failures
        """
        retries = 0
        last_error = None
        
        while retries < max_retries:
            try:
                # Upload to R2 (this may raise CircuitBreakerError)
                public_url, size_mb = r2_uploader.upload_file(file_path, object_key)
                
                # If successful after retries, log it
                if retries > 0:
                    logger.info(f"Successfully uploaded {object_key} after {retries} retries")
                    
                return public_url, size_mb
                
            except CircuitBreakerError as e:
                # Circuit breaker is open - propagate this error immediately
                # No point in retrying if the circuit breaker has determined the service is down
                logger.warning(f"Circuit breaker open during upload of {object_key}: {str(e)}")
                raise
                
            except Exception as e:
                retries += 1
                last_error = e
                
                if retries >= max_retries:
                    logger.error(f"Upload failed after {max_retries} attempts: {str(e)}")
                    break
                    
                # Calculate delay with exponential backoff and jitter
                # Formula: min(max_delay, base_delay * 2^retry) + random jitter
                delay = min(max_delay, base_delay * (2 ** (retries - 1)))
                # Add jitter (random value between 0 and 20% of the delay)
                jitter = random.uniform(0, 0.2 * delay)
                total_delay = delay + jitter
                
                logger.warning(
                    f"Upload attempt {retries} failed: {str(e)}. "
                    f"Retrying in {total_delay:.2f} seconds..."
                )
                
                time.sleep(total_delay)
        
        # If we get here, all retries failed
        error_msg = f"Failed to upload {object_key} after {max_retries} attempts: {str(last_error)}"
        logger.error(error_msg)
        raise StorageError(error_msg)


# Singleton instance
converter = VideoConverter()