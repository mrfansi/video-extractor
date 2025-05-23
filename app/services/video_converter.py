import os
import asyncio
import ffmpeg
import uuid
import time
import json
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
from app.core.config import settings
from app.core.logging import logger, log_error, log_performance

class VideoConverter:
    """Service for converting videos to optimized formats"""
    
    def __init__(self):
        self.temp_dir = settings.temp_dir
        self.max_workers = settings.max_workers
        
    async def process_video(self, input_file: str) -> Dict[str, Dict]:
        """Process a video file into multiple optimized formats
        
        Args:
            input_file: Path to the input video file
            
        Returns:
            Dictionary with information about the original and converted files
        """
        logger.info(f"Starting video processing for file: {input_file}")
        process_start_time = time.time()
        
        # Get video information
        video_info = self._get_video_info(input_file)
        
        # Define output formats
        formats = [
            {"name": "mp4", "extension": "mp4", "codec": "h264"},
            {"name": "webm", "extension": "webm", "codec": "vp9"},
            {"name": "mov", "extension": "mov", "codec": "h264"}
        ]
        
        # Create unique ID for this conversion job
        job_id = str(uuid.uuid4())
        job_dir = os.path.join(self.temp_dir, job_id)
        os.makedirs(job_dir, exist_ok=True)
        
        # Process each format concurrently
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            tasks = []
            for fmt in formats:
                output_file = os.path.join(job_dir, f"output.{fmt['extension']}")
                tasks.append(
                    asyncio.get_event_loop().run_in_executor(
                        executor,
                        self._convert_video,
                        input_file,
                        output_file,
                        fmt,
                        video_info
                    )
                )
            
            # Wait for all conversions to complete
            results = await asyncio.gather(*tasks)
        
        # Organize results
        output = {
            "original": {
                "filename": os.path.basename(input_file),
                "size": os.path.getsize(input_file),
                "resolution": f"{video_info['width']}x{video_info['height']}"
            },
            "formats": {}
        }
        
        for fmt, result in zip(formats, results):
            if result:
                output_file, file_size = result
                output["formats"][fmt["name"]] = {
                    "filename": os.path.basename(output_file),
                    "size": file_size,
                    "resolution": f"{video_info['width']}x{video_info['height']}",
                    "path": output_file
                }
        
        log_performance("process_video", (time.time() - process_start_time) * 1000, {
            "input_file": input_file,
            "formats": [fmt["name"] for fmt in formats]
        })
        
        return output, job_dir
    
    def _get_video_info(self, input_file: str) -> Dict:
        """Get information about the input video
        
        Args:
            input_file: Path to the input video file
            
        Returns:
            Dictionary with video information
        """
        logger.info(f"Getting video information for {input_file}")
        
        # Build the FFprobe command
        command = [
            "ffprobe",
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height,duration,bit_rate",
            "-of", "json",
            input_file
        ]
        
        try:
            # Run the command
            result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            # Parse the JSON output
            import json
            info = json.loads(result.stdout)
            
            # Extract the information
            stream = info['streams'][0]
            
            # Prepare the video info
            video_info = {
                "width": int(stream['width']),
                "height": int(stream['height']),
                "duration": float(stream.get('duration', 0)),
                "bitrate": int(stream.get('bit_rate', 0))
            }
            
            logger.debug(f"Video info: {json.dumps(video_info)}")
            return video_info
            
        except subprocess.CalledProcessError as e:
            error_message = f"FFprobe error: {e.stderr.decode() if e.stderr else str(e)}"
            log_error("ffprobe_error", error_message, {
                "input_file": input_file,
                "command": ' '.join(command),
                "return_code": e.returncode
            })
            logger.error(error_message)
            raise
        except json.JSONDecodeError as e:
            error_message = f"Error parsing FFprobe output: {str(e)}"
            log_error("ffprobe_json_error", error_message, {
                "input_file": input_file,
                "output": result.stdout.decode() if 'result' in locals() else "No output"
            })
            logger.error(error_message)
            raise
    
    def _convert_video(self, input_file: str, output_file: str, format_info: Dict, video_info: Dict) -> Optional[Tuple[str, int]]:
        """Convert a video to a specific format with optimization
        
        Args:
            input_file: Path to the input video file
            output_file: Path to the output video file
            format_info: Dictionary with format information
            video_info: Dictionary with video information
            
        Returns:
            Tuple with output file path and file size, or None if conversion failed
        """
        logger.info(f"Converting video to {format_info['name']} using codec {format_info['codec']}")
        conversion_start_time = time.time()
        
        try:
            # Base settings to maintain quality while reducing size
            stream = ffmpeg.input(input_file)
            
            # Format-specific optimization settings
            if format_info["codec"] == "h264":
                # H.264 optimization (for MP4 and MOV)
                # CRF 23 is a good balance between quality and file size
                # Preset 'slow' provides better compression than 'medium' at the cost of encoding time
                stream = ffmpeg.output(
                    stream,
                    output_file,
                    vcodec="libx264",
                    crf=23,
                    preset="slow",
                    acodec="aac",
                    audio_bitrate="128k",
                    **{"-movflags": "+faststart"}
                )
                logger.debug(f"Using H.264 optimization settings for {format_info['name']}")
            elif format_info["codec"] == "vp9":
                # VP9 optimization (for WebM)
                # CRF 31 for VP9 is roughly equivalent to CRF 23 for H.264
                # Speed 2 is a good balance between quality and encoding time
                stream = ffmpeg.output(
                    stream,
                    output_file,
                    vcodec="libvpx-vp9",
                    crf=31,
                    **{"b:v": "0"},  # Variable bitrate
                    **{"-deadline": "good", "-cpu-used": 2},
                    acodec="libopus",
                    audio_bitrate="128k"
                )
                logger.debug(f"Using VP9 optimization settings for {format_info['name']}")
            
            # Run the conversion
            logger.info(f"Starting FFmpeg conversion for {format_info['name']} format")
            ffmpeg.run(stream, overwrite_output=True, quiet=True)
            
            # Get output file size
            output_size = os.path.getsize(output_file)
            
            # Calculate conversion time and performance metrics
            conversion_time = time.time() - conversion_start_time
            input_size = os.path.getsize(input_file)
            compression_ratio = input_size / output_size if output_size > 0 else 0
            
            # Log performance metrics
            log_performance(f"convert_to_{format_info['name']}", conversion_time * 1000, {
                "codec": format_info['codec'],
                "input_size": input_size,
                "output_size": output_size,
                "compression_ratio": compression_ratio,
                "duration": video_info.get('duration', 0),
                "resolution": f"{video_info['width']}x{video_info['height']}"
            })
            
            logger.info(
                f"Successfully converted to {format_info['name']}: {output_file} "
                f"({output_size} bytes, {compression_ratio:.2f}x compression) "
                f"in {conversion_time:.2f} seconds"
            )
            
            # Return the output file path and size
            return output_file, output_size
        except Exception as e:
            error_message = f"Error converting to {format_info['name']}: {str(e)}"
            log_error("conversion_error", error_message, {
                "format": format_info['name'],
                "codec": format_info['codec'],
                "input_file": input_file,
                "output_file": output_file,
                "error_type": type(e).__name__
            })
            logger.error(error_message)
            return None
    
    def cleanup(self, job_dir: str) -> None:
        """Clean up temporary files
        
        Args:
            job_dir: Path to the job directory
        """
        logger.info(f"Cleaning up temporary directory: {job_dir}")
        cleanup_start = time.time()
        
        try:
            import shutil
            shutil.rmtree(job_dir)
            cleanup_time = time.time() - cleanup_start
            
            log_performance("cleanup_temp_files", cleanup_time * 1000, {
                "job_dir": job_dir
            })
            
            logger.info(f"Successfully cleaned up temporary directory: {job_dir} in {cleanup_time:.2f} seconds")
        except Exception as e:
            error_message = f"Error cleaning up temporary files: {str(e)}"
            log_error("cleanup_error", error_message, {
                "job_dir": job_dir,
                "error_type": type(e).__name__
            })
            logger.error(error_message)
