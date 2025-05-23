import os
import asyncio
import ffmpeg
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
from app.core.config import settings

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
        
        return output, job_dir
    
    def _get_video_info(self, input_file: str) -> Dict:
        """Get information about the input video
        
        Args:
            input_file: Path to the input video file
            
        Returns:
            Dictionary with video information
        """
        probe = ffmpeg.probe(input_file)
        video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
        
        if video_stream is None:
            raise ValueError("No video stream found in the input file")
        
        return {
            "width": int(video_stream['width']),
            "height": int(video_stream['height']),
            "duration": float(probe['format']['duration']),
            "bitrate": int(probe['format']['bit_rate']) if 'bit_rate' in probe['format'] else None,
            "codec": video_stream['codec_name']
        }
    
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
            
            # Run the conversion
            ffmpeg.run(stream, overwrite_output=True, quiet=True)
            
            # Return the output file path and size
            return output_file, os.path.getsize(output_file)
        except Exception as e:
            print(f"Error converting to {format_info['name']}: {str(e)}")
            return None
    
    def cleanup(self, job_dir: str) -> None:
        """Clean up temporary files
        
        Args:
            job_dir: Path to the job directory
        """
        try:
            import shutil
            shutil.rmtree(job_dir)
        except Exception as e:
            print(f"Error cleaning up temporary files: {str(e)}")
