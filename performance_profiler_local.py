#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Performance profiling script for the Video Extractor API (Local Version).

This script profiles the key components of the video processing pipeline
without requiring R2 connectivity, focusing on local operations only.
"""

import os
import time
import tempfile
import cProfile
import pstats
import io
import shutil
from memory_profiler import profile as memory_profile
from line_profiler import LineProfiler
import ffmpeg
from pathlib import Path

# Import application components
from app.schemas.video import OptimizationLevel
from app.core.config import settings

# Create a sample video file for testing
def create_test_video(duration=5, size="640x480", output_path=None):
    """
    Create a test video file using FFmpeg.
    
    Args:
        duration: Duration of the test video in seconds
        size: Resolution of the test video
        output_path: Path to save the test video
        
    Returns:
        Path to the created test video file
    """
    if output_path is None:
        fd, output_path = tempfile.mkstemp(suffix=".mp4")
        os.close(fd)
    
    print(f"Creating test video: {output_path}")
    
    # Create a test video with FFmpeg
    (ffmpeg
        .input('color=c=red:s=' + size, f='lavfi', t=duration)
        .output(output_path, vcodec='libx264', pix_fmt='yuv420p')
        .run(overwrite_output=True, quiet=True)
    )
    
    return output_path

# Simplified VideoConverter class for local testing
class LocalVideoConverter:
    """Simplified version of VideoConverter for local testing."""
    
    def __init__(self):
        """Initialize the local video converter."""
        self.temp_dir = Path(settings.TEMP_DIR)
        
        # Ensure temp directory exists
        if not self.temp_dir.exists():
            self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    def get_ffmpeg_options(self, optimize_level, preserve_audio):
        """Get FFmpeg options based on optimization level and audio preference."""
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
    
    def convert_video(self, input_file, output_format, options, preserve_audio):
        """Convert a video file to a specific format using FFmpeg."""
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
            print(f"Converting {input_file} to {output_format}")
            ffmpeg.run(
                video_output,
                overwrite_output=True,
                quiet=True
            )
            
            print(f"Conversion to {output_format} completed: {output_file}")
            
            return output_file
            
        except Exception as e:
            print(f"Error during conversion to {output_format}: {str(e)}")
            raise

# Profile the video conversion process
def profile_video_conversion():
    """
    Profile the video conversion process to identify bottlenecks.
    """
    print("\n=== Profiling Video Conversion ===\n")
    
    # Create a test video
    test_video = create_test_video()
    
    # Initialize the video converter
    converter = LocalVideoConverter()
    
    # Profile with cProfile
    profiler = cProfile.Profile()
    profiler.enable()
    
    # Convert to different formats with different optimization levels
    for fmt in ['mp4', 'webm']:
        for level in [OptimizationLevel.FAST, OptimizationLevel.BALANCED, OptimizationLevel.MAX]:
            print(f"\nConverting to {fmt} with {level.value} optimization...")
            start_time = time.time()
            
            # Get FFmpeg options
            options = converter.get_ffmpeg_options(level, True)
            
            try:
                # Convert the video
                output_file = converter.convert_video(
                    test_video,
                    fmt,
                    options[fmt],
                    True
                )
                
                # Calculate file size
                original_size = os.path.getsize(test_video) / (1024 * 1024)
                converted_size = os.path.getsize(output_file) / (1024 * 1024)
                
                # Calculate compression ratio
                ratio = converted_size / original_size * 100
                
                # Print results
                print(f"  Original size: {original_size:.2f} MB")
                print(f"  Converted size: {converted_size:.2f} MB")
                print(f"  Compression ratio: {ratio:.2f}%")
                print(f"  Conversion time: {time.time() - start_time:.2f} seconds")
                
                # Clean up
                os.remove(output_file)
                
            except Exception as e:
                print(f"  Error: {str(e)}")
    
    profiler.disable()
    
    # Print profiling results
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
    ps.print_stats(20)  # Print top 20 functions by cumulative time
    print(s.getvalue())
    
    # Clean up
    os.remove(test_video)

# Profile memory usage during video processing
@memory_profile
def profile_memory_usage():
    """
    Profile memory usage during video processing.
    """
    print("\n=== Profiling Memory Usage ===\n")
    
    # Create a test video
    test_video = create_test_video(duration=10, size="1280x720")
    
    # Initialize the video converter
    converter = LocalVideoConverter()
    
    # Convert to different formats
    for fmt in ['mp4', 'webm']:
        print(f"\nConverting to {fmt}...")
        
        # Get FFmpeg options
        options = converter.get_ffmpeg_options(OptimizationLevel.BALANCED, True)
        
        try:
            # Convert the video
            output_file = converter.convert_video(
                test_video,
                fmt,
                options[fmt],
                True
            )
            
            # Clean up
            os.remove(output_file)
            
        except Exception as e:
            print(f"  Error: {str(e)}")
    
    # Clean up
    os.remove(test_video)

# Profile line-by-line execution of critical functions
def profile_line_by_line():
    """
    Profile line-by-line execution of critical functions.
    """
    print("\n=== Line-by-Line Profiling ===\n")
    
    # Create a test video
    test_video = create_test_video()
    
    # Initialize the video converter
    converter = LocalVideoConverter()
    
    # Set up line profiler
    lp = LineProfiler()
    lp.add_function(converter.convert_video)
    lp.add_function(converter.get_ffmpeg_options)
    
    # Profile the conversion
    lp_wrapper = lp(lambda: converter.convert_video(
        test_video,
        'mp4',
        converter.get_ffmpeg_options(OptimizationLevel.BALANCED, True)['mp4'],
        True
    ))
    
    # Run the profiled function
    output_file = lp_wrapper()
    
    # Print results
    lp.print_stats()
    
    # Clean up
    os.remove(output_file)
    os.remove(test_video)

# Main function
def main():
    """
    Main function to run all profiling tests.
    """
    print("Starting performance profiling (local version)...\n")
    # Check FFmpeg availability instead of version
    print(f"FFmpeg available: {shutil.which('ffmpeg') is not None}")
    print(f"Temp directory: {settings.TEMP_DIR}")
    
    # Ensure temp directory exists
    os.makedirs(settings.TEMP_DIR, exist_ok=True)
    
    # Run profiling tests
    profile_video_conversion()
    profile_memory_usage()
    profile_line_by_line()
    
    print("\nPerformance profiling completed.")

if __name__ == "__main__":
    main()
