#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Performance profiling script for the Video Extractor API.

This script profiles the key components of the video processing pipeline
to identify performance bottlenecks and optimization opportunities.
"""

import os
import time
import tempfile
import cProfile
import pstats
import io
from memory_profiler import profile as memory_profile
from line_profiler import LineProfiler
import ffmpeg
from pathlib import Path

# Import application components
from app.services.converter import VideoConverter
from app.services.r2_uploader import r2_uploader
from app.models.job import ConversionJob, JobStatus
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

# Profile the video conversion process
def profile_video_conversion():
    """
    Profile the video conversion process to identify bottlenecks.
    """
    print("\n=== Profiling Video Conversion ===\n")
    
    # Create a test video
    test_video = create_test_video()
    
    # Initialize the video converter
    converter = VideoConverter()
    
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

# Profile the R2 upload process
def profile_r2_upload():
    """
    Profile the R2 upload process to identify bottlenecks.
    """
    print("\n=== Profiling R2 Upload ===\n")
    
    # Create a test video
    test_video = create_test_video()
    
    # Profile with cProfile
    profiler = cProfile.Profile()
    profiler.enable()
    
    try:
        # Upload to R2
        print("Uploading to R2...")
        start_time = time.time()
        public_url, size_mb = r2_uploader.upload_file(test_video, "test/video.mp4")
        print(f"  Upload time: {time.time() - start_time:.2f} seconds")
        print(f"  File size: {size_mb:.2f} MB")
        print(f"  Public URL: {public_url}")
        
        # Delete from R2
        print("Deleting from R2...")
        start_time = time.time()
        result = r2_uploader.delete_file("test/video.mp4")
        print(f"  Deletion time: {time.time() - start_time:.2f} seconds")
        print(f"  Result: {result}")
        
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
    converter = VideoConverter()
    
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
    converter = VideoConverter()
    
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

# Profile concurrent processing
def profile_concurrent_processing():
    """
    Profile concurrent video processing to evaluate thread pool efficiency.
    """
    print("\n=== Profiling Concurrent Processing ===\n")
    
    # Create multiple test videos
    test_videos = [create_test_video(duration=3) for _ in range(4)]
    
    # Initialize the video converter
    converter = VideoConverter()
    
    # Create conversion jobs
    jobs = []
    for i, video in enumerate(test_videos):
        job = ConversionJob(
            original_filename=f"test_video_{i}.mp4",
            temp_file_path=video,
            formats=['mp4'],
            preserve_audio=True,
            optimize_level=OptimizationLevel.BALANCED
        )
        jobs.append(job)
    
    # Profile with cProfile
    profiler = cProfile.Profile()
    profiler.enable()
    
    # Process jobs concurrently
    start_time = time.time()
    for job in jobs:
        print(f"Processing job {job.id}...")
        converter.process_job(job)
    
    print(f"Total processing time: {time.time() - start_time:.2f} seconds")
    
    profiler.disable()
    
    # Print profiling results
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
    ps.print_stats(20)  # Print top 20 functions by cumulative time
    print(s.getvalue())
    
    # Clean up
    for video in test_videos:
        if os.path.exists(video):
            os.remove(video)

# Main function
def main():
    """
    Main function to run all profiling tests.
    """
    print("Starting performance profiling...\n")
    print(f"FFmpeg version: {ffmpeg.probe(version=True)}")
    print(f"Max workers: {settings.MAX_WORKERS}")
    print(f"Temp directory: {settings.TEMP_DIR}")
    
    # Ensure temp directory exists
    os.makedirs(settings.TEMP_DIR, exist_ok=True)
    
    # Run profiling tests
    profile_video_conversion()
    profile_r2_upload()
    profile_memory_usage()
    profile_line_by_line()
    profile_concurrent_processing()
    
    print("\nPerformance profiling completed.")

if __name__ == "__main__":
    main()
