#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Comprehensive performance profiling script for the Video Extractor API.

This script provides detailed profiling of video conversion operations with realistic test data,
including CPU usage, memory consumption, and detailed FFmpeg metrics.
"""

import os
import time
import tempfile
import subprocess
import shutil
import json
import psutil
import cProfile
import pstats
import io
import urllib.request
from pathlib import Path
from tabulate import tabulate
from concurrent.futures import ThreadPoolExecutor

# Constants
SAMPLE_VIDEO_URL = "https://sample-videos.com/video123/mp4/720/big_buck_bunny_720p_1mb.mp4"
TEST_VIDEO_PATH = "/tmp/video-extractor/sample_video.mp4"
RESULTS_DIR = "/tmp/video-extractor/results"

# Ensure directories exist
def ensure_dirs():
    """
    Ensure that necessary directories exist.
    """
    os.makedirs("/tmp/video-extractor", exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)

# Download sample video if needed
def download_sample_video():
    """
    Download a sample video for testing if it doesn't already exist.
    
    Returns:
        Path to the sample video
    """
    if not os.path.exists(TEST_VIDEO_PATH):
        print(f"Downloading sample video from {SAMPLE_VIDEO_URL}...")
        try:
            urllib.request.urlretrieve(SAMPLE_VIDEO_URL, TEST_VIDEO_PATH)
            print(f"Sample video downloaded to {TEST_VIDEO_PATH}")
        except Exception as e:
            print(f"Error downloading sample video: {e}")
            return None
    else:
        print(f"Using existing sample video: {TEST_VIDEO_PATH}")
    
    return TEST_VIDEO_PATH

# Get video information using FFprobe
def get_video_info(video_path):
    """
    Get video information using FFprobe.
    
    Args:
        video_path: Path to the video file
        
    Returns:
        Dictionary with video information
    """
    cmd = [
        'ffprobe',
        '-v', 'quiet',
        '-print_format', 'json',
        '-show_format',
        '-show_streams',
        video_path
    ]
    
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error getting video info: {e}")
        print(f"FFprobe stderr: {e.stderr}")
        return None

# Convert video with detailed metrics
def convert_video_with_metrics(input_file, output_format, preset="medium", crf="23", threads=None):
    """
    Convert a video file using FFmpeg with detailed metrics collection.
    
    Args:
        input_file: Path to the input video file
        output_format: Output format (mp4, webm)
        preset: FFmpeg preset (veryfast, medium, slow)
        crf: Constant Rate Factor for quality
        threads: Number of threads to use (None for default)
        
    Returns:
        Dictionary with conversion metrics
    """
    input_path = Path(input_file)
    output_file = os.path.join(RESULTS_DIR, f"{input_path.stem}_{preset}_{crf}.{output_format}")
    
    print(f"Converting {input_file} to {output_format} (preset={preset}, crf={crf}, threads={threads})")
    
    # Base command
    cmd = ['ffmpeg', '-y', '-i', input_file]
    
    # Format-specific settings
    if output_format == 'mp4':
        cmd.extend([
            '-c:v', 'libx264',
            '-preset', preset,
            '-crf', crf,
            '-pix_fmt', 'yuv420p',
        ])
    elif output_format == 'webm':
        cmd.extend([
            '-c:v', 'libvpx-vp9',
            '-crf', crf,
            '-b:v', '0',
            '-pix_fmt', 'yuv420p',
        ])
    
    # Add thread count if specified
    if threads is not None:
        cmd.extend(['-threads', str(threads)])
    
    # Add detailed FFmpeg logging
    cmd.extend([
        '-benchmark',
        '-stats',
    ])
    
    # Output file
    cmd.append(output_file)
    
    # Start process monitoring
    process = psutil.Process(os.getpid())
    start_cpu_times = process.cpu_times()
    start_memory = process.memory_info().rss / (1024 * 1024)  # MB
    start_time = time.time()
    
    try:
        # Run FFmpeg with profiling
        pr = cProfile.Profile()
        pr.enable()
        
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        
        pr.disable()
        
        # Calculate metrics
        end_time = time.time()
        end_cpu_times = process.cpu_times()
        end_memory = process.memory_info().rss / (1024 * 1024)  # MB
        
        conversion_time = end_time - start_time
        cpu_user_time = end_cpu_times.user - start_cpu_times.user
        cpu_system_time = end_cpu_times.system - start_cpu_times.system
        memory_used = end_memory - start_memory
        
        # Calculate file sizes
        original_size = os.path.getsize(input_file) / (1024 * 1024)  # MB
        converted_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
        compression_ratio = converted_size / original_size * 100
        
        # Extract FFmpeg stats from stderr
        ffmpeg_stats = result.stderr
        
        # Get profiling stats
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
        ps.print_stats(20)  # Top 20 functions
        profiling_stats = s.getvalue()
        
        print(f"Conversion successful in {conversion_time:.2f} seconds")
        print(f"CPU user time: {cpu_user_time:.2f} seconds")
        print(f"CPU system time: {cpu_system_time:.2f} seconds")
        print(f"Memory used: {memory_used:.2f} MB")
        print(f"Original size: {original_size:.2f} MB")
        print(f"Converted size: {converted_size:.2f} MB")
        print(f"Compression ratio: {compression_ratio:.2f}%")
        
        # Save detailed stats to file
        stats_file = f"{output_file}.stats.txt"
        with open(stats_file, 'w') as f:
            f.write(f"FFmpeg Command: {' '.join(cmd)}\n\n")
            f.write(f"Conversion Time: {conversion_time:.2f} seconds\n")
            f.write(f"CPU User Time: {cpu_user_time:.2f} seconds\n")
            f.write(f"CPU System Time: {cpu_system_time:.2f} seconds\n")
            f.write(f"Memory Used: {memory_used:.2f} MB\n")
            f.write(f"Original Size: {original_size:.2f} MB\n")
            f.write(f"Converted Size: {converted_size:.2f} MB\n")
            f.write(f"Compression Ratio: {compression_ratio:.2f}%\n\n")
            f.write("FFmpeg Output:\n")
            f.write(ffmpeg_stats)
            f.write("\n\nProfiling Stats:\n")
            f.write(profiling_stats)
        
        return {
            "output_file": output_file,
            "conversion_time": conversion_time,
            "cpu_user_time": cpu_user_time,
            "cpu_system_time": cpu_system_time,
            "memory_used": memory_used,
            "original_size_mb": original_size,
            "converted_size_mb": converted_size,
            "compression_ratio": compression_ratio,
            "stats_file": stats_file
        }
    except subprocess.CalledProcessError as e:
        print(f"Error converting video: {e}")
        print(f"FFmpeg stderr: {e.stderr}")
        return None

# Profile different optimization levels
def profile_optimization_levels(input_file):
    """
    Profile different optimization levels for video conversion.
    
    Args:
        input_file: Path to the input video file
        
    Returns:
        List of result dictionaries
    """
    print("\n=== Profiling Optimization Levels ===\n")
    results = []
    
    # Test different optimization levels for MP4
    optimization_levels = [
        {"name": "fast", "preset": "veryfast", "crf": "28"},
        {"name": "balanced", "preset": "medium", "crf": "23"},
        {"name": "max", "preset": "slow", "crf": "18"},
    ]
    
    for level in optimization_levels:
        print(f"\nTesting {level['name']} optimization level:")
        result = convert_video_with_metrics(
            input_file,
            "mp4",
            preset=level["preset"],
            crf=level["crf"]
        )
        
        if result:
            result["optimization"] = level["name"]
            result["format"] = "mp4"
            result["preset"] = level["preset"]
            result["crf"] = level["crf"]
            results.append(result)
    
    return results

# Profile format comparison
def profile_format_comparison(input_file):
    """
    Profile different output formats for video conversion.
    
    Args:
        input_file: Path to the input video file
        
    Returns:
        List of result dictionaries
    """
    print("\n=== Profiling Format Comparison ===\n")
    results = []
    
    # Test different formats with balanced settings
    formats = ["mp4", "webm"]
    
    for fmt in formats:
        print(f"\nTesting {fmt} format:")
        result = convert_video_with_metrics(
            input_file,
            fmt,
            preset="medium",
            crf="23"
        )
        
        if result:
            result["format"] = fmt
            result["preset"] = "medium"
            result["crf"] = "23"
            results.append(result)
    
    return results

# Profile thread count impact
def profile_thread_count_impact(input_file):
    """
    Profile the impact of different thread counts on conversion performance.
    
    Args:
        input_file: Path to the input video file
        
    Returns:
        List of result dictionaries
    """
    print("\n=== Profiling Thread Count Impact ===\n")
    results = []
    
    # Get CPU count
    cpu_count = psutil.cpu_count(logical=True)
    thread_counts = [1, max(2, cpu_count // 2), cpu_count]
    
    for threads in thread_counts:
        print(f"\nTesting with {threads} threads:")
        result = convert_video_with_metrics(
            input_file,
            "mp4",
            preset="medium",
            crf="23",
            threads=threads
        )
        
        if result:
            result["threads"] = threads
            result["format"] = "mp4"
            result["preset"] = "medium"
            result["crf"] = "23"
            results.append(result)
    
    return results

# Profile concurrent conversions
def profile_concurrent_conversions(input_file, max_workers=None):
    """
    Profile concurrent video conversions.
    
    Args:
        input_file: Path to the input video file
        max_workers: Maximum number of concurrent workers
        
    Returns:
        Dictionary with profiling results
    """
    print("\n=== Profiling Concurrent Conversions ===\n")
    
    if max_workers is None:
        max_workers = psutil.cpu_count(logical=True)
    
    print(f"Testing with {max_workers} concurrent workers")
    
    # Define conversion tasks
    tasks = [
        {"format": "mp4", "preset": "veryfast", "crf": "28"},
        {"format": "mp4", "preset": "medium", "crf": "23"},
        {"format": "webm", "preset": "medium", "crf": "23"},
    ]
    
    # Start monitoring
    process = psutil.Process(os.getpid())
    start_cpu_times = process.cpu_times()
    start_memory = process.memory_info().rss / (1024 * 1024)  # MB
    start_time = time.time()
    
    # Run concurrent conversions
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for task in tasks:
            future = executor.submit(
                convert_video_with_metrics,
                input_file,
                task["format"],
                task["preset"],
                task["crf"]
            )
            futures.append((future, task))
        
        for future, task in futures:
            try:
                result = future.result()
                if result:
                    result.update(task)
                    results.append(result)
            except Exception as e:
                print(f"Error in concurrent conversion: {e}")
    
    # Calculate metrics
    end_time = time.time()
    end_cpu_times = process.cpu_times()
    end_memory = process.memory_info().rss / (1024 * 1024)  # MB
    
    total_time = end_time - start_time
    cpu_user_time = end_cpu_times.user - start_cpu_times.user
    cpu_system_time = end_cpu_times.system - start_cpu_times.system
    memory_used = end_memory - start_memory
    
    print(f"Concurrent conversions completed in {total_time:.2f} seconds")
    print(f"CPU user time: {cpu_user_time:.2f} seconds")
    print(f"CPU system time: {cpu_system_time:.2f} seconds")
    print(f"Memory used: {memory_used:.2f} MB")
    
    return {
        "total_time": total_time,
        "cpu_user_time": cpu_user_time,
        "cpu_system_time": cpu_system_time,
        "memory_used": memory_used,
        "max_workers": max_workers,
        "task_results": results
    }

# Format results as a table
def format_results_table(results, title, columns):
    """
    Format results as a table.
    
    Args:
        results: List of result dictionaries
        title: Table title
        columns: List of column names to include
        
    Returns:
        Formatted table string
    """
    table_data = []
    for result in results:
        row = [result.get(col, "") for col in columns]
        # Format numeric values
        for i, val in enumerate(row):
            if isinstance(val, float):
                row[i] = f"{val:.2f}"
        table_data.append(row)
    
    return f"\n{title}\n{tabulate(table_data, headers=columns, tablefmt='grid')}\n"

# Main function
def main():
    """
    Main function to run profiling tests.
    """
    print("Starting comprehensive performance profiling...\n")
    
    # Check FFmpeg availability
    ffmpeg_path = shutil.which('ffmpeg')
    print(f"FFmpeg available: {ffmpeg_path is not None}")
    if ffmpeg_path:
        print(f"FFmpeg path: {ffmpeg_path}")
    
    # Ensure directories exist
    ensure_dirs()
    
    # Install required packages if not already installed
    try:
        import psutil
    except ImportError:
        print("Installing psutil package...")
        subprocess.run(["pip", "install", "psutil"], check=True)
        import psutil
    
    try:
        import tabulate
    except ImportError:
        print("Installing tabulate package...")
        subprocess.run(["pip", "install", "tabulate"], check=True)
        from tabulate import tabulate
    
    # Download sample video
    sample_video = download_sample_video()
    if not sample_video:
        print("Failed to get sample video. Exiting.")
        return
    
    # Get video info
    video_info = get_video_info(sample_video)
    if video_info:
        print("\nSample Video Information:")
        print(f"Format: {video_info.get('format', {}).get('format_name', 'unknown')}")
        print(f"Duration: {float(video_info.get('format', {}).get('duration', 0)):.2f} seconds")
        print(f"Size: {float(video_info.get('format', {}).get('size', 0)) / (1024 * 1024):.2f} MB")
        
        for stream in video_info.get('streams', []):
            if stream.get('codec_type') == 'video':
                print(f"Video codec: {stream.get('codec_name', 'unknown')}")
                print(f"Resolution: {stream.get('width', 0)}x{stream.get('height', 0)}")
                print(f"Bitrate: {int(stream.get('bit_rate', 0)) / 1000:.2f} kbps")
    
    # Run profiling tests
    try:
        # Run profiling tests
        optimization_results = profile_optimization_levels(sample_video)
        format_results = profile_format_comparison(sample_video)
        thread_results = profile_thread_count_impact(sample_video)
        concurrent_results = profile_concurrent_conversions(sample_video)
        
        # Format results as tables
        if optimization_results:
            print(format_results_table(
                optimization_results,
                "Optimization Levels Comparison",
                ["optimization", "format", "preset", "crf", "conversion_time", "cpu_user_time", "cpu_system_time", "memory_used", "original_size_mb", "converted_size_mb", "compression_ratio"]
            ))
        
        if format_results:
            print(format_results_table(
                format_results,
                "Format Comparison",
                ["format", "preset", "crf", "conversion_time", "cpu_user_time", "cpu_system_time", "memory_used", "original_size_mb", "converted_size_mb", "compression_ratio"]
            ))
        
        if thread_results:
            print(format_results_table(
                thread_results,
                "Thread Count Impact",
                ["threads", "format", "conversion_time", "cpu_user_time", "cpu_system_time", "memory_used", "compression_ratio"]
            ))
        
        if concurrent_results and concurrent_results.get('task_results'):
            print("\nConcurrent Conversions Summary:")
            print(f"Total time: {concurrent_results['total_time']:.2f} seconds")
            print(f"CPU user time: {concurrent_results['cpu_user_time']:.2f} seconds")
            print(f"CPU system time: {concurrent_results['cpu_system_time']:.2f} seconds")
            print(f"Memory used: {concurrent_results['memory_used']:.2f} MB")
            print(f"Max workers: {concurrent_results['max_workers']}")
            
            print(format_results_table(
                concurrent_results['task_results'],
                "Individual Task Results",
                ["format", "preset", "crf", "conversion_time", "cpu_user_time", "memory_used", "compression_ratio"]
            ))
        
        # Save results to JSON file
        all_results = {
            "optimization_levels": optimization_results,
            "formats": format_results,
            "thread_counts": thread_results,
            "concurrent_conversions": concurrent_results
        }
        
        results_file = "profiling_results_comprehensive.json"
        with open(results_file, "w") as f:
            # Convert to serializable format
            serializable_results = json.dumps(all_results, default=lambda o: str(o) if not isinstance(o, (dict, list, str, int, float, bool, type(None))) else o, indent=2)
            f.write(serializable_results)
        
        print(f"\nResults saved to {results_file}")
        print("\nPerformance profiling completed successfully.")
    except Exception as e:
        print(f"\nPerformance profiling failed: {e}")

if __name__ == "__main__":
    main()
