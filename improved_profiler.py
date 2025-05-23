#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Improved performance profiling script for the Video Extractor API.

This script profiles the FFmpeg video conversion with different formats and optimization levels.
"""

import os
import time
import tempfile
import subprocess
import shutil
import json
from pathlib import Path
from tabulate import tabulate

# Create a sample video file for testing using subprocess directly
def create_test_video(duration=5, size="640x480", output_path=None):
    """
    Create a test video file using FFmpeg subprocess directly.
    
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
    
    # Create a test video with FFmpeg using subprocess directly
    cmd = [
        'ffmpeg',
        '-y',  # Overwrite output file
        '-f', 'lavfi',
        '-i', f'color=c=red:s={size}:d={duration}',
        '-c:v', 'libx264',
        '-pix_fmt', 'yuv420p',
        output_path
    ]
    
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        print("Test video created successfully.")
        return output_path
    except subprocess.CalledProcessError as e:
        print(f"Error creating test video: {e}")
        print(f"FFmpeg stderr: {e.stderr}")
        raise

# Convert video using subprocess directly
def convert_video(input_file, output_format, preset="medium", crf="23"):
    """
    Convert a video file using FFmpeg subprocess directly.
    
    Args:
        input_file: Path to the input video file
        output_format: Output format (mp4, webm)
        preset: FFmpeg preset (veryfast, medium, slow)
        crf: Constant Rate Factor for quality
        
    Returns:
        Tuple containing (output_file_path, conversion_time, original_size, converted_size, ratio)
    """
    input_path = Path(input_file)
    # Create a unique output filename to avoid in-place editing errors
    output_file = str(input_path.parent / f"{input_path.stem}_{preset}_{crf}.{output_format}")
    
    print(f"Converting {input_file} to {output_format} (preset={preset}, crf={crf})")
    
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
    
    # Output file
    cmd.append(output_file)
    
    try:
        start_time = time.time()
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        conversion_time = time.time() - start_time
        
        # Calculate file sizes
        original_size = os.path.getsize(input_file) / (1024 * 1024)
        converted_size = os.path.getsize(output_file) / (1024 * 1024)
        ratio = converted_size / original_size * 100
        
        print(f"Conversion successful in {conversion_time:.2f} seconds")
        print(f"Original size: {original_size:.2f} MB")
        print(f"Converted size: {converted_size:.2f} MB")
        print(f"Compression ratio: {ratio:.2f}%")
        
        return (output_file, conversion_time, original_size, converted_size, ratio)
    except subprocess.CalledProcessError as e:
        print(f"Error converting video: {e}")
        print(f"FFmpeg stderr: {e.stderr}")
        raise

# Profile different optimization levels
def profile_optimization_levels():
    """
    Profile different optimization levels for video conversion.
    
    Returns:
        Dictionary with profiling results
    """
    print("\n=== Profiling Optimization Levels ===\n")
    results = []
    
    # Create a test video
    try:
        test_video = create_test_video(duration=5, size="1280x720")
        
        # Test different optimization levels for MP4
        optimization_levels = [
            {"name": "fast", "preset": "veryfast", "crf": "28"},
            {"name": "balanced", "preset": "medium", "crf": "23"},
            {"name": "max", "preset": "slow", "crf": "18"},
        ]
        
        for level in optimization_levels:
            print(f"\nTesting {level['name']} optimization level:")
            try:
                output_file, conversion_time, original_size, converted_size, ratio = convert_video(
                    test_video,
                    "mp4",
                    preset=level["preset"],
                    crf=level["crf"]
                )
                
                # Add result to results list
                results.append({
                    "optimization": level["name"],
                    "format": "mp4",
                    "preset": level["preset"],
                    "crf": level["crf"],
                    "conversion_time": conversion_time,
                    "original_size_mb": original_size,
                    "converted_size_mb": converted_size,
                    "compression_ratio": ratio
                })
                
                # Clean up output file
                os.remove(output_file)
            except Exception as e:
                print(f"Error with {level['name']} optimization: {e}")
        
        # Clean up test video
        os.remove(test_video)
    except Exception as e:
        print(f"Error in optimization level profiling: {e}")
    
    return results

# Profile format comparison
def profile_format_comparison():
    """
    Profile different output formats for video conversion.
    
    Returns:
        Dictionary with profiling results
    """
    print("\n=== Profiling Format Comparison ===\n")
    results = []
    
    # Create a test video
    try:
        test_video = create_test_video(duration=5, size="1280x720")
        
        # Test different formats with balanced settings
        formats = ["mp4", "webm"]
        
        for fmt in formats:
            print(f"\nTesting {fmt} format:")
            try:
                output_file, conversion_time, original_size, converted_size, ratio = convert_video(
                    test_video,
                    fmt,
                    preset="medium",
                    crf="23"
                )
                
                # Add result to results list
                results.append({
                    "format": fmt,
                    "preset": "medium",
                    "crf": "23",
                    "conversion_time": conversion_time,
                    "original_size_mb": original_size,
                    "converted_size_mb": converted_size,
                    "compression_ratio": ratio
                })
                
                # Clean up output file
                os.remove(output_file)
            except Exception as e:
                print(f"Error with {fmt} format: {e}")
        
        # Clean up test video
        os.remove(test_video)
    except Exception as e:
        print(f"Error in format comparison profiling: {e}")
    
    return results

# Profile resolution impact
def profile_resolution_impact():
    """
    Profile the impact of different resolutions on conversion performance.
    
    Returns:
        Dictionary with profiling results
    """
    print("\n=== Profiling Resolution Impact ===\n")
    results = []
    
    # Test different resolutions
    resolutions = ["640x480", "1280x720", "1920x1080"]
    
    for resolution in resolutions:
        print(f"\nTesting {resolution} resolution:")
        try:
            # Create a test video with the specific resolution
            test_video = create_test_video(duration=5, size=resolution)
            
            # Convert with balanced settings
            output_file, conversion_time, original_size, converted_size, ratio = convert_video(
                test_video,
                "mp4",
                preset="medium",
                crf="23"
            )
            
            # Add result to results list
            results.append({
                "resolution": resolution,
                "format": "mp4",
                "preset": "medium",
                "crf": "23",
                "conversion_time": conversion_time,
                "original_size_mb": original_size,
                "converted_size_mb": converted_size,
                "compression_ratio": ratio
            })
            
            # Clean up files
            os.remove(output_file)
            os.remove(test_video)
        except Exception as e:
            print(f"Error with {resolution} resolution: {e}")
    
    return results

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
    print("Starting improved performance profiling...\n")
    
    # Check FFmpeg availability
    ffmpeg_path = shutil.which('ffmpeg')
    print(f"FFmpeg available: {ffmpeg_path is not None}")
    if ffmpeg_path:
        print(f"FFmpeg path: {ffmpeg_path}")
    
    # Create temp directory if it doesn't exist
    temp_dir = Path("/tmp/video-extractor")
    temp_dir.mkdir(parents=True, exist_ok=True)
    print(f"Temp directory: {temp_dir}")
    
    # Run profiling tests
    try:
        # Install tabulate if not already installed
        try:
            import tabulate
        except ImportError:
            print("Installing tabulate package...")
            subprocess.run(["pip", "install", "tabulate"], check=True)
            from tabulate import tabulate
        
        # Run profiling tests
        optimization_results = profile_optimization_levels()
        format_results = profile_format_comparison()
        resolution_results = profile_resolution_impact()
        
        # Format results as tables
        if optimization_results:
            print(format_results_table(
                optimization_results,
                "Optimization Levels Comparison",
                ["optimization", "format", "preset", "crf", "conversion_time", "original_size_mb", "converted_size_mb", "compression_ratio"]
            ))
        
        if format_results:
            print(format_results_table(
                format_results,
                "Format Comparison",
                ["format", "preset", "crf", "conversion_time", "original_size_mb", "converted_size_mb", "compression_ratio"]
            ))
        
        if resolution_results:
            print(format_results_table(
                resolution_results,
                "Resolution Impact",
                ["resolution", "format", "conversion_time", "original_size_mb", "converted_size_mb", "compression_ratio"]
            ))
        
        # Save results to JSON file
        all_results = {
            "optimization_levels": optimization_results,
            "formats": format_results,
            "resolutions": resolution_results
        }
        
        results_file = "profiling_results.json"
        with open(results_file, "w") as f:
            json.dump(all_results, f, indent=2)
        
        print(f"\nResults saved to {results_file}")
        print("\nPerformance profiling completed successfully.")
    except Exception as e:
        print(f"\nPerformance profiling failed: {e}")

if __name__ == "__main__":
    main()
