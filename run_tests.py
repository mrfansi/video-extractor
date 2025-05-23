#!/usr/bin/env python
"""
Comprehensive test runner for Video Extractor API optimizations.

This script runs all tests (unit, integration, and performance) and generates
a detailed report of the results, including performance metrics and optimization
effectiveness.
"""

import os
import sys
import time
import json
import argparse
import subprocess
from datetime import datetime
from pathlib import Path


def run_command(command, cwd=None):
    """Run a command and return its output."""
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
        cwd=cwd
    )
    stdout, stderr = process.communicate()
    return {
        "stdout": stdout.decode("utf-8"),
        "stderr": stderr.decode("utf-8"),
        "returncode": process.returncode
    }


def create_test_video():
    """Create a sample test video if it doesn't exist."""
    test_video_path = Path("tests/data/test_video.mp4")
    
    if test_video_path.exists():
        print(f"Test video already exists at {test_video_path}")
        return str(test_video_path)
    
    print("Creating sample test video...")
    
    # Check if ffmpeg is installed
    result = run_command("which ffmpeg")
    if result["returncode"] != 0:
        print("Error: ffmpeg is not installed. Please install it to create test videos.")
        print("You can download a sample video manually and place it at tests/data/test_video.mp4")
        return None
    
    # Create a 10-second test video
    cmd = (
        "ffmpeg -f lavfi -i testsrc=duration=10:size=1280x720:rate=30 "
        "-f lavfi -i sine=frequency=440:duration=10 "
        f"-c:v libx264 -c:a aac {test_video_path}"
    )
    
    result = run_command(cmd)
    if result["returncode"] != 0:
        print(f"Error creating test video: {result['stderr']}")
        return None
    
    print(f"Created test video at {test_video_path}")
    return str(test_video_path)


def run_tests(test_type=None, verbose=False):
    """Run tests and return results."""
    # Create test video if needed
    test_video = create_test_video()
    if not test_video and test_type != "unit":
        print("Warning: No test video available. Some tests may be skipped.")
    
    # Prepare pytest command
    pytest_cmd = "python -m pytest"
    if verbose:
        pytest_cmd += " -v"
    
    # Add test type filter
    if test_type == "unit":
        pytest_cmd += " tests/unit"
    elif test_type == "integration":
        pytest_cmd += " tests/integration"
    elif test_type == "performance":
        pytest_cmd += " tests/performance -v"  # Always verbose for performance tests
    
    # Add coverage reporting
    pytest_cmd += " --cov=app --cov-report=term --cov-report=html:reports/coverage"
    
    # Run tests
    print(f"Running {test_type or 'all'} tests...")
    start_time = time.time()
    result = run_command(pytest_cmd)
    end_time = time.time()
    
    # Process results
    success = result["returncode"] == 0
    duration = end_time - start_time
    
    return {
        "success": success,
        "duration": duration,
        "output": result["stdout"],
        "error": result["stderr"],
        "command": pytest_cmd
    }


def generate_report(results):
    """Generate a comprehensive test report."""
    # Create reports directory
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    
    # Generate timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_file = reports_dir / f"test_report_{timestamp}.md"
    
    # Extract performance metrics from test output
    performance_metrics = extract_performance_metrics(results.get("performance", {}).get("output", ""))
    
    # Write report
    with open(report_file, "w") as f:
        f.write(f"# Video Extractor API Test Report\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Overall summary
        f.write("## Summary\n\n")
        all_success = all(r.get("success", False) for r in results.values() if r)
        f.write(f"Overall Status: {'✅ PASSED' if all_success else '❌ FAILED'}\n\n")
        
        total_duration = sum(r.get("duration", 0) for r in results.values() if r)
        f.write(f"Total Duration: {total_duration:.2f} seconds\n\n")
        
        # Test results by type
        f.write("### Results by Test Type\n\n")
        f.write("| Test Type | Status | Duration (s) |\n")
        f.write("|-----------|--------|--------------|\n")
        
        for test_type, result in results.items():
            if result:
                status = "✅ PASSED" if result["success"] else "❌ FAILED"
                f.write(f"| {test_type.capitalize()} | {status} | {result['duration']:.2f} |\n")
        
        # Performance metrics
        if performance_metrics:
            f.write("\n## Performance Metrics\n\n")
            
            # Optimization level comparison
            if "optimization_levels" in performance_metrics:
                f.write("### Optimization Level Performance\n\n")
                f.write("| Level | Time (s) | Output Size (MB) | Compression Ratio (%) |\n")
                f.write("|-------|----------|-----------------|----------------------|\n")
                
                for level in performance_metrics["optimization_levels"]:
                    f.write(f"| {level['level']} | {level['time']:.2f} | {level['output_size']:.2f} | "
                            f"{level['output_size'] / level['original_size'] * 100:.2f} |\n")
            
            # Format comparison
            if "formats" in performance_metrics:
                f.write("\n### Format Performance\n\n")
                f.write("| Format | Time (s) | Output Size (MB) | Compression Ratio (%) |\n")
                f.write("|--------|----------|-----------------|----------------------|\n")
                
                for fmt in performance_metrics["formats"]:
                    f.write(f"| {fmt['format']} | {fmt['time']:.2f} | {fmt['output_size']:.2f} | "
                            f"{fmt['output_size'] / fmt['original_size'] * 100:.2f} |\n")
            
            # Thread count comparison
            if "thread_counts" in performance_metrics:
                f.write("\n### Thread Count Performance\n\n")
                f.write("| Threads | Time (s) | Formats |\n")
                f.write("|---------|----------|---------|\n")
                
                for thread in performance_metrics["thread_counts"]:
                    f.write(f"| {thread['threads']} | {thread['time']:.2f} | {thread['formats']} |\n")
            
            # Concurrent jobs
            if "concurrent_jobs" in performance_metrics:
                f.write("\n### Concurrent Jobs Performance\n\n")
                jobs = performance_metrics["concurrent_jobs"]
                f.write(f"Total Jobs: {jobs['total_jobs']}\n\n")
                f.write(f"Completed Jobs: {jobs['completed_jobs']}\n\n")
                f.write(f"Total Time: {jobs['total_time']:.2f} seconds\n\n")
                f.write(f"Average Time Per Job: {jobs['avg_time_per_job']:.2f} seconds\n\n")
        
        # Detailed test output
        f.write("\n## Detailed Test Output\n\n")
        
        for test_type, result in results.items():
            if result:
                f.write(f"### {test_type.capitalize()} Tests\n\n")
                f.write(f"Command: `{result['command']}`\n\n")
                f.write("```\n")
                f.write(result["output"])
                if result["error"]:
                    f.write("\nErrors:\n")
                    f.write(result["error"])
                f.write("```\n\n")
    
    print(f"Report generated: {report_file}")
    return str(report_file)


def extract_performance_metrics(output):
    """Extract performance metrics from test output."""
    metrics = {}
    
    # Extract optimization level metrics
    opt_levels = []
    for level in ["fast", "balanced", "max"]:
        # Find lines like "Optimization level fast took 0.75 seconds"
        level_time = None
        for line in output.split("\n"):
            if f"Optimization level {level} took" in line:
                try:
                    level_time = float(line.split("took")[1].split("seconds")[0].strip())
                except (IndexError, ValueError):
                    pass
        
        # Find size information
        original_size = 0
        output_size = 0
        for i, line in enumerate(output.split("\n")):
            if f"Original size:" in line and i > 0:
                try:
                    original_size = float(line.split(":")[1].split("MB")[0].strip())
                    # Next line should have format information
                    if i + 1 < len(output.split("\n")):
                        next_line = output.split("\n")[i + 1]
                        if "Format mp4:" in next_line:
                            output_size = float(next_line.split(":")[1].split("MB")[0].strip())
                except (IndexError, ValueError):
                    pass
        
        if level_time is not None:
            opt_levels.append({
                "level": level,
                "time": level_time,
                "original_size": original_size,
                "output_size": output_size
            })
    
    if opt_levels:
        metrics["optimization_levels"] = opt_levels
    
    # Extract format metrics
    formats = []
    for fmt in ["mp4", "webm"]:
        # Find lines like "Format mp4 took 0.75 seconds"
        fmt_time = None
        for line in output.split("\n"):
            if f"Format {fmt} took" in line:
                try:
                    fmt_time = float(line.split("took")[1].split("seconds")[0].strip())
                except (IndexError, ValueError):
                    pass
        
        if fmt_time is not None:
            formats.append({
                "format": fmt,
                "time": fmt_time,
                "original_size": original_size,  # Reuse from above
                "output_size": output_size      # Reuse from above
            })
    
    if formats:
        metrics["formats"] = formats
    
    # Extract thread count metrics
    thread_counts = []
    for threads in [1, 2, 4, 8]:
        # Find lines like "4 threads took 1.23 seconds for multiple formats"
        thread_time = None
        for line in output.split("\n"):
            if f"{threads} threads took" in line and "for multiple formats" in line:
                try:
                    thread_time = float(line.split("took")[1].split("seconds")[0].strip())
                except (IndexError, ValueError):
                    pass
        
        if thread_time is not None:
            thread_counts.append({
                "threads": threads,
                "time": thread_time,
                "formats": 2  # Hardcoded from test
            })
    
    if thread_counts:
        metrics["thread_counts"] = thread_counts
    
    # Extract concurrent jobs metrics
    for line in output.split("\n"):
        if "Processed" in line and "concurrent jobs in" in line:
            try:
                total_jobs = int(line.split("Processed")[1].split("concurrent")[0].strip())
                total_time = float(line.split("in")[1].split("seconds")[0].strip())
                
                # Find "Successfully completed X/Y jobs"
                for next_line in output.split("\n"):
                    if "Successfully completed" in next_line and "/" in next_line and "jobs" in next_line:
                        completed_jobs = int(next_line.split("completed")[1].split("/")[0].strip())
                        metrics["concurrent_jobs"] = {
                            "total_jobs": total_jobs,
                            "completed_jobs": completed_jobs,
                            "total_time": total_time,
                            "avg_time_per_job": total_time / total_jobs
                        }
                        break
            except (IndexError, ValueError):
                pass
    
    return metrics


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Run tests for Video Extractor API")
    parser.add_argument(
        "--type", 
        choices=["unit", "integration", "performance", "all"],
        default="all",
        help="Type of tests to run"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    args = parser.parse_args()
    
    # Create reports directory
    Path("reports").mkdir(exist_ok=True)
    
    # Run tests
    results = {}
    if args.type == "all":
        results["unit"] = run_tests("unit", args.verbose)
        results["integration"] = run_tests("integration", args.verbose)
        results["performance"] = run_tests("performance", args.verbose)
    else:
        results[args.type] = run_tests(args.type, args.verbose)
    
    # Generate report
    report_file = generate_report(results)
    
    # Print summary
    print("\nTest Summary:")
    all_success = all(r.get("success", False) for r in results.values() if r)
    print(f"Overall Status: {'PASSED' if all_success else 'FAILED'}")
    
    total_duration = sum(r.get("duration", 0) for r in results.values() if r)
    print(f"Total Duration: {total_duration:.2f} seconds")
    
    for test_type, result in results.items():
        if result:
            status = "PASSED" if result["success"] else "FAILED"
            print(f"{test_type.capitalize()} Tests: {status} ({result['duration']:.2f}s)")
    
    print(f"\nDetailed report: {report_file}")
    
    # Return exit code based on test results
    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())
