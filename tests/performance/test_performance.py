import os
import time
import pytest
import asyncio
from pathlib import Path
import concurrent.futures
from typing import List, Dict
from unittest.mock import patch, MagicMock

from app.models.job import ConversionJob, JobStatus
from app.schemas.video import OptimizationLevel
from app.services.converter import VideoConverter


@pytest.fixture
def sample_video_paths(temp_dir):
    """Create multiple test video files for performance testing."""
    paths = []
    
    # Create 3 test video files
    for i in range(3):
        dest_path = os.path.join(temp_dir, f"test_input_{i}.mp4")
        with open(dest_path, "wb") as f:
            # Create a small dummy file
            f.write(b"test video content")
        paths.append(dest_path)
    
    return paths


@pytest.fixture
def sample_jobs(sample_video_paths):
    """Create sample jobs for performance testing."""
    jobs = []
    
    # Create jobs with different optimization levels and formats
    optimization_levels = [
        OptimizationLevel.FAST,
        OptimizationLevel.BALANCED,
        OptimizationLevel.MAX
    ]
    
    format_combinations = [
        ["mp4"],
        ["webm"],
        ["mp4", "webm"]
    ]
    
    job_id = 0
    for path in sample_video_paths:
        for level in optimization_levels:
            for formats in format_combinations:
                job = ConversionJob(
                    id=f"perf-job-{job_id}",
                    original_filename=os.path.basename(path),
                    temp_file_path=path,
                    formats=formats,
                    optimize_level=level,
                    preserve_audio=True
                )
                jobs.append(job)
                job_id += 1
    
    return jobs


class TestPerformance:
    """Performance tests for video conversion."""
    
    @pytest.mark.asyncio
    @pytest.mark.parametrize("optimize_level", [
        OptimizationLevel.FAST,
        OptimizationLevel.BALANCED,
        OptimizationLevel.MAX
    ])
    @patch("app.services.converter.VideoConverter.convert_video")
    @patch("app.services.r2_uploader.r2_uploader.upload_file")
    async def test_optimization_level_performance(self, mock_upload, mock_convert, optimize_level, video_converter, temp_dir):
        """Test performance of different optimization levels."""
        # Create a job with the specified optimization level
        dest_path = os.path.join(temp_dir, f"test_opt_{optimize_level.value}.mp4")
        with open(dest_path, "wb") as f:
            f.write(b"test video content")
        
        job = ConversionJob(
            id=f"opt-level-{optimize_level.value}",
            original_filename=os.path.basename(dest_path),
            temp_file_path=dest_path,
            formats=["mp4"],  # Use MP4 for consistent comparison
            optimize_level=optimize_level,
            preserve_audio=True
        )
        
        # Mock conversion and create output files
        def mock_convert_side_effect(input_file, output_format, options, preserve_audio):
            output_path = f"{os.path.dirname(input_file)}/output.{output_format}"
            # Create the output file so it exists for the uploader
            with open(output_path, "wb") as f:
                f.write(b"mock converted content")
            return output_path
        
        mock_convert.side_effect = mock_convert_side_effect
        
        # Mock R2 upload
        mock_upload.return_value = ("https://example.com/video.mp4", 1.0)
        
        # Measure conversion time
        start_time = time.time()
        await video_converter.process_job(job)
        end_time = time.time()
        conversion_time = end_time - start_time
        
        # Log performance metrics
        print(f"\nOptimization level {optimize_level.value} took {conversion_time:.2f} seconds")
        print(f"Original size: {job.original_size_mb:.2f}MB")
        
        if job.converted_files:
            for fmt in job.converted_files:
                print(f"Format {fmt}: {job.converted_sizes_mb.get(fmt, 0):.2f}MB ({job.compression_ratios.get(fmt, '0%')})")
        
        # Assert job completed successfully
        assert job.status == JobStatus.COMPLETED
        
        # Store metrics for comparison
        return {
            "level": optimize_level.value,
            "time": conversion_time,
            "original_size": job.original_size_mb,
            "output_size": job.converted_sizes_mb.get("mp4", 0) if job.converted_files else 0,
        }
    
    @pytest.mark.asyncio
    @pytest.mark.parametrize("format_name", ["mp4", "webm"])
    @patch("app.services.converter.VideoConverter.convert_video")
    @patch("app.services.r2_uploader.r2_uploader.upload_file")
    async def test_format_performance(self, mock_upload, mock_convert, format_name, video_converter, temp_dir):
        """Test performance of different output formats."""
        # Create a job with the specified format
        dest_path = os.path.join(temp_dir, f"test_format_{format_name}.mp4")
        with open(dest_path, "wb") as f:
            f.write(b"test video content")
        
        job = ConversionJob(
            id=f"format-{format_name}",
            original_filename=os.path.basename(dest_path),
            temp_file_path=dest_path,
            formats=[format_name],
            optimize_level=OptimizationLevel.BALANCED,  # Use balanced for consistent comparison
            preserve_audio=True
        )
        
        # Mock conversion and create output files
        def mock_convert_side_effect(input_file, output_format, options, preserve_audio):
            output_path = f"{os.path.dirname(input_file)}/output.{output_format}"
            # Create the output file so it exists for the uploader
            with open(output_path, "wb") as f:
                f.write(b"mock converted content")
            return output_path
        
        mock_convert.side_effect = mock_convert_side_effect
        
        # Mock R2 upload
        mock_upload.return_value = ("https://example.com/video.mp4", 1.0)
        
        # Measure conversion time
        start_time = time.time()
        await video_converter.process_job(job)
        end_time = time.time()
        conversion_time = end_time - start_time
        
        # Log performance metrics
        print(f"\nFormat {format_name} took {conversion_time:.2f} seconds")
        
        # Assert job completed successfully
        assert job.status == JobStatus.COMPLETED
        
        # Store metrics for comparison
        return {
            "format": format_name,
            "time": conversion_time,
            "original_size": job.original_size_mb,
            "output_size": job.converted_sizes_mb.get(format_name, 0) if job.converted_files else 0,
        }
    
    @pytest.mark.asyncio
    @pytest.mark.parametrize("thread_count", [1, 2, 4, 8])
    @patch("app.services.converter.VideoConverter.convert_video")
    @patch("app.services.r2_uploader.r2_uploader.upload_file")
    async def test_thread_count_performance(self, mock_upload, mock_convert, thread_count, temp_dir):
        """Test performance with different thread counts."""
        # Create a custom converter with specified thread count
        converter = VideoConverter(max_workers=thread_count)
        
        # Create a job
        dest_path = os.path.join(temp_dir, f"test_threads_{thread_count}.mp4")
        with open(dest_path, "wb") as f:
            f.write(b"test video content")
        
        job = ConversionJob(
            id=f"threads-{thread_count}",
            original_filename=os.path.basename(dest_path),
            temp_file_path=dest_path,
            formats=["mp4", "webm"],  # Test with multiple formats
            optimize_level=OptimizationLevel.BALANCED,
            preserve_audio=True
        )
        
        # Mock conversion and create output files
        def mock_convert_side_effect(input_file, output_format, options, preserve_audio):
            output_path = f"{os.path.dirname(input_file)}/output.{output_format}"
            # Create the output file so it exists for the uploader
            with open(output_path, "wb") as f:
                f.write(b"mock converted content")
            return output_path
        
        mock_convert.side_effect = mock_convert_side_effect
        
        # Mock R2 upload
        mock_upload.return_value = ("https://example.com/video.mp4", 1.0)
        
        # Measure conversion time
        start_time = time.time()
        await converter.process_job(job)
        end_time = time.time()
        conversion_time = end_time - start_time
        
        # Log performance metrics
        print(f"\n{thread_count} threads took {conversion_time:.2f} seconds for multiple formats")
        
        # Assert job completed successfully
        assert job.status == JobStatus.COMPLETED
        
        # Store metrics for comparison
        return {
            "threads": thread_count,
            "time": conversion_time,
            "formats": len(job.formats),
        }
    
    @pytest.mark.asyncio
    @patch("app.services.converter.VideoConverter.convert_video")
    @patch("app.services.r2_uploader.r2_uploader.upload_file")
    async def test_concurrent_jobs_performance(self, mock_upload, mock_convert, video_converter, temp_dir):
        """Test performance with multiple concurrent jobs."""
        # Create test jobs with valid file paths
        test_jobs = []
        for i in range(6):  # Create 6 jobs for testing
            # Create a test file in the temp directory
            dest_path = os.path.join(temp_dir, f"test_concurrent_{i}.mp4")
            with open(dest_path, "wb") as f:
                f.write(b"test video content")
            
            # Create job
            job = ConversionJob(
                id=f"concurrent-job-{i}",
                original_filename=f"test_concurrent_{i}.mp4",
                temp_file_path=dest_path,
                formats=["mp4"],
                optimize_level=OptimizationLevel.BALANCED,
                preserve_audio=True
            )
            test_jobs.append(job)
        
        # Mock conversion and create output files
        def mock_convert_side_effect(input_file, output_format, options, preserve_audio):
            output_path = f"{os.path.dirname(input_file)}/output.{output_format}"
            # Create the output file so it exists for the uploader
            with open(output_path, "wb") as f:
                f.write(b"mock converted content")
            return output_path
        
        mock_convert.side_effect = mock_convert_side_effect
        
        # Mock R2 upload
        mock_upload.return_value = ("https://example.com/video.mp4", 1.0)
        
        # Process jobs concurrently
        start_time = time.time()
        await asyncio.gather(*[video_converter.process_job(job) for job in test_jobs])
        end_time = time.time()
        
        total_time = end_time - start_time
        
        # Log performance metrics
        print(f"\nProcessed {len(test_jobs)} concurrent jobs in {total_time:.2f} seconds")
        print(f"Average time per job: {total_time / len(test_jobs):.2f} seconds")
        
        # Verify all jobs completed
        completed_jobs = sum(1 for job in test_jobs if job.status == JobStatus.COMPLETED)
        print(f"Successfully completed {completed_jobs}/{len(test_jobs)} jobs")
        
        # Assert most jobs completed successfully
        assert completed_jobs >= len(test_jobs) * 0.8  # At least 80% success rate
        
        return {
            "total_jobs": len(test_jobs),
            "completed_jobs": completed_jobs,
            "total_time": total_time,
            "avg_time_per_job": total_time / len(test_jobs),
        }
