import os
import pytest
import asyncio
from pathlib import Path
from unittest.mock import patch, MagicMock

from app.models.job import ConversionJob, JobStatus
from app.schemas.video import OptimizationLevel


@pytest.fixture
def sample_job(temp_dir):
    """Create a sample conversion job for testing."""
    # Create a test file in the temp directory
    dest_path = os.path.join(temp_dir, "test_input.mp4")
    with open(dest_path, "wb") as f:
        # Create a small dummy file
        f.write(b"test file content")
    
    # Create job
    job = ConversionJob(
        id="test-job-id",
        original_filename="test_input.mp4",
        temp_file_path=dest_path,
        formats=["mp4", "webm"],
        optimize_level=OptimizationLevel.BALANCED,
        preserve_audio=True
    )
    return job


class TestConversionPipeline:
    """Integration tests for the video conversion pipeline."""
    
    @pytest.mark.asyncio
    @patch("app.services.converter.VideoConverter.convert_video")
    @patch("app.services.r2_uploader.r2_uploader.upload_file")
    async def test_process_job_success(self, mock_upload, mock_convert, video_converter, sample_job):
        """Test successful job processing with multiple formats."""
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
        
        # Process job
        await video_converter.process_job(sample_job)
        
        # Verify job status
        assert sample_job.status == JobStatus.COMPLETED
        assert len(sample_job.converted_files) == 2  # mp4 and webm
        
        # Verify uploads happened
        assert mock_upload.call_count == 2
    
    @pytest.mark.asyncio
    @patch("app.services.converter.VideoConverter.convert_video")
    @patch("app.services.r2_uploader.r2_uploader.upload_file")
    async def test_process_job_partial_failure(self, mock_upload, mock_convert, video_converter, sample_job):
        """Test job processing with one format failing."""
        # Mock conversion and create output files
        def mock_convert_side_effect(input_file, output_format, options, preserve_audio):
            output_path = f"{os.path.dirname(input_file)}/output.{output_format}"
            # Create the output file so it exists for the uploader
            with open(output_path, "wb") as f:
                f.write(b"mock converted content")
            return output_path
        
        mock_convert.side_effect = mock_convert_side_effect
        
        # Mock R2 upload - first succeeds, second fails
        mock_upload.side_effect = [
            ("https://example.com/video.mp4", 1.0),  # mp4 succeeds
            Exception("Upload failed")  # webm fails all retries
        ]
        
        # Process job
        await video_converter.process_job(sample_job)
        
        # Verify job status - should be partially completed
        assert sample_job.status == JobStatus.PARTIALLY_COMPLETED
        assert len(sample_job.converted_files) == 1  # Only mp4 succeeded
    
    @pytest.mark.asyncio
    @patch("app.services.converter.VideoConverter.convert_video")
    async def test_process_job_conversion_failure(self, mock_convert, video_converter, sample_job):
        """Test job processing with conversion failure."""
        # Mock conversion - all formats fail
        mock_convert.side_effect = Exception("Conversion failed")
        
        # Process job
        await video_converter.process_job(sample_job)
        
        # Verify job status
        assert sample_job.status == JobStatus.FAILED
        assert "All conversions failed for job" in sample_job.error_message
        assert len(sample_job.converted_files) == 0
    
    @pytest.mark.asyncio
    @patch("app.services.converter.VideoConverter.convert_video")
    @patch("app.services.converter.VideoConverter._get_video_info")
    @patch("app.services.r2_uploader.r2_uploader.upload_file")
    async def test_adaptive_optimizations(self, mock_upload, mock_get_info, mock_convert, video_converter, sample_job):
        """Test that adaptive optimizations are applied based on video info."""
        # Mock video info
        mock_get_info.return_value = {
            "width": 3840,  # 4K video
            "height": 2160,
            "format": "mp4",
            "video_codec": "h264",
            "duration": 60.0,
            "bitrate": 20000000,  # High bitrate
            "video_bitrate": 18000000,
            "has_audio": True
        }
        
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
        
        # Process job
        with patch.object(video_converter, "_apply_adaptive_optimizations", wraps=video_converter._apply_adaptive_optimizations) as mock_adapt:
            await video_converter.process_job(sample_job)
            
            # Verify adaptive optimizations were applied
            assert mock_adapt.called
            
            # Verify job completed successfully
            assert sample_job.status == JobStatus.COMPLETED
    
    @pytest.mark.asyncio
    async def test_optimal_worker_allocation(self, video_converter):
        """Test that optimal worker allocation works correctly for different format combinations."""
        # Test with single format
        single_format = video_converter._calculate_optimal_workers(["mp4"])
        
        # Test with multiple formats
        multi_format = video_converter._calculate_optimal_workers(["mp4", "webm", "mov"])
        
        # Multiple formats should use more workers
        assert multi_format >= single_format
        
        # But should still respect our upper limit from profiling
        assert multi_format <= 8
