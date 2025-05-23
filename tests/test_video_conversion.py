import os
import tempfile
import time
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import BackgroundTasks
from fastapi.testclient import TestClient

from app.core.config import settings
from app.main import app
from app.models.job import ConversionJob, JobStatus, JOBS, create_job
from app.schemas.video import OptimizationLevel


@pytest.fixture
def test_client():
    """Test client fixture."""
    return TestClient(app)


@pytest.fixture
def mock_background_tasks():
    """Mock background tasks fixture."""
    return MagicMock(spec=BackgroundTasks)


@pytest.fixture
def sample_video_file():
    """Sample video file fixture."""
    # Create a temporary file that simulates a video file
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
        tmp.write(b"dummy video content")
        return tmp.name


@pytest.fixture(autouse=True)
def clear_jobs():
    """Clear jobs before and after each test."""
    JOBS.clear()
    yield
    JOBS.clear()


def test_health_endpoint(test_client):
    """Test health check endpoint."""
    response = test_client.get("/api/health")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "ok"
    assert "video-extractor API is healthy" in data["message"]


def test_metrics_endpoint(test_client):
    """Test metrics endpoint."""
    response = test_client.get("/api/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]


@patch("app.api.video.video_converter")
def test_start_conversion_endpoint(mock_converter, test_client, mock_background_tasks, sample_video_file):
    """Test start conversion endpoint."""
    # Mock the save_upload_file method
    mock_converter.save_upload_file.return_value = (sample_video_file, "test.mp4")
    
    # Replace the background_tasks dependency with our mock
    app.dependency_overrides[BackgroundTasks] = lambda: mock_background_tasks
    
    # Create a test file to upload
    with open(sample_video_file, "rb") as f:
        response = test_client.post(
            "/api/convert",
            files={"file": ("test.mp4", f, "video/mp4")},
            data={
                "formats": "mp4",
                "preserve_audio": "true",
                "optimize_level": "balanced",
            },
        )
    
    # Check response
    assert response.status_code == 202
    
    data = response.json()
    assert data["status"] == "processing"
    assert "request_id" in data
    assert "Conversion started" in data["message"]
    
    # Verify background task was added
    mock_background_tasks.add_task.assert_called_once()
    
    # Clean up the dependency override
    app.dependency_overrides.clear()


def test_get_conversion_status_processing(test_client):
    """Test get conversion status endpoint with processing job."""
    # Create a job
    job_id = str(uuid.uuid4())
    job = ConversionJob(
        id=job_id,
        original_filename="test.mp4",
        temp_file_path="/tmp/test.mp4",
        formats=["mp4"],
        preserve_audio=True,
        optimize_level=OptimizationLevel.BALANCED,
        status=JobStatus.PROCESSING,
    )
    create_job(job)
    
    # Test endpoint
    response = test_client.get(f"/api/convert/{job_id}")
    
    # Check response
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "processing"
    assert "being processed" in data["message"]


def test_get_conversion_status_completed(test_client):
    """Test get conversion status endpoint with completed job."""
    # Create a job
    job_id = str(uuid.uuid4())
    job = ConversionJob(
        id=job_id,
        original_filename="test.mp4",
        temp_file_path="/tmp/test.mp4",
        formats=["mp4"],
        preserve_audio=True,
        optimize_level=OptimizationLevel.BALANCED,
        status=JobStatus.COMPLETED,
        original_size_mb=10.0,
    )
    
    # Add conversion result
    job.add_converted_file("mp4", "https://example.com/video.mp4", 5.0)
    
    # Update job status
    job.update_status(JobStatus.COMPLETED)
    
    # Store job
    create_job(job)
    
    # Test endpoint
    response = test_client.get(f"/api/convert/{job_id}")
    
    # Check response
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "completed"
    assert "converted_files" in data
    assert "mp4" in data["converted_files"]
    assert data["converted_files"]["mp4"] == "https://example.com/video.mp4"
    assert "metadata" in data
    assert data["metadata"]["original_size_mb"] == 10.0
    assert data["metadata"]["converted_sizes_mb"]["mp4"] == 5.0
    assert "50.0%" in data["metadata"]["compression_ratio"]["mp4"]


def test_get_conversion_status_not_found(test_client):
    """Test get conversion status endpoint with non-existent job."""
    job_id = str(uuid.uuid4())
    
    # Test endpoint
    response = test_client.get(f"/api/convert/{job_id}")
    
    # Check response
    assert response.status_code == 404
    
    data = response.json()
    assert data["status"] == "error"
    assert "not found" in data["message"]


def test_get_conversion_logs(test_client):
    """Test get conversion logs endpoint."""
    # Create a job
    job_id = str(uuid.uuid4())
    job = ConversionJob(
        id=job_id,
        original_filename="test.mp4",
        temp_file_path="/tmp/test.mp4",
        formats=["mp4"],
        preserve_audio=True,
        optimize_level=OptimizationLevel.BALANCED,
        status=JobStatus.COMPLETED,
    )
    create_job(job)
    
    # Test endpoint
    response = test_client.get(f"/api/convert/{job_id}/logs")
    
    # Check response
    assert response.status_code == 200
    
    data = response.json()
    assert "job_info" in data
    assert "logs" in data
    assert data["job_info"]["id"] == job_id