import pytest
import os
import sys
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

@pytest.fixture(scope="module")
def mock_r2_client():
    """Mock the Cloudflare R2 client"""
    with patch("app.services.storage.boto3.client") as mock_client:
        # Create a mock S3 client
        mock_s3 = MagicMock()
        
        # Mock the upload_file method
        mock_s3.upload_file.return_value = None
        
        # Mock the list_objects_v2 method
        mock_s3.list_objects_v2.return_value = {
            "CommonPrefixes": []
        }
        
        # Mock the put_object method
        mock_s3.put_object.return_value = None
        
        # Return the mock client factory
        mock_client.return_value = mock_s3
        yield mock_client

@pytest.fixture(scope="module")
def mock_ffmpeg():
    """Mock FFmpeg functionality"""
    with patch("app.services.video_converter.ffmpeg") as mock_ffmpeg:
        # Mock the probe function
        mock_probe = MagicMock()
        mock_probe.return_value = {
            "streams": [
                {
                    "codec_type": "video",
                    "width": 640,
                    "height": 480,
                    "codec_name": "h264"
                }
            ],
            "format": {
                "duration": "5.0",
                "bit_rate": "500000"
            }
        }
        mock_ffmpeg.probe = mock_probe
        
        # Mock the input function
        mock_input = MagicMock()
        mock_ffmpeg.input.return_value = mock_input
        
        # Mock the output function
        mock_output = MagicMock()
        mock_ffmpeg.output.return_value = mock_output
        
        # Mock the run function
        mock_run = MagicMock()
        mock_run.return_value = None
        mock_ffmpeg.run = mock_run
        
        yield mock_ffmpeg

@pytest.fixture(scope="module")
def mock_file_operations():
    """Mock file operations"""
    with patch("os.path.getsize") as mock_getsize:
        # Mock the getsize function to return a fixed file size
        mock_getsize.return_value = 1024 * 1024  # 1MB
        yield mock_getsize

@pytest.fixture(scope="module")
def test_client(mock_r2_client, mock_ffmpeg, mock_file_operations):
    """Create a test client with mocked dependencies"""
    # Set test environment variables
    os.environ["R2_ENDPOINT_URL"] = "https://test.r2.cloudflarestorage.com"
    os.environ["R2_ACCESS_KEY_ID"] = "test_access_key"
    os.environ["R2_SECRET_ACCESS_KEY"] = "test_secret_key"
    os.environ["R2_BUCKET_NAME"] = "test-bucket"
    os.environ["R2_PUBLIC_URL"] = "https://test.r2.dev"
    
    # Ensure boto3 uses 'auto' region for Cloudflare R2
    with patch("boto3.client") as mock_boto_client:
        mock_boto_client.return_value = mock_r2_client.return_value
    
    # Import the app after setting environment variables
    from app.main import app
    
    # Create a test client
    client = TestClient(app)
    
    # Mock the VideoConverter._convert_video method
    with patch("app.services.video_converter.VideoConverter._convert_video") as mock_convert:
        # Make the _convert_video method return a mock output file and size
        mock_convert.return_value = ("/tmp/output.mp4", 512 * 1024)  # 512KB
        
        yield client
