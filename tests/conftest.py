import os
import pytest
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from app.core.config import settings
from app.services.converter import VideoConverter
from app.core.circuit_breaker import CircuitBreaker
from app.services.r2_uploader import R2Uploader


@pytest.fixture(scope="session")
def test_video_path():
    """Return path to test video file."""
    # This assumes there's a test video in the tests/data directory
    # You may need to create this directory and add a test video
    test_data_dir = Path(__file__).parent / "data"
    test_data_dir.mkdir(exist_ok=True)
    
    test_video = test_data_dir / "test_video.mp4"
    
    # If test video doesn't exist, you might want to generate one
    # or download one from a reliable source
    if not test_video.exists():
        pytest.skip(f"Test video not found at {test_video}")
    
    return str(test_video)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Clean up after test
    shutil.rmtree(temp_dir)


@pytest.fixture
def circuit_breaker():
    """Create a CircuitBreaker instance for testing."""
    # Create a circuit breaker with a low threshold for testing
    breaker = CircuitBreaker(
        name="test_breaker",
        failure_threshold=2,
        reset_timeout=1,  # Short timeout for testing
        half_open_max_calls=3  # Max calls in half-open state
    )
    
    yield breaker
    
    # Reset the circuit breaker after test
    breaker.reset()


@pytest.fixture
def mock_r2_uploader():
    """Create a mock R2Uploader for testing."""
    with patch("app.services.r2_uploader.R2Uploader") as mock_uploader_class:
        mock_uploader = MagicMock()
        mock_uploader_class.return_value = mock_uploader
        
        # Mock the upload_file method
        mock_uploader.upload_file.return_value = ("https://example.com/test.mp4", 1.0)
        
        # Mock the delete_file method
        mock_uploader.delete_file.return_value = True
        
        yield mock_uploader


@pytest.fixture
def video_converter(temp_dir, mock_r2_uploader):
    """Create a VideoConverter instance for testing."""
    # Override settings for testing
    original_temp_dir = settings.TEMP_DIR
    settings.TEMP_DIR = temp_dir
    
    # Create converter with 2 workers for testing
    converter = VideoConverter(max_workers=2)
    
    yield converter
    
    # Restore original settings
    settings.TEMP_DIR = original_temp_dir
