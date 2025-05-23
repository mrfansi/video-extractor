import os
import pytest
import shutil
import tempfile
from pathlib import Path

from app.core.config import settings
from app.services.converter import VideoConverter


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
def video_converter(temp_dir):
    """Create a VideoConverter instance for testing."""
    # Override settings for testing
    original_temp_dir = settings.TEMP_DIR
    settings.TEMP_DIR = temp_dir
    
    # Create converter with 2 workers for testing
    converter = VideoConverter(max_workers=2)
    
    yield converter
    
    # Restore original settings
    settings.TEMP_DIR = original_temp_dir
