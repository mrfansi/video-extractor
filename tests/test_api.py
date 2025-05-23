import pytest
import os
from pathlib import Path

# Test data directory
TEST_DIR = Path(__file__).parent / "data"
os.makedirs(TEST_DIR, exist_ok=True)

def test_health_endpoint(test_client):
    """Test the health check endpoint"""
    response = test_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_api_health_endpoint(test_client):
    """Test the API health check endpoint"""
    response = test_client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_formats_endpoint(test_client):
    """Test the formats endpoint"""
    response = test_client.get("/api/formats")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "input_formats" in data["data"]
    assert "output_formats" in data["data"]

def test_convert_endpoint(test_client):
    """Test the convert endpoint with a sample video"""
    # Use the sample video file
    test_video = TEST_DIR / "sample.mp4"
    
    # Skip if sample video not available
    if not test_video.exists():
        pytest.skip("Sample video not available")
    
    with open(test_video, "rb") as f:
        response = test_client.post(
            "/api/convert",
            files={"file": ("sample.mp4", f, "video/mp4")}
        )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "data" in data
    assert "formats" in data["data"]
    
    # Check that all formats are present
    formats = data["data"]["formats"]
    assert "mp4" in formats
    assert "webm" in formats
    assert "mov" in formats
    
    # Check that URLs are present and properly formatted
    for format_name, format_data in formats.items():
        assert "url" in format_data
        assert format_data["url"].startswith("https://")
        assert format_name in format_data["url"]
    
    # Check metadata
    assert "metadata" in data
    assert "original_size" in data["metadata"]
    assert "total_output_size" in data["metadata"]
    assert "compression_ratio" in data["metadata"]
    assert "formats_count" in data["metadata"]
    assert data["metadata"]["formats_count"] == 3  # mp4, webm, mov
