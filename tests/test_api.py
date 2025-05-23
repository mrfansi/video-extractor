import pytest
from fastapi.testclient import TestClient
from app.main import app
import os
from pathlib import Path

# Initialize test client
client = TestClient(app)

# Test data directory
TEST_DIR = Path(__file__).parent / "data"
os.makedirs(TEST_DIR, exist_ok=True)

def test_health_endpoint():
    """Test the health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_api_health_endpoint():
    """Test the API health check endpoint"""
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_formats_endpoint():
    """Test the formats endpoint"""
    response = client.get("/api/formats")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "input_formats" in data["data"]
    assert "output_formats" in data["data"]

# This test requires a real video file and configured R2 credentials
# Uncomment and modify when ready to test with actual files
'''
def test_convert_endpoint():
    """Test the convert endpoint with a sample video"""
    # Create a small test video file
    test_video = TEST_DIR / "test_video.mp4"
    
    # Skip if no test video available
    if not test_video.exists():
        pytest.skip("Test video not available")
    
    with open(test_video, "rb") as f:
        response = client.post(
            "/api/convert",
            files={"file": ("test_video.mp4", f, "video/mp4")}
        )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "data" in data
    assert "formats" in data["data"]
'''
