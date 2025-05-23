import os
import time
import requests
import pytest
from pathlib import Path

# Base URL for the API
BASE_URL = "http://localhost:8000/api"

# Path to a test video file
TEST_VIDEO_PATH = Path(__file__).parent.parent / "data" / "test_video.mp4"

# Create test data directory if it doesn't exist
if not TEST_VIDEO_PATH.parent.exists():
    TEST_VIDEO_PATH.parent.mkdir(parents=True)

# Create a simple test video if it doesn't exist
def create_test_video():
    if not TEST_VIDEO_PATH.exists():
        import subprocess
        # Create a 5-second test video using ffmpeg
        cmd = [
            "ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=duration=5:size=1280x720:rate=30", 
            "-c:v", "libx264", "-pix_fmt", "yuv420p", str(TEST_VIDEO_PATH)
        ]
        subprocess.run(cmd, check=True, capture_output=True)

# Test the health endpoint
def test_health_endpoint():
    response = requests.get(f"{BASE_URL}/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    print("✅ Health endpoint test passed")

# Test the metrics endpoint
def test_metrics_endpoint():
    response = requests.get("http://localhost:8000/metrics")
    assert response.status_code == 200
    assert "# HELP" in response.text
    print("✅ Metrics endpoint test passed")

# Test the conversion endpoint with MP4 format
def test_conversion_mp4():
    # Ensure we have a test video
    create_test_video()
    
    # Prepare the file for upload
    files = {
        "file": ("test_video.mp4", open(TEST_VIDEO_PATH, "rb"), "video/mp4")
    }
    data = {
        "formats": "mp4",
        "preserve_audio": "true",
        "optimize_level": "balanced"
    }
    
    # Start conversion
    response = requests.post(f"{BASE_URL}/convert", files=files, data=data)
    assert response.status_code == 202
    result = response.json()
    assert result["status"] == "processing"
    request_id = result["request_id"]
    print(f"✅ Conversion started with request ID: {request_id}")
    
    # Check conversion status (with timeout)
    max_retries = 30
    for i in range(max_retries):
        status_response = requests.get(f"{BASE_URL}/convert/{request_id}")
        assert status_response.status_code == 200
        status_data = status_response.json()
        
        if status_data["status"] == "completed":
            print(f"✅ Conversion completed successfully")
            # Verify the converted file URLs
            assert "mp4" in status_data["files"]
            assert status_data["files"]["mp4"].endswith(".mp4")
            return
        elif status_data["status"] == "failed":
            pytest.fail(f"Conversion failed: {status_data['message']}")
        
        # Wait before checking again
        time.sleep(1)
        print(f"Waiting for conversion... ({i+1}/{max_retries})")
    
    pytest.fail("Conversion timed out")

# Test the conversion endpoint with WebM format
def test_conversion_webm():
    # Ensure we have a test video
    create_test_video()
    
    # Prepare the file for upload
    files = {
        "file": ("test_video.mp4", open(TEST_VIDEO_PATH, "rb"), "video/mp4")
    }
    data = {
        "formats": "webm",
        "preserve_audio": "true",
        "optimize_level": "fast"
    }
    
    # Start conversion
    response = requests.post(f"{BASE_URL}/convert", files=files, data=data)
    assert response.status_code == 202
    result = response.json()
    assert result["status"] == "processing"
    request_id = result["request_id"]
    print(f"✅ WebM Conversion started with request ID: {request_id}")
    
    # Check conversion status (with timeout)
    max_retries = 30
    for i in range(max_retries):
        status_response = requests.get(f"{BASE_URL}/convert/{request_id}")
        assert status_response.status_code == 200
        status_data = status_response.json()
        
        if status_data["status"] == "completed":
            print(f"✅ WebM Conversion completed successfully")
            # Verify the converted file URLs
            assert "webm" in status_data["files"]
            assert status_data["files"]["webm"].endswith(".webm")
            return
        elif status_data["status"] == "failed":
            pytest.fail(f"WebM Conversion failed: {status_data['message']}")
        
        # Wait before checking again
        time.sleep(1)
        print(f"Waiting for WebM conversion... ({i+1}/{max_retries})")
    
    pytest.fail("WebM Conversion timed out")

# Test the conversion endpoint with multiple formats
def test_conversion_multiple_formats():
    # Ensure we have a test video
    create_test_video()
    
    # Prepare the file for upload
    files = {
        "file": ("test_video.mp4", open(TEST_VIDEO_PATH, "rb"), "video/mp4")
    }
    data = {
        "formats": "mp4,webm",
        "preserve_audio": "true",
        "optimize_level": "max"
    }
    
    # Start conversion
    response = requests.post(f"{BASE_URL}/convert", files=files, data=data)
    assert response.status_code == 202
    result = response.json()
    assert result["status"] == "processing"
    request_id = result["request_id"]
    print(f"✅ Multiple format conversion started with request ID: {request_id}")
    
    # Check conversion status (with timeout)
    max_retries = 60  # Longer timeout for multiple formats
    for i in range(max_retries):
        status_response = requests.get(f"{BASE_URL}/convert/{request_id}")
        assert status_response.status_code == 200
        status_data = status_response.json()
        
        if status_data["status"] == "completed":
            print(f"✅ Multiple format conversion completed successfully")
            # Verify the converted file URLs
            assert "mp4" in status_data["files"]
            assert "webm" in status_data["files"]
            assert status_data["files"]["mp4"].endswith(".mp4")
            assert status_data["files"]["webm"].endswith(".webm")
            return
        elif status_data["status"] == "failed":
            pytest.fail(f"Multiple format conversion failed: {status_data['message']}")
        
        # Wait before checking again
        time.sleep(1)
        print(f"Waiting for multiple format conversion... ({i+1}/{max_retries})")
    
    pytest.fail("Multiple format conversion timed out")

# Test invalid file type
def test_invalid_file_type():
    # Create a text file
    invalid_file_path = TEST_VIDEO_PATH.parent / "invalid.txt"
    with open(invalid_file_path, "w") as f:
        f.write("This is not a video file")
    
    # Prepare the file for upload
    files = {
        "file": ("invalid.txt", open(invalid_file_path, "rb"), "text/plain")
    }
    data = {
        "formats": "mp4",
        "preserve_audio": "true",
        "optimize_level": "balanced"
    }
    
    # Attempt conversion
    response = requests.post(f"{BASE_URL}/convert", files=files, data=data)
    assert response.status_code == 400  # Bad request
    print("✅ Invalid file type test passed")
    
    # Clean up
    invalid_file_path.unlink()

# Run all tests
def run_all_tests():
    print("\n🔍 Starting endpoint tests for Video Extractor API\n")
    
    try:
        test_health_endpoint()
        test_metrics_endpoint()
        test_conversion_mp4()
        test_conversion_webm()
        test_conversion_multiple_formats()
        test_invalid_file_type()
        print("\n✅ All tests passed successfully!")
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    run_all_tests()
