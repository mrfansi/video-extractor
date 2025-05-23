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
    max_retries = 60  # Longer timeout (60 seconds)
    for i in range(max_retries):
        status_response = requests.get(f"{BASE_URL}/convert/{request_id}")
        assert status_response.status_code == 200
        status_data = status_response.json()
        
        print(f"Status: {status_data['status']} - {status_data.get('message', '')}")
        
        if status_data["status"] == "completed":
            print(f"\u2705 Conversion completed successfully")
            # Verify the converted file URLs
            assert "converted_files" in status_data, f"Response missing 'converted_files' field: {status_data}"
            assert "mp4" in status_data["converted_files"], f"No mp4 file in converted_files: {status_data['converted_files']}"
            assert status_data["converted_files"]["mp4"].endswith(".mp4")
            return
        elif status_data["status"] == "failed" or status_data["status"] == "error":
            error_msg = status_data.get('message', 'Unknown error')
            print(f"\u274c Conversion failed: {error_msg}")
            
            # Get more detailed information about the error
            print("\nDetailed error information:")
            for key, value in status_data.items():
                print(f"  {key}: {value}")
                
            pytest.fail(f"Conversion failed: {error_msg}")
        
        # Wait before checking again
        time.sleep(2)  # Longer wait between checks
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
    max_retries = 60  # Longer timeout (60 seconds)
    for i in range(max_retries):
        status_response = requests.get(f"{BASE_URL}/convert/{request_id}")
        assert status_response.status_code == 200
        status_data = status_response.json()
        
        print(f"Status: {status_data['status']} - {status_data.get('message', '')}")
        
        if status_data["status"] == "completed":
            print(f"\u2705 WebM Conversion completed successfully")
            # Verify the converted file URLs
            assert "converted_files" in status_data, f"Response missing 'converted_files' field: {status_data}"
            assert "webm" in status_data["converted_files"], f"No webm file in converted_files: {status_data['converted_files']}"
            assert status_data["converted_files"]["webm"].endswith(".webm")
            return
        elif status_data["status"] == "failed" or status_data["status"] == "error":
            error_msg = status_data.get('message', 'Unknown error')
            print(f"\u274c WebM Conversion failed: {error_msg}")
            
            # Get more detailed information about the error
            print("\nDetailed error information:")
            for key, value in status_data.items():
                print(f"  {key}: {value}")
                
            pytest.fail(f"WebM Conversion failed: {error_msg}")
        
        # Wait before checking again
        time.sleep(2)  # Longer wait between checks
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
    max_retries = 90  # Longer timeout for multiple formats (90 seconds)
    for i in range(max_retries):
        status_response = requests.get(f"{BASE_URL}/convert/{request_id}")
        assert status_response.status_code == 200
        status_data = status_response.json()
        
        print(f"Status: {status_data['status']} - {status_data.get('message', '')}")
        
        if status_data["status"] == "completed":
            print(f"\u2705 Multiple format conversion completed successfully")
            # Verify the converted file URLs
            assert "converted_files" in status_data, f"Response missing 'converted_files' field: {status_data}"
            assert "mp4" in status_data["converted_files"], f"No mp4 file in converted_files: {status_data['converted_files']}"
            assert "webm" in status_data["converted_files"], f"No webm file in converted_files: {status_data['converted_files']}"
            assert status_data["converted_files"]["mp4"].endswith(".mp4")
            assert status_data["converted_files"]["webm"].endswith(".webm")
            return
        elif status_data["status"] == "failed" or status_data["status"] == "error":
            error_msg = status_data.get('message', 'Unknown error')
            print(f"\u274c Multiple format conversion failed: {error_msg}")
            
            # Get more detailed information about the error
            print("\nDetailed error information:")
            for key, value in status_data.items():
                print(f"  {key}: {value}")
                
            pytest.fail(f"Multiple format conversion failed: {error_msg}")
        
        # Wait before checking again
        time.sleep(2)  # Longer wait between checks
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

# Run individual test
def run_single_test(test_name):
    print(f"\n🔍 Running {test_name} test\n")
    
    try:
        if test_name == "health":
            test_health_endpoint()
        elif test_name == "metrics":
            test_metrics_endpoint()
        elif test_name == "mp4":
            test_conversion_mp4()
        elif test_name == "webm":
            test_conversion_webm()
        elif test_name == "multiple":
            test_conversion_multiple_formats()
        elif test_name == "invalid":
            test_invalid_file_type()
        else:
            print(f"Unknown test: {test_name}")
            return
            
        print(f"\n\u2705 {test_name} test passed successfully!")
    except Exception as e:
        print(f"\n\u274c {test_name} test failed: {str(e)}")
        raise

# Run all tests
def run_all_tests():
    print("\n🔍 Starting endpoint tests for Video Extractor API\n")
    
    tests = [
        ("health", test_health_endpoint),
        ("metrics", test_metrics_endpoint),
        ("mp4", test_conversion_mp4),
        ("webm", test_conversion_webm),
        ("multiple", test_conversion_multiple_formats),
        ("invalid", test_invalid_file_type)
    ]
    
    results = []
    for name, test_func in tests:
        print(f"\n🔍 Running {name} test\n")
        try:
            test_func()
            print(f"\u2705 {name} test passed")
            results.append((name, "PASS"))
        except Exception as e:
            print(f"\u274c {name} test failed: {str(e)}")
            results.append((name, f"FAIL: {str(e)}"))
    
    # Print summary
    print("\n📊 Test Summary:\n")
    for name, result in results:
        status = "\u2705 PASS" if result == "PASS" else f"\u274c {result}"
        print(f"{name}: {status}")
    
    # Check if all tests passed
    all_passed = all(result == "PASS" for _, result in results)
    if all_passed:
        print("\n\u2705 All tests passed successfully!")
    else:
        print("\n\u274c Some tests failed. See summary above.")
        # Raise exception to indicate test failure
        failed_tests = [name for name, result in results if result != "PASS"]
        raise Exception(f"Failed tests: {', '.join(failed_tests)}")

if __name__ == "__main__":
    import sys
    
    # Check if a specific test was requested
    if len(sys.argv) > 1:
        test_name = sys.argv[1].lower()
        run_single_test(test_name)
    else:
        # Run all tests if no specific test was requested
        run_all_tests()
