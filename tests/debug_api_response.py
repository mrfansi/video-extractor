import requests
import sys
import time
import json
import subprocess
from pathlib import Path

# Base URL for API
BASE_URL = "http://localhost:8000/api"

# Path to a test video file
TEST_VIDEO_PATH = Path(__file__).parent / "data" / "test_video.mp4"

# Create test data directory if it doesn't exist
if not TEST_VIDEO_PATH.parent.exists():
    TEST_VIDEO_PATH.parent.mkdir(parents=True)

# Create a simple test video if it doesn't exist
def create_test_video():
    if not TEST_VIDEO_PATH.exists():
        print("Creating test video file...")
        # Create a 5-second test video using ffmpeg
        cmd = [
            "ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=duration=5:size=1280x720:rate=30", 
            "-c:v", "libx264", "-pix_fmt", "yuv420p", str(TEST_VIDEO_PATH)
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        print(f"Test video created at {TEST_VIDEO_PATH}")

# Get request ID from command line argument if provided
request_id = sys.argv[1] if len(sys.argv) > 1 else None

if not request_id:
    print("No request ID provided. Making a new conversion request...")
    # Ensure we have a test video
    create_test_video()
    
    # Start a conversion
    with open(TEST_VIDEO_PATH, "rb") as f:
        files = {"file": ("test_video.mp4", f, "video/mp4")}
        data = {"formats": "mp4", "optimize_level": "balanced"}
        response = requests.post(f"{BASE_URL}/convert", files=files, data=data)
    
    if response.status_code != 202:
        print(f"Error starting conversion: {response.status_code}")
        print(response.text)
        sys.exit(1)
    
    result = response.json()
    request_id = result["request_id"]
    print(f"Conversion started with request ID: {request_id}")

# Check conversion status
print(f"Checking status for request ID: {request_id}")

max_retries = 60
for i in range(max_retries):
    status_response = requests.get(f"{BASE_URL}/convert/{request_id}")
    
    if status_response.status_code != 200:
        print(f"Error checking status: {status_response.status_code}")
        print(status_response.text)
        sys.exit(1)
    
    status_data = status_response.json()
    
    # Print the full response
    print(f"\nResponse status code: {status_response.status_code}")
    print(f"Response headers: {status_response.headers}")
    print(f"Response body (raw): {status_response.text}")
    print(f"Response body (parsed): {json.dumps(status_data, indent=2)}")
    
    if status_data["status"] == "completed":
        print("\nConversion completed successfully")
        break
    elif status_data["status"] == "failed" or status_data["status"] == "error":
        print(f"\nConversion failed: {status_data.get('message', 'Unknown error')}")
        break
    
    print(f"Waiting for conversion... ({i+1}/{max_retries})")
    time.sleep(1)
