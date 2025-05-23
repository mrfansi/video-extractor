# Video Extractor API

A Python-based API that accepts video files and converts them into multiple optimized formats while maintaining original resolution and visual quality but reducing file size. The optimized videos are then uploaded to a Cloudflare R2 bucket.

## Features

- Convert videos to multiple formats (.mp4, .webm, .mov)
- Optimize videos using efficient codecs (H.264, VP9)
- Maintain original resolution and visual quality
- Reduce file size through advanced compression techniques
- Upload optimized videos to Cloudflare R2 bucket
- Store files in format-specific subdirectories
- Support concurrent processing
- Return JSON response with URLs of all uploaded videos

## Requirements

- Python 3.9+
- FFmpeg installed on the system
- Cloudflare R2 account and credentials

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/video-extractor.git
   cd video-extractor
   ```

2. Install dependencies using Poetry:
   ```bash
   poetry install
   ```

3. Copy the example environment file and update it with your credentials:
   ```bash
   cp .env.example .env
   # Edit .env with your Cloudflare R2 credentials
   ```

## Usage

### Running Locally

1. Start the API server:
   ```bash
   python main.py
   ```
   
   Or directly with uvicorn:
   ```bash
   poetry run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
   ```

### Running with Docker

1. Build the Docker image:
   ```bash
   docker build -t video-extractor .
   ```

2. Run the container:
   ```bash
   docker run -p 8000:8000 --env-file .env -v /tmp/video-extractor:/tmp/video-extractor video-extractor
   ```

3. To run the container in the background:
   ```bash
   docker run -d -p 8000:8000 --env-file .env -v /tmp/video-extractor:/tmp/video-extractor --name video-extractor-api video-extractor
   ```

4. To stop the background container:
   ```bash
   docker stop video-extractor-api
   ```

2. Access the API documentation at http://localhost:8000/docs

3. Use the `/convert` endpoint to upload a video file and get optimized versions

## API Endpoints

### POST /api/convert

Uploads a video file and converts it into multiple optimized formats.

**Request:**
- Form data with a video file

**Response:**
```json
{
  "status": "success",
  "message": "Video processed successfully",
  "data": {
    "original": {
      "filename": "original.mp4",
      "size": 10485760,
      "url": "https://example.com/original.mp4"
    },
    "formats": {
      "mp4": {
        "filename": "optimized.mp4",
        "size": 5242880,
        "url": "https://example.com/mp4/optimized.mp4"
      },
      "webm": {
        "filename": "optimized.webm",
        "size": 4194304,
        "url": "https://example.com/webm/optimized.webm"
      },
      "mov": {
        "filename": "optimized.mov",
        "size": 6291456,
        "url": "https://example.com/mov/optimized.mov"
      }
    }
  }
}
```

## Configuration

The application can be configured using environment variables in the `.env` file:

- `API_HOST`: Host to bind the API server (default: 0.0.0.0)
- `API_PORT`: Port to bind the API server (default: 8000)
- `API_WORKERS`: Number of worker processes (default: 4)
- `R2_ENDPOINT_URL`: Cloudflare R2 endpoint URL
- `R2_ACCESS_KEY_ID`: Cloudflare R2 access key ID
- `R2_SECRET_ACCESS_KEY`: Cloudflare R2 secret access key
- `R2_BUCKET_NAME`: Cloudflare R2 bucket name
- `R2_PUBLIC_URL`: Public URL for the R2 bucket
- `TEMP_DIR`: Directory for temporary files (default: /tmp/video-extractor)
- `MAX_WORKERS`: Maximum number of worker threads (default: 4)
- `MAX_UPLOAD_SIZE_MB`: Maximum upload size in MB (default: 500)

## License

MIT
