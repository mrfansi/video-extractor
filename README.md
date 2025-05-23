# Video Extractor API

A production-grade, asynchronous FastAPI application that handles video uploads, converts them into optimized formats using FFmpeg, and uploads results to Cloudflare R2.

## Features

- **Asynchronous Video Processing**: Handle video uploads and process them asynchronously in the background
- **Multiple Format Support**: Convert videos to MP4, WebM, and MOV formats
- **Optimization Levels**: Choose from fast, balanced, or max optimization levels
- **Audio Control**: Option to preserve or remove audio from the output
- **Progress Tracking**: Monitor conversion progress with unique request IDs
- **Metrics Monitoring**: Prometheus-compatible metrics for monitoring the API
- **Docker Support**: Easy deployment with Docker

## API Endpoints

### POST /api/convert

Upload a video file and start an asynchronous conversion process.

**Form Data:**

- `file` (required): Video file (e.g., .mp4, .mov, .mkv)
- `formats` (optional): Comma-separated list of output formats (default: mp4)
- `preserve_audio` (optional): Whether to preserve audio (default: true)
- `optimize_level` (optional): Optimization level: fast, balanced, or max (default: balanced)

**Response:**

```json
{
  "status": "processing",
  "request_id": "9e4c182e-43e0-4b6e-a274-9849bd2eb3f7",
  "message": "Conversion started. Monitor at /api/convert/{request_id}"
}
```

### GET /api/convert/{request_id}

Check the status of a conversion request.

**Response (Completed):**

```json
{
  "status": "completed",
  "converted_files": {
    "mp4": "https://pub-r2.example.dev/mp4/video.mp4"
  },
  "metadata": {
    "original_size_mb": 102.3,
    "converted_sizes_mb": { "mp4": 41.9 },
    "compression_ratio": { "mp4": "59.0%" }
  }
}
```

**Response (Processing):**

```json
{
  "status": "processing",
  "message": "Video is being processed."
}
```

**Response (Error):**

```json
{
  "status": "error",
  "message": "Request ID not found."
}
```

### GET /api/health

Basic health check endpoint.

**Response:**

```json
{
  "status": "ok",
  "message": "video-extractor API is healthy"
}
```

### GET /api/metrics

Prometheus-compatible metrics endpoint for monitoring the API.

## Installation

### Prerequisites

- Python 3.10+
- FFmpeg
- Cloudflare R2 account (or compatible S3 storage)

### Compatibility Notes

This project has been updated to use:
- FastAPI's new lifespan context manager (replacing deprecated `on_event` handlers)
- Pydantic V2 field validators (replacing deprecated V1 validators)
- Timezone-aware datetime objects (replacing deprecated `datetime.utcnow()`)

All code is compatible with the latest versions of FastAPI and Pydantic.

### Environment Variables

Create a `.env` file with the following variables:

```
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=4
API_PREFIX=/api

R2_ENDPOINT_URL=https://xxxxxxxxxxxx.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=your_access_key_id
R2_SECRET_ACCESS_KEY=your_secret_access_key
R2_BUCKET_NAME=your_bucket_name
R2_PUBLIC_URL=https://pub-xxxxxxxxxxxxxxxx.r2.dev

TEMP_DIR=/tmp/video-extractor
MAX_WORKERS=4
MAX_UPLOAD_SIZE_MB=500
ENABLE_METRICS=true
```

### Local Development

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/video-extractor.git
   cd video-extractor
   ```

2. Create and activate a virtual environment:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Run the application:

   ```bash
   python main.py
   ```

The API will be available at <http://localhost:8000>.

### Docker Deployment

1. Build the Docker image:

   ```bash
   docker build -t video-extractor .
   ```

2. Run the container:

   ```bash
   docker run -p 8000:8000 \
     --env-file .env \
     -v /tmp/video-extractor:/tmp/video-extractor \
     video-extractor
   ```

## Metrics

The application exposes the following Prometheus metrics:

- `video_extractor_requests_total{endpoint="/api/convert"}`
- `video_extractor_processing_jobs{status="in_progress"}`
- `video_extractor_completed_total`
- `video_extractor_failed_total`
- `video_extractor_processing_duration_seconds`
- `video_extractor_file_size_bytes{type="original"}`

## License

MIT
