# Video Extractor API Documentation

## Overview

The Video Extractor API is a RESTful service that accepts video files and converts them into multiple optimized formats while maintaining original resolution and visual quality but reducing file size. The optimized videos are then uploaded to a Cloudflare R2 bucket and made available via public URLs.

## Base URL

```
http://localhost:8000
```

When deployed, replace with your actual domain.

## API Endpoints

### Health Check

```
GET /health
```

Check if the API is running properly.

**Response**

```json
{
  "status": "ok"
}
```

### API Health Check

```
GET /api/health
```

Check if the API endpoints are functioning properly.

**Response**

```json
{
  "status": "ok"
}
```

### Get Supported Formats

```
GET /api/formats
```

Get a list of supported input and output video formats.

**Response**

```json
{
  "status": "success",
  "data": {
    "input_formats": [".mp4", ".webm", ".mov", ".avi", ".mkv", ".mpeg", ".ogg"],
    "output_formats": [".mp4", ".webm", ".mov"]
  }
}
```

### Convert Video

```
POST /api/convert
```

Upload a video file and convert it to multiple optimized formats (.mp4, .webm, .mov) while maintaining original resolution and visual quality. The optimized videos are uploaded to Cloudflare R2 and the response includes URLs for all formats.

**Request**

- Content-Type: `multipart/form-data`
- Body: Form data with a video file named `file`

**Example Request using cURL**

```bash
curl -X POST "http://localhost:8000/api/convert" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@/path/to/your/video.mp4"
```

**Example Request using Python**

```python
import requests

url = "http://localhost:8000/api/convert"
files = {"file": open("/path/to/your/video.mp4", "rb")}

response = requests.post(url, files=files)
print(response.json())
```

**Successful Response (200 OK)**

```json
{
  "status": "success",
  "message": "Video processed successfully",
  "data": {
    "original": {
      "filename": "input.mp4",
      "size": 10485760,
      "resolution": "1920x1080"
    },
    "formats": {
      "mp4": {
        "filename": "output.mp4",
        "size": 5242880,
        "resolution": "1920x1080",
        "url": "https://example.com/mp4/output.mp4"
      },
      "webm": {
        "filename": "output.webm",
        "size": 4194304,
        "resolution": "1920x1080",
        "url": "https://example.com/webm/output.webm"
      },
      "mov": {
        "filename": "output.mov",
        "size": 6291456,
        "resolution": "1920x1080",
        "url": "https://example.com/mov/output.mov"
      }
    }
  },
  "metadata": {
    "original_size": 10485760,
    "total_output_size": 15728640,
    "compression_ratio": 0.67,
    "formats_count": 3
  }
}
```

**Error Responses**

*Invalid File (400 Bad Request)*

```json
{
  "detail": "Unsupported file type. File must be a valid video."
}
```

*File Too Large (413 Payload Too Large)*

```json
{
  "detail": "File too large. Maximum size is 500MB"
}
```

*Server Error (500 Internal Server Error)*

```json
{
  "detail": "Error processing video: [error details]"
}
```

## Response Structure

### Convert Endpoint Response

| Field | Type | Description |
|-------|------|-------------|
| status | string | Status of the request ("success" or "error") |
| message | string | Message describing the result |
| data | object | Data containing original file info and converted formats |
| data.original | object | Information about the original uploaded file |
| data.original.filename | string | Name of the original file |
| data.original.size | integer | Size of the original file in bytes |
| data.original.resolution | string | Resolution of the original video (width x height) |
| data.formats | object | Information about each converted format |
| data.formats.[format] | object | Information about a specific format (mp4, webm, mov) |
| data.formats.[format].filename | string | Name of the converted file |
| data.formats.[format].size | integer | Size of the converted file in bytes |
| data.formats.[format].resolution | string | Resolution of the converted video (width x height) |
| data.formats.[format].url | string | Public URL to access the converted video |
| metadata | object | Additional metadata about the conversion |
| metadata.original_size | integer | Size of the original file in bytes |
| metadata.total_output_size | integer | Total size of all converted files in bytes |
| metadata.compression_ratio | float | Ratio of original size to total output size |
| metadata.formats_count | integer | Number of output formats generated |

## Error Handling

The API uses standard HTTP status codes to indicate the success or failure of a request:

- 200 OK: The request was successful
- 400 Bad Request: The request was invalid (e.g., unsupported file type)
- 413 Payload Too Large: The uploaded file exceeds the maximum allowed size
- 500 Internal Server Error: An error occurred on the server

Error responses include a `detail` field with a description of the error.

## Limitations

- Maximum upload file size: 500MB (configurable via environment variables)
- Supported input formats: .mp4, .webm, .mov, .avi, .mkv, .mpeg, .ogg
- Supported output formats: .mp4, .webm, .mov

## Configuration

The API can be configured using environment variables:

- `API_HOST`: Host to bind the API server (default: 0.0.0.0)
- `API_PORT`: Port to bind the API server (default: 8000)
- `API_WORKERS`: Number of worker processes (default: 4)
- `API_PREFIX`: Prefix for API endpoints (default: /api)
- `R2_ENDPOINT_URL`: Cloudflare R2 endpoint URL
- `R2_ACCESS_KEY_ID`: Cloudflare R2 access key ID
- `R2_SECRET_ACCESS_KEY`: Cloudflare R2 secret access key
- `R2_BUCKET_NAME`: Cloudflare R2 bucket name
- `R2_PUBLIC_URL`: Public URL for the R2 bucket
- `TEMP_DIR`: Directory for temporary files (default: /tmp/video-extractor)
- `MAX_WORKERS`: Maximum number of worker threads (default: 4)
- `MAX_UPLOAD_SIZE_MB`: Maximum upload size in MB (default: 500)
