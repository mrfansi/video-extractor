FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install FFmpeg
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create temp directory
RUN mkdir -p /tmp/video-extractor && \
    chmod -R 777 /tmp/video-extractor

# Expose port
EXPOSE 8000

# Run the application
CMD ["python", "main.py"]