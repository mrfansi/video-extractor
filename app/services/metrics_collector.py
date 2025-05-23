import time
from typing import Dict, List, Optional

from prometheus_client import Counter, Gauge, Histogram, Summary

from app.models.job import JobStatus


class MetricsCollector:
    """Service for collecting and exposing application metrics."""
    
    def __init__(self):
        """Initialize the metrics collector."""
        # Total requests counter
        self.requests_total = Counter(
            'video_extractor_requests_total',
            'Total number of requests',
            ['endpoint']
        )
        
        # Processing jobs gauge
        self.processing_jobs = Gauge(
            'video_extractor_processing_jobs',
            'Number of video processing jobs',
            ['status']
        )
        
        # Completed conversions counter
        self.completed_total = Counter(
            'video_extractor_completed_total',
            'Total number of completed conversions',
            ['format']
        )
        
        # Failed conversions counter
        self.failed_total = Counter(
            'video_extractor_failed_total',
            'Total number of failed conversions'
        )
        
        # Processing duration histogram
        self.processing_duration = Histogram(
            'video_extractor_processing_duration_seconds',
            'Video processing duration in seconds',
            ['format'],
            buckets=(5, 15, 30, 60, 120, 300, 600, 900, 1800, 3600)
        )
        
        # File size gauge
        self.file_size = Gauge(
            'video_extractor_file_size_bytes',
            'File size in bytes',
            ['type', 'format']
        )
        
        # Compression ratio summary
        self.compression_ratio = Summary(
            'video_extractor_compression_ratio',
            'Video compression ratio',
            ['format']
        )
    
    def record_request(self, endpoint: str) -> None:
        """
        Record an API request.
        
        Args:
            endpoint: API endpoint path
        """
        self.requests_total.labels(endpoint=endpoint).inc()
    
    def update_jobs_gauge(self, status_counts: Dict[str, int]) -> None:
        """
        Update the jobs gauge with current counts.
        
        Args:
            status_counts: Dictionary mapping status to count
        """
        for status, count in status_counts.items():
            self.processing_jobs.labels(status=status).set(count)
    
    def record_completion(self, format: str) -> None:
        """
        Record a completed conversion.
        
        Args:
            format: Video format that was converted to
        """
        self.completed_total.labels(format=format).inc()
    
    def record_failure(self) -> None:
        """Record a failed conversion."""
        self.failed_total.inc()
    
    def record_processing_time(self, format: str, duration_seconds: float) -> None:
        """
        Record processing time for a video conversion.
        
        Args:
            format: Video format that was converted to
            duration_seconds: Processing time in seconds
        """
        self.processing_duration.labels(format=format).observe(duration_seconds)
    
    def record_file_size(
        self, file_type: str, format: str, size_bytes: float
    ) -> None:
        """
        Record file size.
        
        Args:
            file_type: Type of file (original or converted)
            format: Video format
            size_bytes: File size in bytes
        """
        self.file_size.labels(type=file_type, format=format).set(size_bytes)
    
    def record_compression_ratio(self, format: str, ratio: float) -> None:
        """
        Record compression ratio.
        
        Args:
            format: Video format
            ratio: Compression ratio (original/converted)
        """
        self.compression_ratio.labels(format=format).observe(ratio)


# Singleton instance
metrics_collector = MetricsCollector()