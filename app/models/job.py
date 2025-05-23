import enum
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Set

from pydantic import BaseModel, Field

from app.schemas.video import OptimizationLevel


class JobStatus(str, enum.Enum):
    """Enum for job status."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class ConversionJob(BaseModel):
    """Model for video conversion job."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    original_filename: str
    temp_file_path: str
    formats: List[str]
    preserve_audio: bool
    optimize_level: OptimizationLevel
    status: JobStatus = JobStatus.PENDING
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)
    completed_at: Optional[float] = None
    error_message: Optional[str] = None
    
    # For storing processing results
    original_size_mb: Optional[float] = None
    converted_files: Dict[str, str] = Field(default_factory=dict)
    converted_sizes_mb: Dict[str, float] = Field(default_factory=dict)
    compression_ratios: Dict[str, str] = Field(default_factory=dict)
    
    def update_status(self, status: JobStatus, error: Optional[str] = None) -> None:
        """Update job status."""
        self.status = status
        self.updated_at = time.time()
        
        if status == JobStatus.FAILED and error:
            self.error_message = error
        
        if status == JobStatus.COMPLETED:
            self.completed_at = time.time()
    
    def add_converted_file(self, format: str, url: str, size_mb: float) -> None:
        """Add information about a converted file."""
        self.converted_files[format] = url
        self.converted_sizes_mb[format] = size_mb
        
        if self.original_size_mb and size_mb > 0:
            ratio = (1 - (size_mb / self.original_size_mb)) * 100
            self.compression_ratios[format] = f"{ratio:.1f}%"
    
    def get_processing_time(self) -> Optional[float]:
        """Get processing time in seconds if job is completed."""
        if self.status == JobStatus.COMPLETED and self.completed_at:
            return self.completed_at - self.created_at
        return None
    
    def to_dict(self) -> dict:
        """Convert job to dictionary representation."""
        return {
            "id": self.id,
            "status": self.status.value,
            "created_at": datetime.fromtimestamp(self.created_at).isoformat(),
            "updated_at": datetime.fromtimestamp(self.updated_at).isoformat(),
            "completed_at": (
                datetime.fromtimestamp(self.completed_at).isoformat()
                if self.completed_at
                else None
            ),
            "formats": self.formats,
            "original_filename": self.original_filename,
            "preserve_audio": self.preserve_audio,
            "optimize_level": self.optimize_level.value,
            "error_message": self.error_message,
            "converted_files": self.converted_files,
            "metadata": {
                "original_size_mb": self.original_size_mb,
                "converted_sizes_mb": self.converted_sizes_mb,
                "compression_ratio": self.compression_ratios,
            } if self.original_size_mb else None,
        }


# In-memory storage for jobs (for simplicity)
# In a production app, use a database or Redis
JOBS: Dict[str, ConversionJob] = {}


def get_job(job_id: str) -> Optional[ConversionJob]:
    """Get job by ID."""
    return JOBS.get(job_id)


def create_job(job: ConversionJob) -> None:
    """Store a new job."""
    JOBS[job.id] = job


def get_all_jobs() -> List[ConversionJob]:
    """Get all jobs."""
    return list(JOBS.values())


def get_jobs_by_status(status: JobStatus) -> List[ConversionJob]:
    """Get jobs by status."""
    return [job for job in JOBS.values() if job.status == status]


def get_stats() -> Dict[str, int]:
    """Get statistics about job processing."""
    total = len(JOBS)
    pending = len([job for job in JOBS.values() if job.status == JobStatus.PENDING])
    processing = len([job for job in JOBS.values() if job.status == JobStatus.PROCESSING])
    completed = len([job for job in JOBS.values() if job.status == JobStatus.COMPLETED])
    failed = len([job for job in JOBS.values() if job.status == JobStatus.FAILED])
    
    return {
        "total": total,
        "pending": pending,
        "processing": processing,
        "completed": completed,
        "failed": failed,
    }