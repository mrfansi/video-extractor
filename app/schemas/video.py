from enum import Enum
from typing import Dict, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field, validator


class OptimizationLevel(str, Enum):
    """Enum for video optimization levels."""
    FAST = "fast"
    BALANCED = "balanced"
    MAX = "max"


class VideoConversionRequest(BaseModel):
    """Schema for video conversion request parameters."""
    formats: Optional[str] = Field(
        default="mp4",
        description="Comma-separated list of output formats"
    )
    preserve_audio: Optional[bool] = Field(
        default=True,
        description="Whether to preserve audio in the output"
    )
    optimize_level: OptimizationLevel = Field(
        default=OptimizationLevel.BALANCED,
        description="Level of optimization to apply"
    )

    @validator("formats")
    def validate_formats(cls, v):
        """Validate that formats are supported."""
        if not v:
            return "mp4"
        
        formats = [fmt.strip().lower() for fmt in v.split(",")]
        from app.core.config import settings
        
        for fmt in formats:
            if fmt not in settings.SUPPORTED_FORMATS:
                raise ValueError(
                    f"Format '{fmt}' is not supported. "
                    f"Supported formats: {', '.join(settings.SUPPORTED_FORMATS)}"
                )
        
        return v


class ConversionStatusBase(BaseModel):
    """Base schema for conversion status responses."""
    status: str


class ConversionRequestResponse(ConversionStatusBase):
    """Schema for response when a conversion request is submitted."""
    request_id: UUID
    message: str


class ConversionErrorResponse(ConversionStatusBase):
    """Schema for error responses."""
    message: str


class ConversionProcessingResponse(ConversionStatusBase):
    """Schema for response when a conversion is still processing."""
    message: str


class FileMetadata(BaseModel):
    """Schema for file metadata."""
    original_size_mb: float
    converted_sizes_mb: Dict[str, float]
    compression_ratio: Dict[str, str]


class ConversionCompletedResponse(ConversionStatusBase):
    """Schema for response when a conversion is completed."""
    converted_files: Dict[str, str]
    metadata: FileMetadata


class HealthResponse(BaseModel):
    """Schema for health check response."""
    status: str
    message: str