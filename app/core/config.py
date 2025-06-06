import os
from pathlib import Path
from typing import Dict, List, Optional, Union

from pydantic import AnyHttpUrl, field_validator, ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Application settings.
    """
    API_HOST: str
    API_PORT: int
    API_WORKERS: int
    API_PREFIX: str
    
    # R2 Storage configuration
    R2_ENDPOINT_URL: str
    R2_ACCESS_KEY_ID: str
    R2_SECRET_ACCESS_KEY: str
    R2_BUCKET_NAME: str
    R2_PUBLIC_URL: str
    R2_REGION: str = "auto"  # Default to 'auto' if not specified
    
    # Processing configuration
    TEMP_DIR: str
    MAX_WORKERS: int
    MAX_UPLOAD_SIZE_MB: int
    ENABLE_METRICS: bool
    
    @field_validator("TEMP_DIR")
    def create_temp_dir(cls, v):
        """Validate and create temp directory if it doesn't exist."""
        temp_dir = Path(v)
        if not temp_dir.exists():
            temp_dir.mkdir(parents=True, exist_ok=True)
        return v
    
    # Format options
    SUPPORTED_FORMATS: List[str] = ["mp4", "webm", "mov"]
    OPTIMIZATION_LEVELS: List[str] = ["fast", "balanced", "max"]
    
    model_config = ConfigDict(
        env_file=".env",
        case_sensitive=True
    )


settings = Settings()