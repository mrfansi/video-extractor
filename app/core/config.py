from pydantic_settings import BaseSettings
from pydantic import Field, ConfigDict
from typing import Optional
import os
from pathlib import Path

class Settings(BaseSettings):
    # Use ConfigDict instead of class Config to avoid deprecation warning
    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )
    
    # API Settings
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    api_workers: int = Field(default=4)
    api_prefix: str = Field(default="/api")
    
    # Cloudflare R2 Settings
    r2_endpoint_url: str
    r2_access_key_id: str
    r2_secret_access_key: str
    r2_bucket_name: str
    r2_public_url: str
    
    # Processing Settings
    temp_dir: str = Field(default="/tmp/video-extractor")
    max_workers: int = Field(default=4)
    max_upload_size_mb: int = Field(default=500)

# Create temp directory if it doesn't exist
def create_temp_dir(temp_dir: str) -> None:
    Path(temp_dir).mkdir(parents=True, exist_ok=True)

# Initialize settings
settings = Settings()

# Ensure temp directory exists
create_temp_dir(settings.temp_dir)
