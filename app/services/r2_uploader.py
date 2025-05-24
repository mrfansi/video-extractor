import os
import random
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from loguru import logger

from app.core.config import settings
from app.core.errors import StorageError
from app.core.circuit_breaker import circuit_breaker, CircuitBreakerError


class R2Uploader:
    """Service for uploading files to Cloudflare R2."""
    
    def __init__(self):
        """Initialize the R2 client."""
        self.bucket_name = settings.R2_BUCKET_NAME
        self.public_url = settings.R2_PUBLIC_URL
        self.is_available = False
        
        try:
            # Initialize S3 client
            self.s3_client = boto3.client(
                's3',
                endpoint_url=settings.R2_ENDPOINT_URL,
                aws_access_key_id=settings.R2_ACCESS_KEY_ID,
                aws_secret_access_key=settings.R2_SECRET_ACCESS_KEY,
                region_name=settings.R2_REGION,  # Use the region from settings
            )
            
            # Try to ensure bucket exists, but don't fail if it doesn't
            self._ensure_bucket_exists()
            self.is_available = True
        except Exception as e:
            logger.warning(f"R2 storage initialization failed: {str(e)}. "
                          f"File storage will be unavailable.")
            self.s3_client = None
    
    def _ensure_bucket_exists(self) -> None:
        """Ensure the bucket exists, create if it doesn't."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket {self.bucket_name} exists")
            return True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == '404':
                logger.info(f"Bucket {self.bucket_name} does not exist. Creating...")
                try:
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                    logger.info(f"Bucket {self.bucket_name} created successfully")
                    return True
                except Exception as create_error:
                    logger.warning(f"Failed to create bucket: {str(create_error)}")
                    return False
            else:
                logger.warning(f"Error checking bucket: {str(e)}")
                return False
    
    def _get_content_type(self, file_extension: str) -> str:
        """Get the content type based on file extension."""
        content_types = {
            '.mp4': 'video/mp4',
            '.webm': 'video/webm',
            '.mov': 'video/quicktime',
            '.avi': 'video/x-msvideo',
            '.mkv': 'video/x-matroska',
        }
        return content_types.get(file_extension.lower(), 'application/octet-stream')
    
    @circuit_breaker(
        name='r2_storage',
        failure_threshold=5,
        reset_timeout=60,
        half_open_max_calls=2
    )
    def upload_file(
        self, file_path: str, object_key: Optional[str] = None
    ) -> Tuple[str, float]:
        """
        Upload a file to R2 storage with circuit breaker protection.
        
        Args:
            file_path: Path to the file to upload
            object_key: Custom object key, if None, the file name will be used
            
        Returns:
            Tuple containing public URL and file size in MB
            
        Raises:
            StorageError: If the upload fails
            CircuitBreakerError: If the circuit breaker is open due to previous failures
        """
        # Check if R2 is available
        if not self.is_available or self.s3_client is None:
            error_msg = "R2 storage is not available"
            logger.error(error_msg)
            raise StorageError(error_msg)
            
        file_path = Path(file_path)
        
        if not file_path.exists():
            error_msg = f"File not found: {file_path}"
            logger.error(error_msg)
            raise StorageError(error_msg)
        
        # Get file size in MB
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        
        # If object_key is not provided, use the file name
        if not object_key:
            object_key = file_path.name
        
        # Get content type based on file extension
        content_type = self._get_content_type(file_path.suffix)
        
        try:
            logger.info(f"Uploading file {file_path} to R2 as {object_key}")
            
            # Upload file to R2
            with open(file_path, 'rb') as file_data:
                self.s3_client.upload_fileobj(
                    file_data,
                    self.bucket_name,
                    object_key,
                    ExtraArgs={
                        'ContentType': content_type,
                        'ACL': 'public-read',  # Make it publicly accessible
                    }
                )
            
            # Generate public URL
            public_url = f"{self.public_url}/{object_key}"
            logger.info(f"Upload successful. Public URL: {public_url}")
            
            return public_url, file_size_mb
            
        except Exception as e:
            error_msg = f"Failed to upload file to R2: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    @circuit_breaker(
        name='r2_storage',
        failure_threshold=5,
        reset_timeout=60,
        half_open_max_calls=2
    )
    def delete_file(self, object_key: str) -> bool:
        """
        Delete a file from R2 storage with circuit breaker protection.
        
        Args:
            object_key: Object key to delete
            
        Returns:
            True if deletion was successful, False otherwise
            
        Raises:
            CircuitBreakerError: If the circuit breaker is open due to previous failures
        """
        # Check if R2 is available
        if not self.is_available or self.s3_client is None:
            logger.warning(f"Cannot delete file {object_key}: R2 storage is not available")
            return False
            
        try:
            logger.info(f"Deleting file {object_key} from R2")
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=object_key)
            logger.info(f"File {object_key} deleted successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to delete file from R2: {str(e)}")
            return False
    
    def delete_files(self, object_keys: list) -> Dict[str, bool]:
        """
        Delete multiple files from R2 storage.
        
        Args:
            object_keys: List of object keys to delete
            
        Returns:
            Dictionary mapping object keys to deletion status
            
        Note:
            This method uses the circuit-breaker-protected delete_file method,
            so it will respect the circuit breaker state.
        """
        results = {}
        
        for key in object_keys:
            try:
                results[key] = self.delete_file(key)
            except CircuitBreakerError as e:
                # Circuit breaker is open, stop processing remaining files
                logger.warning(f"Circuit breaker open during batch deletion: {str(e)}")
                # Mark remaining files as failed
                for remaining_key in object_keys:
                    if remaining_key not in results:
                        results[remaining_key] = False
                break
        
        return results


# Singleton instance
r2_uploader = R2Uploader()