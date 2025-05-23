import os
from pathlib import Path
from typing import Dict, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from loguru import logger

from app.core.config import settings
from app.core.errors import StorageError


class R2Uploader:
    """Service for uploading files to Cloudflare R2."""
    
    def __init__(self):
        """Initialize the R2 client."""
        self.s3_client = boto3.client(
            's3',
            endpoint_url=settings.R2_ENDPOINT_URL,
            aws_access_key_id=settings.R2_ACCESS_KEY_ID,
            aws_secret_access_key=settings.R2_SECRET_ACCESS_KEY,
        )
        self.bucket_name = settings.R2_BUCKET_NAME
        self.public_url = settings.R2_PUBLIC_URL
        
        # Ensure bucket exists
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self) -> None:
        """Ensure the bucket exists, create if it doesn't."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket {self.bucket_name} exists")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == '404':
                logger.info(f"Bucket {self.bucket_name} does not exist. Creating...")
                try:
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                    logger.info(f"Bucket {self.bucket_name} created successfully")
                except Exception as create_error:
                    logger.error(f"Failed to create bucket: {str(create_error)}")
                    raise StorageError(f"Failed to create bucket: {str(create_error)}")
            else:
                logger.error(f"Error checking bucket: {str(e)}")
                raise StorageError(f"Error checking bucket: {str(e)}")
    
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
    
    def upload_file(
        self, file_path: str, object_key: Optional[str] = None
    ) -> Tuple[str, float]:
        """
        Upload a file to R2 storage.
        
        Args:
            file_path: Path to the file to upload
            object_key: Custom object key, if None, the file name will be used
            
        Returns:
            Tuple containing public URL and file size in MB
        """
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
    
    def delete_file(self, object_key: str) -> bool:
        """
        Delete a file from R2 storage.
        
        Args:
            object_key: Object key to delete
            
        Returns:
            True if deletion was successful, False otherwise
        """
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
        """
        results = {}
        
        for key in object_keys:
            results[key] = self.delete_file(key)
        
        return results


# Singleton instance
r2_uploader = R2Uploader()