import os
import boto3
import time
from botocore.exceptions import ClientError
from typing import Dict, List, Optional
from pathlib import Path
from app.core.config import settings
from app.core.logging import logger, log_error, log_performance

class R2Storage:
    """Service for storing files in Cloudflare R2"""
    
    def __init__(self):
        self.endpoint_url = settings.r2_endpoint_url
        self.access_key_id = settings.r2_access_key_id
        self.secret_access_key = settings.r2_secret_access_key
        self.bucket_name = settings.r2_bucket_name
        self.public_url = settings.r2_public_url
        
        # Initialize S3 client (R2 uses S3-compatible API)
        self.client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name='auto'  # Cloudflare R2 uses 'auto' region
        )
    
    async def upload_files(self, files_info: Dict) -> Dict:
        """Upload files to R2 bucket
        
        Args:
            files_info: Dictionary with information about the files to upload
            
        Returns:
            Dictionary with updated file information including URLs
        """
        result = {
            "original": files_info["original"],
            "formats": {}
        }
        
        logger.info(f"Starting upload of {len(files_info['formats'])} files to R2 bucket {self.bucket_name}")
        
        # Upload each format to its respective directory
        for format_name, file_info in files_info["formats"].items():
            file_path = file_info["path"]
            file_name = file_info["filename"]
            file_size = file_info["size"]
            
            # Generate a unique key for the file
            # Format: format_name/timestamp_filename.extension
            timestamp = int(time.time())
            key = f"{format_name}/{timestamp}_{file_name}"
            
            logger.info(f"Uploading {format_name} file {file_name} ({file_size} bytes) to R2 bucket {self.bucket_name}")
            
            try:
                # Measure upload time
                upload_start = time.time()
                
                # Upload the file to R2
                self.client.upload_file(
                    file_path,
                    self.bucket_name,
                    key,
                    ExtraArgs={
                        'ContentType': self._get_content_type(file_name)
                    }
                )
                
                # Calculate upload time
                upload_time = time.time() - upload_start
                upload_speed = file_size / upload_time if upload_time > 0 else 0
                
                # Log upload performance
                log_performance(f"upload_{format_name}", upload_time * 1000, {
                    "file_size": file_size,
                    "format": format_name,
                    "upload_speed_bytes_per_sec": upload_speed
                })
                
                # Generate the public URL
                url = f"{self.public_url}/{key}"
                
                # Update the result with the URL
                result["formats"][format_name] = {
                    "filename": file_name,
                    "size": file_info["size"],
                    "resolution": file_info["resolution"],
                    "url": url
                }
                
                logger.info(f"Successfully uploaded {format_name} file to {url}")
            except ClientError as e:
                error_message = f"Error uploading {file_name} to R2: {str(e)}"
                log_error("r2_upload_error", error_message, {
                    "format": format_name,
                    "file_name": file_name,
                    "file_size": file_size,
                    "bucket": self.bucket_name,
                    "key": key
                })
                logger.error(error_message)
        
        logger.info(f"Completed upload of {len(result['formats'])} files to R2 bucket {self.bucket_name}")
        return result
    
    def _get_content_type(self, filename: str) -> str:
        """Get the content type for a file based on its extension
        
        Args:
            filename: Name of the file
            
        Returns:
            Content type string
        """
        extension = os.path.splitext(filename)[1].lower()
        content_types = {
            ".mp4": "video/mp4",
            ".webm": "video/webm",
            ".mov": "video/quicktime"
        }
        
        return content_types.get(extension, "application/octet-stream")
    
    async def ensure_directories_exist(self) -> None:
        """Ensure that the required directories exist in the R2 bucket"""
        directories = ["mp4", "webm", "mov"]
        
        logger.info(f"Ensuring directories exist in R2 bucket {self.bucket_name}")
        
        try:
            # Measure operation time
            start_time = time.time()
            
            # List existing objects to check if directories exist
            existing_objects = self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Delimiter='/'
            )
            
            existing_prefixes = [prefix['Prefix'] for prefix in existing_objects.get('CommonPrefixes', [])]
            logger.debug(f"Existing prefixes in bucket: {existing_prefixes}")
            
            # Create directories that don't exist
            created_dirs = []
            for directory in directories:
                prefix = f"{directory}/"
                if prefix not in existing_prefixes:
                    logger.info(f"Creating directory {prefix} in R2 bucket {self.bucket_name}")
                    # Create an empty object with the directory name as the key
                    self.client.put_object(
                        Bucket=self.bucket_name,
                        Key=prefix,
                        Body=''
                    )
                    created_dirs.append(prefix)
            
            # Calculate operation time
            operation_time = time.time() - start_time
            
            # Log performance
            log_performance("ensure_r2_directories", operation_time * 1000, {
                "bucket": self.bucket_name,
                "directories": directories,
                "created_directories": created_dirs
            })
            
            logger.info(f"Directory check completed for R2 bucket {self.bucket_name}")
        except ClientError as e:
            error_message = f"Error ensuring directories exist: {str(e)}"
            log_error("r2_directory_error", error_message, {
                "bucket": self.bucket_name,
                "directories": directories
            })
            logger.error(error_message)
