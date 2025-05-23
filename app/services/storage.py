import os
import boto3
from botocore.exceptions import ClientError
from typing import Dict, List, Optional
from pathlib import Path
from app.core.config import settings

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
        
        # Upload each format to its respective directory
        for format_name, file_info in files_info["formats"].items():
            file_path = file_info["path"]
            file_name = file_info["filename"]
            
            # Generate a unique key for the file
            # Format: format_name/timestamp_filename.extension
            import time
            timestamp = int(time.time())
            key = f"{format_name}/{timestamp}_{file_name}"
            
            try:
                # Upload the file to R2
                self.client.upload_file(
                    file_path,
                    self.bucket_name,
                    key,
                    ExtraArgs={
                        'ContentType': self._get_content_type(file_name)
                    }
                )
                
                # Generate the public URL
                url = f"{self.public_url}/{key}"
                
                # Update the result with the URL
                result["formats"][format_name] = {
                    "filename": file_name,
                    "size": file_info["size"],
                    "resolution": file_info["resolution"],
                    "url": url
                }
            except ClientError as e:
                print(f"Error uploading {file_name} to R2: {str(e)}")
        
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
        
        try:
            # List existing objects to check if directories exist
            existing_objects = self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Delimiter='/'
            )
            
            existing_prefixes = [prefix['Prefix'] for prefix in existing_objects.get('CommonPrefixes', [])]
            
            # Create directories that don't exist
            for directory in directories:
                prefix = f"{directory}/"
                if prefix not in existing_prefixes:
                    # Create an empty object with the directory name as the key
                    self.client.put_object(
                        Bucket=self.bucket_name,
                        Key=prefix,
                        Body=''
                    )
        except ClientError as e:
            print(f"Error ensuring directories exist: {str(e)}")
