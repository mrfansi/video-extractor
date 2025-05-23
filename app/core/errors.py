from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from loguru import logger


class VideoProcessingError(Exception):
    """Exception raised for errors during video processing."""
    
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class FileUploadError(Exception):
    """Exception raised for errors during file upload."""
    
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class StorageError(Exception):
    """Exception raised for errors related to R2 storage."""
    
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class RequestNotFoundError(Exception):
    """Exception raised when a request ID is not found."""
    
    def __init__(self, request_id: str):
        self.message = f"Request ID {request_id} not found."
        super().__init__(self.message)


def setup_exception_handlers(app: FastAPI) -> None:
    """Configure exception handlers for the FastAPI application."""
    
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        """Handle validation errors."""
        logger.error(f"Validation error: {exc.errors()}")
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "status": "error",
                "message": "Validation error",
                "details": exc.errors(),
            },
        )
    
    @app.exception_handler(VideoProcessingError)
    async def video_processing_exception_handler(request: Request, exc: VideoProcessingError):
        """Handle video processing errors."""
        logger.error(f"Video processing error: {exc.message}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "message": exc.message},
        )
    
    @app.exception_handler(FileUploadError)
    async def file_upload_exception_handler(request: Request, exc: FileUploadError):
        """Handle file upload errors."""
        logger.error(f"File upload error: {exc.message}")
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"status": "error", "message": exc.message},
        )
    
    @app.exception_handler(StorageError)
    async def storage_exception_handler(request: Request, exc: StorageError):
        """Handle storage errors."""
        logger.error(f"Storage error: {exc.message}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "message": exc.message},
        )
    
    @app.exception_handler(RequestNotFoundError)
    async def request_not_found_exception_handler(request: Request, exc: RequestNotFoundError):
        """Handle request not found errors."""
        logger.warning(f"Request not found: {exc.message}")
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"status": "error", "message": exc.message},
        )
    
    @app.exception_handler(Exception)
    async def generic_exception_handler(request: Request, exc: Exception):
        """Handle any unhandled exceptions."""
        logger.exception(f"Unhandled exception: {str(exc)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status": "error",
                "message": "An unexpected error occurred",
            },
        )