import logging
import sys
from typing import Any, Dict, List, Optional

from loguru import logger
from pydantic import BaseSettings


class LogConfig(BaseSettings):
    """Logging configuration"""
    
    # Logging level
    LEVEL: str = "INFO"
    
    # Logging format
    FORMAT: str = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )
    
    # Whether to serialize the log message to JSON
    JSON_LOGS: bool = False


class InterceptHandler(logging.Handler):
    """
    Intercept handler to route standard logging to loguru.
    """
    
    def emit(self, record: logging.LogRecord) -> None:
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logging() -> None:
    """Configure logging with loguru."""
    log_config = LogConfig()
    
    # Remove default configuration
    logger.remove()
    
    # Add custom configuration
    logger.add(
        sys.stdout,
        enqueue=True,
        backtrace=True,
        level=log_config.LEVEL,
        format=log_config.FORMAT,
        serialize=log_config.JSON_LOGS,
    )
    
    # Add file logging if needed
    logger.add(
        "logs/video-extractor.log",
        rotation="10 MB",
        retention="1 week",
        enqueue=True,
        backtrace=True,
        level=log_config.LEVEL,
        format=log_config.FORMAT,
        serialize=log_config.JSON_LOGS,
    )
    
    # Intercept standard logging
    logging.basicConfig(handlers=[InterceptHandler()], level=0)
    
    # Update root logger
    logging.getLogger("uvicorn").handlers = [InterceptHandler()]
    
    # Replace logging handlers for commonly used libraries
    for logger_name in [
        "uvicorn",
        "uvicorn.error",
        "fastapi",
        "gunicorn",
        "gunicorn.error",
        "gunicorn.access",
    ]:
        logging_logger = logging.getLogger(logger_name)
        logging_logger.handlers = [InterceptHandler()]

    # Done
    logger.info("Logging configuration completed")