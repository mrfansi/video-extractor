import os
import uvicorn
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

if __name__ == "__main__":
    # Get configuration from environment variables
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    workers = int(os.getenv("API_WORKERS", "4"))
    
    # Run the application with uvicorn
    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        workers=workers,
        reload=True,  # Enable auto-reload during development
        log_level="info",
    )