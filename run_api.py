#!/usr/bin/env python
"""
Standalone script to run the Piply API server.
"""
import uvicorn
from piply.api.app import app

if __name__ == "__main__":
    uvicorn.run(
        "piply.api.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
