#!/usr/bin/env python
"""
Standalone script to run the Piply API server.
"""
import uvicorn
from piply.api.main import app

if __name__ == "__main__":
    uvicorn.run(
        "piply.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
