#!/usr/bin/env python
from __future__ import annotations

import uvicorn


if __name__ == "__main__":
    uvicorn.run("piply.api.app:create_app", factory=True, host="127.0.0.1", port=8000, reload=True)
