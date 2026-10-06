"""Serve bundled UI assets with deterministic content types."""

from __future__ import annotations

import os
from pathlib import Path

from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

_UI_MEDIA_TYPES = {".js": "text/javascript", ".css": "text/css"}


class UIStaticFiles(StaticFiles):
    """Keep host MIME registrations from breaking scripts and styles."""

    def file_response(
        self,
        full_path: str | os.PathLike[str],
        stat_result: os.stat_result,
        scope: dict,
        status_code: int = 200,
    ) -> Response:
        response = super().file_response(full_path, stat_result, scope, status_code)
        media_type = _UI_MEDIA_TYPES.get(Path(full_path).suffix.lower())
        # Leave conditional responses and all StaticFiles safety checks intact.
        # Only these bundled types are fixed; artifact MIME guesses stay local.
        if isinstance(response, FileResponse) and media_type is not None:
            response.media_type = media_type
            response.headers["content-type"] = f"{media_type}; charset={response.charset}"
        return response
