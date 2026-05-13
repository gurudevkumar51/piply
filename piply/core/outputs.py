"""Task output serialization helpers used by the engine, store, and API."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


MAX_OUTPUT_PREVIEW_CHARS = 2_000
MAX_STORED_JSON_CHARS = 64_000


@dataclass(slots=True)
class SerializedTaskOutput:
    """A storage-friendly view of a task output value."""

    output_type: str
    preview: str
    is_json: bool
    json_value: str | None
    metadata: dict[str, Any] = field(default_factory=dict)
    size_bytes: int = 0


def _preview_text(value: Any) -> str:
    """Return a compact preview for logs and UI drawers."""
    if isinstance(value, str):
        preview = value
    else:
        try:
            preview = json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError):
            preview = repr(value)
    if len(preview) > MAX_OUTPUT_PREVIEW_CHARS:
        return f"{preview[:MAX_OUTPUT_PREVIEW_CHARS]}..."
    return preview


def _metadata_for(value: Any) -> dict[str, Any]:
    """Collect lightweight type metadata without importing heavy optional libraries."""
    metadata: dict[str, Any] = {
        "python_type": type(value).__name__,
        "python_module": type(value).__module__,
    }
    try:
        metadata["length"] = len(value)  # type: ignore[arg-type]
    except TypeError:
        pass

    shape = getattr(value, "shape", None)
    if shape is not None:
        try:
            metadata["shape"] = list(shape)
        except TypeError:
            metadata["shape"] = str(shape)

    columns = getattr(value, "columns", None)
    if columns is not None:
        try:
            metadata["columns"] = [str(item) for item in list(columns)[:30]]
        except TypeError:
            metadata["columns"] = str(columns)
    return metadata


def serialize_task_output(value: Any) -> SerializedTaskOutput:
    """Serialize one task output to a bounded JSON/preview representation."""
    metadata = _metadata_for(value)
    output_type = f"{type(value).__module__}.{type(value).__name__}"
    preview = _preview_text(value)
    json_value: str | None = None
    is_json = False

    try:
        rendered = json.dumps(value, ensure_ascii=False, sort_keys=True)
        is_json = True
        metadata["json_serializable"] = True
        metadata["json_stored"] = len(rendered) <= MAX_STORED_JSON_CHARS
        if metadata["json_stored"]:
            json_value = rendered
        size_bytes = len(rendered.encode("utf-8"))
    except (TypeError, ValueError):
        metadata["json_serializable"] = False
        metadata["json_stored"] = False
        size_bytes = len(preview.encode("utf-8"))

    return SerializedTaskOutput(
        output_type=output_type,
        preview=preview,
        is_json=is_json,
        json_value=json_value,
        metadata=metadata,
        size_bytes=size_bytes,
    )


def load_json_output(json_value: str | None) -> Any:
    """Decode a stored JSON task output when one is available."""
    if json_value is None:
        return None
    return json.loads(json_value)
