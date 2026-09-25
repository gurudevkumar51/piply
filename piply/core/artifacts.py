"""Artifact discovery for files a task declares as its outputs.

Artifacts are not copied into a store. A task declares glob patterns and Piply
records where the produced files live, how large they are, and when they were
written, so the UI can browse and download them straight from disk.
"""

from __future__ import annotations

import mimetypes
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

MAX_ARTIFACTS_PER_TASK = 200


@dataclass(slots=True, frozen=True)
class ArtifactRecord:
    """One file produced by a task."""

    task_id: str
    name: str
    path: str
    size_bytes: int
    modified_at: datetime
    content_type: str

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-friendly payload."""
        return {
            "task_id": self.task_id,
            "name": self.name,
            "path": self.path,
            "size_bytes": self.size_bytes,
            "modified_at": self.modified_at.isoformat(),
            "content_type": self.content_type,
        }


def _iter_matches(base_dir: Path, pattern: str) -> list[Path]:
    """Resolve one declared artifact pattern relative to the task working directory."""
    candidate = Path(pattern)
    if candidate.is_absolute():
        root = candidate.parent
        glob = candidate.name
    else:
        root = base_dir
        glob = pattern
    try:
        if not any(character in glob for character in "*?["):
            single = (root / glob) if not Path(glob).is_absolute() else Path(glob)
            return [single] if single.is_file() else []
        return sorted(item for item in root.glob(glob) if item.is_file())
    except (OSError, ValueError):
        return []


def collect_task_artifacts(
    task_id: str,
    patterns: tuple[str, ...] | list[str],
    base_dir: Path | None,
) -> list[ArtifactRecord]:
    """Return every existing file matching the task's declared artifact patterns."""
    if not patterns:
        return []
    root = (base_dir or Path.cwd()).resolve()
    seen: set[Path] = set()
    records: list[ArtifactRecord] = []
    for pattern in patterns:
        for match in _iter_matches(root, str(pattern)):
            resolved = match.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            try:
                stat = resolved.stat()
            except OSError:
                continue
            content_type, _ = mimetypes.guess_type(resolved.name)
            records.append(
                ArtifactRecord(
                    task_id=task_id,
                    name=resolved.name,
                    path=str(resolved),
                    size_bytes=stat.st_size,
                    modified_at=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
                    content_type=content_type or "application/octet-stream",
                )
            )
            if len(records) >= MAX_ARTIFACTS_PER_TASK:
                return records
    return records


def is_readable_artifact(path: Path, allowed_roots: list[Path]) -> bool:
    """Return whether a requested download path sits inside an allowed root.

    Download requests carry a filesystem path, so this guards against a crafted
    path escaping the workspace or artifacts directory.
    """
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError):
        return False
    if not resolved.is_file():
        return False
    for root in allowed_roots:
        try:
            resolved.relative_to(root.resolve())
        except ValueError:
            continue
        return True
    return False
