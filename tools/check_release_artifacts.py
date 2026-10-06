"""Verify release archives against current source and reject accidental data."""

from __future__ import annotations

import argparse
import configparser
import hashlib
import json
import tarfile
import zipfile
from email.parser import BytesParser
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    wheels = list(args.directory.glob("mr_piply-0.3.4-*.whl"))
    sources = list(args.directory.glob("mr_piply-0.3.4.tar.gz"))
    assert len(wheels) == len(sources) == 1, "Expected exactly one wheel and source archive."
    expected = [
        path
        for path in (root / "piply").rglob("*")
        if path.is_file() and "__pycache__" not in path.parts and path.suffix not in {".pyc", ".pyo"}
    ]
    with zipfile.ZipFile(wheels[0]) as wheel:
        names = wheel.namelist()
        assert all(name.startswith(("piply/", "mr_piply-0.3.4.dist-info/")) for name in names), names
        for path in expected:
            name = path.relative_to(root).as_posix()
            assert wheel.read(name) == path.read_bytes(), f"Missing/stale wheel file: {name}"
        metadata = BytesParser().parsebytes(wheel.read("mr_piply-0.3.4.dist-info/METADATA"))
        assert metadata["Name"] == "mr-piply" and metadata["Version"] == "0.3.4"
        assert metadata["Requires-Python"] == ">=3.10"
        runtime = [value for value in metadata.get_all("Requires-Dist") if ";" not in value]
        assert len(runtime) == 8, runtime
        assert any(
            value.startswith("tzdata>=2024.1;") and 'sys_platform == "win32"' in value
            for value in metadata.get_all("Requires-Dist")
        ), "Windows timezone data must be installed with the wheel."
        entry_points = configparser.ConfigParser()
        entry_points.read_string(wheel.read("mr_piply-0.3.4.dist-info/entry_points.txt").decode())
        assert entry_points["console_scripts"]["piply"] == "piply.cli.main:app"
    with tarfile.open(sources[0], "r:gz") as source:
        names = source.getnames()
        forbidden = {".env", ".git", ".piply", "__pycache__", ".pytest_cache"}
        for name in names:
            parts = Path(name).parts
            assert not forbidden.intersection(parts), f"Private/generated path in source archive: {name}"
            assert Path(name).suffix not in {".db", ".sqlite", ".sqlite3", ".pyc", ".pyo"}, name
        docs = list((root / "docs").rglob("*.md")) + list(root.glob("*.md"))
        workflows = list((root / ".gitea" / "workflows").glob("*.yml"))
        checks = expected + docs + workflows + list((root / "tools").glob("*.py")) + list((root / "tests").glob("*.py"))
        for path in checks:
            name = "mr_piply-0.3.4/" + path.relative_to(root).as_posix()
            with source.extractfile(name) as file:
                assert file.read() == path.read_bytes(), f"Missing/stale source file: {name}"
    print(
        json.dumps(
            {
                "result": "passed",
                "wheel_files": len(expected),
                "documentation_files": len(docs),
                "workflow_files": len(workflows),
                "artifacts": {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in wheels + sources},
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
