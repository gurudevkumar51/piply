"""Connection profiles and secrets helpers for Piply operators."""

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

import yaml

from piply.core.loader import discover_config
from piply.settings import load_settings


def get_profile(name: str) -> dict[str, Any]:
    """
    Retrieve a connection profile by name. 
    First checks the local SQLite database from PIPLY_DATABASE.
    Falls back to reading from profiles.yaml or profiles.yml in the workspace.
    """
    # 1. Check SQLite DB
    try:
        config_path = Path(os.environ.get("PIPLY_CONFIG") or discover_config())
        settings = load_settings(config_path)
        db_path = settings.database_path or (config_path.parent / ".piply" / "piply.db")
        if db_path.exists():
            with sqlite3.connect(db_path) as conn:
                row = conn.execute("SELECT value FROM meta WHERE key = ?", (f"profile:{name}",)).fetchone()
                if row:
                    return json.loads(row[0])
    except Exception:
        pass  # Silently fall back to file
        
    # 2. Check File-based (profiles.yaml)
    try:
        config_path = Path(os.environ.get("PIPLY_CONFIG") or discover_config())
        workspace = config_path.parent
        for candidate in ["profiles.yaml", "profiles.yml"]:
            prof_file = workspace / candidate
            if prof_file.exists():
                with prof_file.open("r", encoding="utf-8") as f:
                    profiles_data = yaml.safe_load(f)
                    if isinstance(profiles_data, dict) and name in profiles_data:
                        return dict(profiles_data[name])
    except Exception:
        pass
        
    raise KeyError(f"Profile '{name}' not found in database or profiles.yaml")
