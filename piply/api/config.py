"""
Configuration settings.
"""
import os
from dataclasses import dataclass


@dataclass
class Settings:
    """Application settings."""
    database_url: str = os.getenv("PIPLY_DATABASE_URL", "sqlite:///./piply.db")
    pipeline_search_paths: list = None

    def __post_init__(self):
        if self.pipeline_search_paths is None:
            self.pipeline_search_paths = [".", "examples"]


settings = Settings()
