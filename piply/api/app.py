"""FastAPI application factory for the Piply API and UI."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from piply.core.scheduler import PipelineScheduler
from piply.core.service import PipelineService

from .routes import dashboard, pipelines, runs, ui


def _ui_directory() -> Path:
    """Return the absolute path to the bundled UI assets."""
    return Path(__file__).resolve().parent.parent / "ui"


def _build_service(config_path: str | None = None) -> PipelineService:
    """Build the shared service instance from config and environment overrides."""
    env_config = config_path or os.getenv("PIPLY_CONFIG")
    env_database = os.getenv("PIPLY_DATABASE")
    return PipelineService(config_path=env_config, database_path=env_database)


def create_app(config_path: str | None = None) -> FastAPI:
    """Create the Piply FastAPI app with routes, templates, and scheduler."""
    service = _build_service(config_path)
    scheduler = PipelineScheduler(service)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.service = service
        app.state.scheduler = scheduler
        app.state.templates = Jinja2Templates(directory=str(_ui_directory() / "templates"))
        scheduler.start()
        yield
        scheduler.stop()

    app = FastAPI(
        title="Piply",
        description="Lightweight script orchestration with a modular runtime and professional UI.",
        version="0.2.0",
        lifespan=lifespan,
    )

    app.mount("/static", StaticFiles(directory=str(_ui_directory() / "static")), name="static")
    app.include_router(ui.router)
    app.include_router(dashboard.router)
    app.include_router(pipelines.router)
    app.include_router(runs.router)
    return app


app = create_app()
