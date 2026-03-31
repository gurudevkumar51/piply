"""FastAPI application factory for the Piply API and UI."""

from __future__ import annotations

import asyncio
import os
import signal
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from piply.api.auth import AuthMiddleware
from piply.core.scheduler import PipelineScheduler
from piply.core.service import PipelineService
from piply.settings import PiplySettings, load_settings

from .routes import dashboard, pipelines, runs, ui


def _ui_directory() -> Path:
    """Return the absolute path to the bundled UI assets."""
    return Path(__file__).resolve().parent.parent / "ui"


def _build_service(
    config_path: str | None = None,
    settings: PiplySettings | None = None,
) -> PipelineService:
    """Build the shared service instance from config and environment overrides."""
    current_settings = settings or load_settings(config_path or os.getenv("PIPLY_CONFIG"))
    env_config = config_path or (str(current_settings.config_path) if current_settings.config_path else None)
    env_database = (
        str(current_settings.database_path) if current_settings.database_path is not None else os.getenv("PIPLY_DATABASE")
    )
    return PipelineService(
        config_path=env_config,
        database_path=env_database,
        settings=current_settings,
    )


def create_app(config_path: str | None = None) -> FastAPI:
    """Create the Piply FastAPI app with routes, templates, and scheduler."""
    settings = load_settings(config_path or os.getenv("PIPLY_CONFIG"))
    service = _build_service(config_path, settings)
    scheduler = PipelineScheduler(service)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.service = service
        app.state.scheduler = scheduler
        app.state.settings = settings
        app.state.templates = Jinja2Templates(directory=str(_ui_directory() / "templates"))
        scheduler.start()
        
        async def _shutdown_watcher():
            while True:
                if service.store.get_meta("shutdown_requested") == "true":
                    service.store.set_meta("shutdown_requested", "false")
                    if os.name == 'nt':
                        os.kill(os.getpid(), signal.CTRL_C_EVENT)
                    else:
                        os.kill(os.getpid(), signal.SIGINT)
                    break
                await asyncio.sleep(2)
        
        watcher_task = asyncio.create_task(_shutdown_watcher())
        
        yield
        
        watcher_task.cancel()
        scheduler.stop()

    app = FastAPI(
        title="Piply",
        description="Lightweight script orchestration with a modular runtime and professional UI.",
        version="0.2.0",
        lifespan=lifespan,
    )

    app.add_middleware(AuthMiddleware)
    app.mount("/static", StaticFiles(directory=str(_ui_directory() / "static")), name="static")
    app.include_router(ui.router)
    app.include_router(dashboard.router)
    app.include_router(pipelines.router)
    app.include_router(runs.router)
    return app


app = create_app()
