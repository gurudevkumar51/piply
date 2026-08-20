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

from piply.api.auth import auth_middleware, security_headers_middleware
from piply.core.scheduler import PipelineScheduler
from piply.core.service import PipelineService
from piply.settings import PiplySettings, load_settings
from piply.version import get_version

from .routes import accounts, dashboard, execution, maintenance, observability, pipelines, runs, ui


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
    if current_settings.database_dsn is not None:
        env_database: str | None = current_settings.database_dsn
    elif current_settings.database_path is not None:
        env_database = str(current_settings.database_path)
    else:
        env_database = os.getenv("PIPLY_DATABASE")
    return PipelineService(
        config_path=env_config,
        database_path=env_database,
        settings=current_settings,
    )


def create_app(config_path: str | None = None) -> FastAPI:
    """Create the Piply FastAPI app with routes, templates, and scheduler."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings = load_settings(config_path or os.getenv("PIPLY_CONFIG"))
        service = _build_service(config_path, settings)
        scheduler = PipelineScheduler(service)
        app.state.service = service
        app.state.scheduler = scheduler
        app.state.settings = settings
        app.state.templates = Jinja2Templates(directory=str(_ui_directory() / "templates"))
        # Create the first admin when the install has no accounts yet. This is
        # how a server deployment gets its first login without shell access.
        bootstrap = service.bootstrap_admin()
        if bootstrap is not None:
            name, secret = bootstrap
            print("=" * 68, flush=True)
            print("Piply created an initial administrator account:", flush=True)
            print(f"    username: {name}", flush=True)
            if secret is None:
                # The operator supplied the password, so echoing it here would
                # only copy a known secret into the container's log stream.
                print("    password: (as configured)", flush=True)
            else:
                print(f"    password: {secret}", flush=True)
                print("Store it now. It cannot be shown again.", flush=True)
            print("=" * 68, flush=True)

        scheduler.start()

        async def _shutdown_watcher():
            while True:
                if service.store.get_meta("shutdown_requested") == "true":
                    service.store.set_meta("shutdown_requested", "false")
                    if os.name == "nt":
                        os.kill(os.getpid(), signal.CTRL_C_EVENT)
                    else:
                        os.kill(os.getpid(), signal.SIGINT)
                    break
                await asyncio.sleep(2)

        watcher_task = asyncio.create_task(_shutdown_watcher())

        yield

        watcher_task.cancel()
        service.shutdown_runtime("Run interrupted because the Piply service shut down.")
        scheduler.stop()

    app = FastAPI(
        title="Piply",
        description="Lightweight script orchestration with a modular runtime and professional UI.",
        version=get_version(),
        lifespan=lifespan,
    )

    # Registered through FastAPI's own middleware decorator so nothing here
    # imports Starlette directly, keeping the declared dependencies honest.
    app.middleware("http")(auth_middleware)
    # Added last so it wraps outermost, which means the hardening headers are
    # also present on the 401 and login-redirect responses the auth middleware
    # returns without ever reaching a route.
    app.middleware("http")(security_headers_middleware)
    app.mount("/static", StaticFiles(directory=str(_ui_directory() / "static")), name="static")
    app.include_router(accounts.router)
    app.include_router(ui.router)
    app.include_router(dashboard.router)
    app.include_router(execution.router)
    app.include_router(observability.router)
    # Registered before the pipeline/run routers so its explicit sub-paths are
    # not shadowed by their `{pipeline_id}` / `{run_id}` wildcards.
    app.include_router(maintenance.router)
    app.include_router(pipelines.router)
    app.include_router(runs.router)
    return app


def get_app():
    return create_app()


app = get_app()
