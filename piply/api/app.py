"""
FastAPI application factory and main entry point.
"""
import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .db import init_db
from .routes import pipelines, runs, dashboard, ui
from .utils.pipeline_finder import PipelineDiscovery


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""

    # Initialize FastAPI
    app = FastAPI(
        title="Piply API",
        description="REST API for Piply pipeline orchestration",
        version="0.1.0"
    )

    # Setup UI paths
    current_dir = os.path.dirname(os.path.abspath(__file__))
    ui_dir = os.path.join(os.path.dirname(current_dir), "ui")
    templates_dir = os.path.abspath(os.path.join(ui_dir, "templates"))
    static_dir = os.path.abspath(os.path.join(ui_dir, "static"))

    # Ensure directories exist
    os.makedirs(templates_dir, exist_ok=True)
    os.makedirs(static_dir, exist_ok=True)

    # Mount static files
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    # Setup Jinja2 templates with custom environment to avoid compatibility issues
    from jinja2 import Environment, FileSystemLoader

    jinja_env = Environment(
        loader=FileSystemLoader(templates_dir),
        autoescape=True,
        cache_size=0  # Disable caching to avoid Jinja2 3.1.6 issues
    )

    class CustomTemplates:
        def __init__(self, env):
            self.env = env

        def TemplateResponse(self, name, context, **kwargs):
            from starlette.templating import _TemplateResponse
            template = self.env.get_template(name)
            return _TemplateResponse(template, context, **kwargs)

    # Make templates available to routes
    app.state.templates = CustomTemplates(jinja_env)

    # Include routers
    app.include_router(pipelines.router)
    app.include_router(runs.router)
    app.include_router(dashboard.router)
    app.include_router(ui.router)

    # Startup event
    @app.on_event("startup")
    def startup_event():
        init_db()

    # Exception handlers
    @app.exception_handler(HTTPException)
    def http_exception_handler(request: Request, exc: HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail}
        )

    return app


# Create the app instance
app = create_app()
