"""
UI page routes (HTML pages).
"""
from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["ui"])


def get_templates(request: Request):
    """Dependency to get templates from app state."""
    return request.app.state.templates


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request, templates=Depends(get_templates)):
    """Main dashboard page."""
    return templates.TemplateResponse("dashboard.html", {"request": request})


@router.get("/pipelines", response_class=HTMLResponse)
def pipelines_page(request: Request, templates=Depends(get_templates)):
    """Pipelines listing page."""
    return templates.TemplateResponse("pipelines.html", {"request": request})


@router.get("/pipelines/{pipeline_name}", response_class=HTMLResponse)
def pipeline_detail_page(request: Request, pipeline_name: str, templates=Depends(get_templates)):
    """Pipeline detail page with DAG visualization."""
    return templates.TemplateResponse("pipeline_detail.html", {"request": request, "pipeline_name": pipeline_name})


@router.get("/runs/{run_id}", response_class=HTMLResponse)
def run_detail_page(request: Request, run_id: int, templates=Depends(get_templates)):
    """Run detail page with task status and logs."""
    return templates.TemplateResponse("run_detail.html", {"request": request, "run_id": run_id})
