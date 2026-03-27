# Piply Web UI & API Guide

## Overview

Piply includes a lightweight web UI built with FastAPI, SQLAlchemy, and Bootstrap. The UI provides:

- **Dashboard** - Overview of pipelines, runs, and tenants
- **Pipeline DAG Visualization** - Interactive DAG view with dependencies
- **Run Management** - Trigger, monitor, and retry pipeline runs
- **Task Details** - View task status, logs, and execution details
- **Tenant Views** - Filter and view executions by tenant
- **Retry with Resume/Startover** - Smart retry options

## Starting the UI Server

```bash
# Start the UI server (default: http://localhost:8000)
piply ui

# Custom host and port
piply ui --host 0.0.0.0 --port 8080

# Enable auto-reload for development
piply ui --reload
```

Or directly:
```bash
python run_api.py
```

## API Endpoints

### Dashboard
- `GET /api/dashboard` - Dashboard statistics
- `GET /` - Web UI dashboard

### Pipelines
- `GET /api/pipelines` - List all pipelines
- `GET /api/pipelines/{name}` - Get pipeline details
- `GET /pipelines` - Web UI: Pipelines listing
- `GET /pipelines/{name}` - Web UI: Pipeline detail with DAG

### Runs
- `POST /api/runs` - Create and trigger a new run
- `GET /api/runs` - List runs (filters: pipeline, tenant, status)
- `GET /api/runs/{id}` - Get run details
- `POST /api/runs/{id}/retry` - Retry a failed run
- `GET /runs` - Web UI: Runs listing
- `GET /runs/{id}` - Web UI: Run detail page

### Tasks
- `GET /api/runs/{id}/tasks` - List tasks in a run
- `GET /api/runs/{id}/tasks/{task_name}/logs` - Get task logs

### Tenants
- `GET /api/tenants` - List tenants with statistics

## API Request/Response Examples

### Create a Pipeline Run

```bash
curl -X POST "http://localhost:8000/api/runs" \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_name": "ecw_pipeline",
    "tenant": "practice1",
    "trigger_type": "manual"
  }'
```

Response:
```json
{
  "id": 1,
  "pipeline_name": "ecw_pipeline",
  "tenant": "practice1",
  "status": "pending",
  "started_at": "2026-03-26T13:18:36.079624",
  "completed_at": null,
  "trigger_type": "manual",
  "task_count": 0,
  "tasks_completed": 0
}
```

### Retry a Failed Run

```bash
curl -X POST "http://localhost:8000/api/runs/1/retry" \
  -H "Content-Type: application/json" \
  -d '{
    "run_id": 1,
    "mode": "resume",
    "tenant": "practice1"
  }'
```

Modes:
- `resume` - Continue from failed tasks only (skips successful tasks from previous run)
- `startover` - Run all tasks from the beginning

### Get Dashboard Stats

```bash
curl "http://localhost:8000/api/dashboard"
```

Response:
```json
{
  "total_pipelines": 5,
  "total_runs": 150,
  "running": 3,
  "success": 120,
  "failed": 30,
  "recent_runs": [...],
  "tenant_summary": [...]
}
```

## UI Features

### Dashboard
- Summary cards: Total Pipelines, Total Runs, Running, Success Rate
- Recent runs table with status badges
- Tenant summary with success rates
- Quick action buttons to run pipelines

### Pipeline Detail Page
- **DAG Visualization** - Interactive graph showing task dependencies
  - Toggle between Dagre and Tree layouts
  - Color-coded nodes by status (success/failed/running)
  - Hover effects and smooth transitions
- Pipeline configuration overview
- Recent runs for this pipeline
- "Run Pipeline" button with tenant selection

### Run Detail Page
- Run overview: status, duration, trigger type
- Task execution table with:
  - Task name and status
  - Start/completion times
  - Attempt number
  - View logs button
  - Retry button (for failed tasks)
- Logs viewer with syntax highlighting
- Auto-refresh for running pipelines (every 30s)

### Retry Modal
- Choose between **Resume** and **Start Over**:
  - **Resume**: Only runs tasks that failed in the original run. Successful tasks are skipped and their outputs are restored from the previous run.
  - **Start Over**: Runs all tasks from the beginning, ignoring previous results.
- Option to specify a different tenant

## Database

The UI uses SQLite by default (`piply.db`). You can configure a different database:

```bash
export PIPLY_DATABASE_URL="postgresql://user:pass@localhost/piply"
piply ui
```

Tables:
- `pipeline_runs` - Pipeline execution records
- `task_runs` - Individual task executions
- `logs` - Structured log entries

## Integration with Existing Piply

The UI works alongside the existing CLI and runner:

1. **CLI** (`piply run`) - Direct execution without UI tracking
2. **UI API** - Tracks all runs in database, provides REST API
3. **Runner** (`piply/runner.py`) - Can be used programmatically

When you trigger a run via the UI:
1. A `PipelineRun` record is created in the database
2. The pipeline executes in a background thread
3. Task executions are tracked in `TaskRun` records
4. Logs are captured and stored
5. UI auto-refreshes to show progress

## Retry Logic Details

### Resume Mode
- Creates a new `PipelineRun` linked to the original
- Pre-populates the pipeline context with outputs from successful tasks in the original run
- The `retry_failed=True` flag is passed to `pipeline.run()`
- Steps check `context.get_output(step_name)` and skip if successful
- Only failed steps are executed

### Start Over Mode
- Creates a new `PipelineRun` without any pre-populated context
- Equivalent to a fresh run
- All steps execute regardless of previous outcomes

## Customization

### Adding New Step Types
1. Create step class in `piply/plugins/yourtype/step.py`
2. Inherit from `Step` and implement `_execute()`
3. Register in `piply/plugins/__init__.py`:
   ```python
   from piply.plugins.registry import register_step
   register_step("yourtype", YourStepClass)
   ```

### Styling
- Custom CSS: `piply/ui/static/styles.css`
- Templates: `piply/ui/templates/`
- Uses Bootstrap 5.3 + custom theme

### Extending the API
Add new endpoints in `piply/api/main.py`:

```python
@app.get("/api/custom-endpoint")
def custom_endpoint(db: Session = Depends(get_db)):
    # Your logic
    return {"result": "data"}
```

## Troubleshooting

### Database Errors
If you see database errors, initialize the database:
```bash
# The database auto-initializes on first run
# To reset:
rm piply.db
piply ui
```

### Port Already in Use
Change the port:
```bash
piply ui --port 8081
```

### Pipeline Not Found
The API looks for YAML files in:
- Current working directory
- `examples/` subdirectory
- Any path specified in the pipeline config

Ensure your pipeline YAML is in one of these locations.

### Dependencies Missing
Install all dependencies:
```bash
pip install -e ".[dev]"
```

Required for UI:
- fastapi
- uvicorn[standard]
- sqlalchemy
- alembic
- jinja2
- python-multipart

## Production Considerations

For production deployments:

1. **Use a proper database** (PostgreSQL/MySQL instead of SQLite)
   ```bash
   export PIPLY_DATABASE_URL="postgresql://user:pass@host/db"
   ```

2. **Use a production ASGI server** (Gunicorn with Uvicorn workers)
   ```bash
   gunicorn piply.api.main:app -w 4 -k uvicorn.workers.UvicornWorker -b :8000
   ```

3. **Add authentication** - Currently the API is open. Add middleware for auth.

4. **Enable HTTPS** - Use a reverse proxy (nginx, Traefik) with TLS.

5. **Persistent storage** - Ensure database and file storage are persistent.

6. **Monitoring** - Set up logging to files, add metrics endpoint.

7. **Scheduling** - Use Prefect deployments or external scheduler for production pipelines.

## Architecture

```
┌─────────────────┐
│   FastAPI App   │
├─────────────────┤
│   REST API      │◄── External clients, curl, etc.
├─────────────────┤
│   Jinja2        │◄── Web UI templates
│   Templates     │
├─────────────────┤
│   SQLAlchemy    │◄── Database ORM
│   ORM            │
├─────────────────┤
│   Background    │◄── Async task execution
│   Tasks          │
└─────────────────┘
         │
         ▼
┌─────────────────┐
│  Piply Core     │
│  (Pipeline,     │
│   Engine, etc.) │
└─────────────────┘
```

## Future Enhancements

- User authentication and authorization
- Pipeline editor (YAML editor with validation)
- Real-time logs via WebSocket
- Advanced DAG customization (zoom, pan, node details)
- Pipeline versioning
- Integration with Prefect Cloud/Server
- Email/Slack notifications
- Metrics and alerting
- Multi-user support
- RBAC (Role-Based Access Control)

## Support

For issues, feature requests, or questions, please open an issue on GitHub.