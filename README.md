👉 # Piply - YAML-driven Workflow Orchestration Framework

A lightweight, low-code workflow orchestration framework built on top of Prefect.

## Features

- **YAML-driven pipeline definition** - No code required for basic pipelines
- **DAG support** - Define task dependencies
- **Multi-tenant execution** - Run the same pipeline for multiple tenants
- **Multiple step types**:
  - Python functions (from modules or files)
  - Shell/CLI commands
  - dbt workflows
- **Retry mechanisms** - Automatic retries with configurable delays
- **Two execution engines**:
  - Prefect (default) - Full-featured with scheduling, monitoring
  - Local - Simple sequential execution for testing
- **CLI interface** - Easy command-line operations
- **Extensible** - Plugin architecture for custom step types

## Installation

```bash
pip install -e .
```

## Quick Start

### 1. Initialize a project

```bash
piply init --name my_pipeline
```

This creates:
- `piply.yaml` - Pipeline configuration
- `pipelines/` directory with example Python functions

### 2. Edit the pipeline configuration

Edit `piply.yaml` to customize your pipeline:

```yaml
pipeline:
  name: my_pipeline
  schedule: "0 0 * * *"  # Optional: daily at midnight
  retries: 1
  variables:
    environment: "dev"

  steps:
    - name: extract
      type: python
      function: "pipelines.extract.main"
      retries: 2

    - name: transform
      type: shell
      command: "python transform.py"
      depends_on:
        - extract

    - name: load
      type: dbt
      command: "run"
      models:
        - "staging"
      depends_on:
        - transform
```

### 3. Run the pipeline

```bash
# Run with Prefect (default)
piply run

# Run with local engine (for testing)
piply run --engine local

# Run for a specific tenant
piply run --tenant tenant1

# Retry only failed steps
piply run --retry-failed
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `piply init` | Create a new pipeline configuration |
| `piply run` | Run the pipeline |
| `piply list dags` | List all available pipelines |
| `piply test` | Test pipeline execution |
| `piply tasks list` | List tasks in a pipeline |
| `piply tasks run` | Run specific tasks |
| `piply logs` | View execution logs || `piply ui` | Start the web UI server (default: http://localhost:8000) |
## Pipeline Configuration Reference

### Top-level fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Pipeline name (required) |
| `schedule` | string | Cron expression for scheduling (optional) |
| `retries` | integer | Number of pipeline-level retries (default: 0) |
| `timeout` | integer | Pipeline timeout in seconds (optional) |
| `tenants` | list | List of tenant identifiers for multi-tenant execution |
| `variables` | dict | Global variables accessible in all steps |

### Step configuration

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Step name (required) |
| `type` | string | Step type: `python`, `shell`, or `dbt` (required) |
| `depends_on` | list | List of step names this step depends on |
| `retries` | integer | Number of retry attempts (default: 0) |
| `retry_delay` | integer | Delay between retries in seconds (default: 60) |
| `timeout` | integer | Step timeout in seconds (optional) |

### Python step specific

| Field | Type | Description |
|-------|------|-------------|
| `function` | string | Function to execute. Format: `module.function` or `file.py:function` |

The function receives a `PipelineContext` object as its first argument (if it accepts parameters).

Example:
```python
# In file: pipelines/extract.py
def main(context):
    tenant = context.tenant
    # Do work...
    return {"rows": 100}
```

### Shell step specific

| Field | Type | Description |
|-------|------|-------------|
| `command` | string | Shell command to execute |

The command runs in a subprocess with output captured.

### dbt step specific

| Field | Type | Description |
|-------|------|-------------|
| `command` | string | dbt command: `run`, `test`, `seed`, etc. (default: `run`) |
| `models` | list | List of dbt models to run |
| `select` | string | dbt selection criteria |
| `vars` | dict | Variables to pass to dbt |
| `profiles_dir` | string | Path to profiles directory |
| `project_dir` | string | Path to dbt project directory |

## Multi-tenant Execution

Define tenants in your pipeline:

```yaml
pipeline:
  name: etl_pipeline
  tenants:
    - client_a
    - client_b
    - client_c
  steps:
    - name: extract
      type: python
      function: "extract.main"
```

The pipeline will run once for each tenant, with `context.tenant` set accordingly.

Run for a specific tenant:
```bash
piply run --tenant client_a
```

## Dependencies and DAGs

Define step dependencies to create a directed acyclic graph (DAG):

```yaml
steps:
  - name: extract
    type: python
    function: "extract.main"

  - name: transform
    type: shell
    command: "python transform.py"
    depends_on:
      - extract

  - name: load
    type: dbt
    command: "run"
    depends_on:
      - transform
```

Steps execute in dependency order. Independent steps can run in parallel (when using Prefect engine).

## Retry Mechanisms

Configure retries at both pipeline and step level:

```yaml
pipeline:
  name: robust_pipeline
  retries: 1  # Retry entire pipeline once if any step fails

  steps:
    - name: flaky_step
      type: shell
      command: "unreliable_command"
      retries: 3  # Retry this step up to 3 times
      retry_delay: 120  # Wait 2 minutes between retries
```

Retry failed steps only:
```bash
piply run --retry-failed
```

## Execution Engines

### Prefect (default)

Uses Prefect for orchestration. Provides:
- Flow visualization
- Scheduling and deployments
- Error handling and retries
- Monitoring dashboard

Requires: `prefect` installed (included in dependencies)

### Local

Simple sequential execution. Useful for:
- Testing
- Development
- Environments without Prefect

```bash
piply run --engine local
```

## Scheduling

For production scheduling, use Prefect deployments:

```bash
# Create a deployment
prefect deployment build piply.run:run_pipeline -n my_deployment

# Apply and schedule
prefect deployment apply my_deployment
```

Alternatively, use the `schedule` field in YAML with a cron expression. Piply will validate the format but actual scheduling is handled by the execution engine.

## Project Structure

```
my_project/
├── piply.yaml           # Pipeline configuration
├── pipelines/           # Python modules for steps
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── dbt/                 # Optional: dbt project
│   ├── models/
│   └── dbt_project.yml
└── data/               # Data files (optional)
```

## Extending with Custom Steps

Create custom step types by inheriting from `Step`:

```python
from piply.core.step import Step

class MyCustomStep(Step):
    def _execute(self, context):
        # Your custom logic
        return {"result": "success"}

# Register in plugins/__init__.py
from piply.plugins.registry import register_step
register_step("my_custom", MyCustomStep)
```

## Development

### Running Tests

```bash
pytest tests/
```

### Code Style

```bash
black piply/
ruff piply/
```

## License

MIT

## Contributing

Contributions welcome! Please open an issue or PR.