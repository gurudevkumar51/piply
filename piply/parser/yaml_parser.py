import yaml
from piply.core.pipeline import Pipeline
from piply.core.models import PipelineYAML
from piply.plugins.registry import get_step


def load_pipeline(path: str) -> Pipeline:
    """
    Load and validate a pipeline from a YAML file.

    Args:
        path: Path to the YAML configuration file

    Returns:
        Validated Pipeline object

    Raises:
        FileNotFoundError: If the file doesn't exist
        ValidationError: If the YAML doesn't match the schema
        ValueError: If step type is not registered
    """
    with open(path) as f:
        raw_config = yaml.safe_load(f)

    # Validate configuration
    config = PipelineYAML(**raw_config)
    pipeline_conf = config.pipeline

    # Build steps
    steps = []
    for step_conf in pipeline_conf.steps:
        step_cls = get_step(step_conf.type)
        step = step_cls(step_conf.name, step_conf.dict())
        steps.append(step)

    # Extract tenant names using the model method
    tenants = pipeline_conf.get_tenant_names()

    return Pipeline(
        name=pipeline_conf.name,
        steps=steps,
        tenants=tenants,
        config=pipeline_conf.dict()
    )
