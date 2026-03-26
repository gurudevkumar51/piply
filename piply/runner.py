from piply.parser.yaml_parser import load_pipeline
from piply.engine.prefect_engine import PrefectEngine
from piply.engine.local_engine import LocalEngine
from piply.utils.logging import setup_logger


def run_pipeline(file_path: str, engine_type: str = "prefect", tenant: str = None, retry_failed: bool = False):
    """
    Run a pipeline from a YAML file.

    Args:
        file_path: Path to the pipeline YAML configuration
        engine_type: Engine to use ('prefect' or 'local')
        tenant: Optional tenant to run for (overrides multi-tenant loop)
        retry_failed: If True, only retry failed steps

    Returns:
        Pipeline execution result
    """
    setup_logger(level="INFO")

    pipeline = load_pipeline(file_path)

    if engine_type.lower() == "prefect":
        engine = PrefectEngine()
    elif engine_type.lower() == "local":
        engine = LocalEngine()
    else:
        raise ValueError(
            f"Unknown engine: {engine_type}. Use 'prefect' or 'local'")

    pipeline.run(engine, tenant=tenant, retry_failed=retry_failed)
