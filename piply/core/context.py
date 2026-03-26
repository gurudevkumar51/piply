from typing import Dict, Any, Optional
from dataclasses import dataclass, field


@dataclass
class StepOutput:
    """Represents the output of a step execution."""
    step_name: str
    success: bool
    result: Any = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineContext:
    """Typed context for pipeline execution."""
    tenant: Optional[str] = None
    pipeline_name: Optional[str] = None
    step_outputs: Dict[str, StepOutput] = field(default_factory=dict)
    variables: Dict[str, Any] = field(default_factory=dict)

    def set_output(self, step_name: str, output: StepOutput):
        """Store step output in context."""
        self.step_outputs[step_name] = output

    def get_output(self, step_name: str) -> Optional[StepOutput]:
        """Retrieve output from a previous step."""
        return self.step_outputs.get(step_name)

    def set_variable(self, key: str, value: Any):
        """Set a pipeline variable."""
        self.variables[key] = value

    def get_variable(self, key: str, default: Any = None) -> Any:
        """Get a pipeline variable."""
        return self.variables.get(key, default)
