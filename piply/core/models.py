from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field


class StepConfig(BaseModel):
    """Configuration for a single step."""
    name: str
    type: str
    function: Optional[str] = None
    command: Optional[str] = None
    depends_on: Optional[List[str]] = None
    retries: int = 0
    retry_delay: int = 60  # seconds
    timeout: Optional[int] = None
    # Additional fields for extensibility
    config: Dict[str, Any] = Field(default_factory=dict)


class TenantConfig(BaseModel):
    """Configuration for a tenant (multi-tenant execution)."""
    name: str
    variables: Dict[str, Any] = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    """Top-level pipeline configuration."""
    name: str
    steps: List[StepConfig]
    tenants: Optional[List[Union[str, TenantConfig]]] = None
    schedule: Optional[str] = None  # cron expression
    timeout: Optional[int] = None
    retries: int = 0
    variables: Dict[str, Any] = Field(default_factory=dict)

    def get_tenant_names(self) -> List[str]:
        """Extract tenant names from mixed string/TenantConfig list."""
        if not self.tenants:
            return []
        names = []
        for t in self.tenants:
            if isinstance(t, TenantConfig):
                names.append(t.name)
            else:
                names.append(t)
        return names


class PipelineYAML(BaseModel):
    """Root model for YAML file validation."""
    pipeline: PipelineConfig
