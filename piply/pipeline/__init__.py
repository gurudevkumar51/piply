"""Pipeline loading, graph helpers, and metadata expansion utilities."""

from __future__ import annotations

from piply.core.graph import dependents_map, downstream_closure, topological_order, upstream_closure

from .expander import (
    EntityItem,
    EntitySelection,
    ExpansionError,
    RuntimeTaskTemplate,
    entity_selections,
    expand_task_templates,
    merge_entity_maps,
    parse_entity_map,
)

__all__ = [
    "ConfigError",
    "EntityItem",
    "EntitySelection",
    "ExpansionError",
    "RuntimeTaskTemplate",
    "dependents_map",
    "discover_config",
    "downstream_closure",
    "entity_selections",
    "expand_task_templates",
    "load_project",
    "merge_entity_maps",
    "parse_entity_map",
    "topological_order",
    "upstream_closure",
]


def __getattr__(name: str):
    """Lazy-load loader symbols without creating import cycles."""
    if name in {"ConfigError", "discover_config", "load_project"}:
        from piply.core import loader

        return getattr(loader, name)
    raise AttributeError(name)
