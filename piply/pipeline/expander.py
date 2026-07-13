"""Metadata-driven task expansion for entity-mapped Piply pipelines."""

from __future__ import annotations

import copy
import itertools
import re
from dataclasses import dataclass, field
from typing import Any


class ExpansionError(ValueError):
    """Raised when entity-driven task expansion is invalid."""


@dataclass(slots=True, frozen=True)
class EntityItem:
    """One configured value for an entity dimension."""

    entity: str
    key: str
    value: str
    variables: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class EntitySelection:
    """One runtime entity selection across one or more dimensions."""

    key: str
    values: dict[str, str]
    variables: dict[str, str]


@dataclass(slots=True, frozen=True)
class RuntimeTaskTemplate:
    """One raw task template materialized into a runtime task id."""

    template_id: str
    runtime_id: str
    raw_task: dict[str, Any]
    entity_key: str | None = None
    entity_values: dict[str, str] = field(default_factory=dict)
    variables: dict[str, str] = field(default_factory=dict)


EntityMap = dict[str, tuple[EntityItem, ...]]

_UNSAFE_FRAGMENT_PATTERN = re.compile(r"[^A-Za-z0-9_-]+")


def slug_fragment(value: object) -> str:
    """Return a stable id fragment for one entity value."""
    slug = _UNSAFE_FRAGMENT_PATTERN.sub("-", str(value).strip()).strip("-")
    return slug or "item"


def _entity_item_from_scalar(entity_name: str, raw_value: object) -> EntityItem:
    value = str(raw_value)
    return EntityItem(
        entity=entity_name,
        key=slug_fragment(value),
        value=value,
        variables={
            entity_name: value,
            f"{entity_name}_value": value,
        },
    )


def _entity_item_from_mapping(entity_name: str, raw_key: object, raw_value: Any) -> EntityItem:
    if isinstance(raw_value, dict):
        variables = {str(key): str(value) for key, value in raw_value.items() if value is not None}
        value = str(
            raw_value.get(entity_name)
            or raw_value.get("value")
            or raw_value.get("name")
            or raw_value.get("id")
            or raw_key
        )
        variables.setdefault(entity_name, value)
        variables.setdefault(f"{entity_name}_value", value)
        return EntityItem(entity=entity_name, key=slug_fragment(raw_key), value=value, variables=variables)
    item = _entity_item_from_scalar(entity_name, raw_value)
    return EntityItem(entity=entity_name, key=slug_fragment(raw_key), value=item.value, variables=item.variables)


def parse_entity_map(raw_value: Any, label: str) -> EntityMap:
    """Parse a YAML ``entities`` block into normalized entity items."""
    if raw_value in (None, "", False):
        return {}
    if not isinstance(raw_value, dict):
        raise ExpansionError(f"{label} must be a mapping")

    entities: EntityMap = {}
    for raw_entity_name, raw_items in raw_value.items():
        entity_name = str(raw_entity_name)
        if not entity_name:
            raise ExpansionError(f"{label} contains an empty entity name")

        if isinstance(raw_items, dict) and "values" in raw_items:
            raw_items = raw_items["values"]

        items: list[EntityItem] = []
        if isinstance(raw_items, dict):
            for raw_key, raw_item in raw_items.items():
                items.append(_entity_item_from_mapping(entity_name, raw_key, raw_item))
        elif isinstance(raw_items, list):
            for raw_item in raw_items:
                if isinstance(raw_item, dict):
                    raw_key = raw_item.get("id") or raw_item.get("name") or raw_item.get("value") or raw_item.get(
                        entity_name
                    )
                    if raw_key is None:
                        raise ExpansionError(f"{label}.{entity_name} mapping items need id, name, value, or {entity_name}")
                    items.append(_entity_item_from_mapping(entity_name, raw_key, raw_item))
                else:
                    items.append(_entity_item_from_scalar(entity_name, raw_item))
        else:
            items.append(_entity_item_from_scalar(entity_name, raw_items))

        if items:
            entities[entity_name] = tuple(items)

    return entities


def merge_entity_maps(*entity_maps: EntityMap) -> EntityMap:
    """Merge entity maps where later maps override matching dimensions."""
    merged: EntityMap = {}
    for entity_map in entity_maps:
        for entity_name, items in entity_map.items():
            if items:
                merged[entity_name] = items
    return merged


def entity_selections(entity_map: EntityMap) -> tuple[EntitySelection, ...]:
    """Return the cartesian entity selections for a map/matrix."""
    if not entity_map:
        return ()

    names = list(entity_map)
    selections: list[EntitySelection] = []
    for selected_items in itertools.product(*(entity_map[name] for name in names)):
        key = ".".join(item.key for item in selected_items)
        values = {item.entity: item.value for item in selected_items}
        variables: dict[str, str] = {}
        for item in selected_items:
            variables.update(item.variables)
        variables["entity_key"] = key
        selections.append(EntitySelection(key=key, values=values, variables=variables))
    return tuple(selections)


def _without_entities(raw_task: dict[str, Any]) -> dict[str, Any]:
    runtime_task = copy.deepcopy(raw_task)
    runtime_task.pop("entities", None)
    return runtime_task


def _task_entity_map(
    template_id: str,
    raw_task: dict[str, Any],
    pipeline_entities: EntityMap,
    task_entities: dict[str, EntityMap],
) -> EntityMap:
    if "entities" not in raw_task:
        return pipeline_entities
    if raw_task.get("entities") in (None, "", False) or raw_task.get("entities") == {}:
        return {}
    return merge_entity_maps(pipeline_entities, task_entities.get(template_id, {}))


def _runtime_title(template_id: str, raw_task: dict[str, Any], selection: EntitySelection | None) -> None:
    if selection is None or raw_task.get("title") is not None or raw_task.get("name") is not None:
        return
    human_task = template_id.replace("_", " ").replace("-", " ").title()
    raw_task["title"] = f"{selection.key} / {human_task}"


def expand_task_templates(
    raw_tasks: dict[str, Any],
    *,
    pipeline_entities: EntityMap,
    task_entities: dict[str, EntityMap] | None = None,
) -> tuple[RuntimeTaskTemplate, ...]:
    """Expand template tasks into runtime tasks and rewrite dependencies."""
    task_entities = task_entities or {}
    specs: list[RuntimeTaskTemplate] = []

    for template_id, raw_task in raw_tasks.items():
        if not isinstance(raw_task, dict):
            raise ExpansionError(f"Task '{template_id}' must be a mapping")

        selections = entity_selections(_task_entity_map(template_id, raw_task, pipeline_entities, task_entities))
        if not selections:
            runtime_task = _without_entities(raw_task)
            specs.append(RuntimeTaskTemplate(template_id=template_id, runtime_id=template_id, raw_task=runtime_task))
            continue

        for selection in selections:
            runtime_task = _without_entities(raw_task)
            _runtime_title(template_id, runtime_task, selection)
            specs.append(
                RuntimeTaskTemplate(
                    template_id=template_id,
                    runtime_id=f"{selection.key}.{template_id}",
                    raw_task=runtime_task,
                    entity_key=selection.key,
                    entity_values=selection.values,
                    variables=selection.variables,
                )
            )

    by_template: dict[str, dict[str, str]] = {}
    seen_runtime_ids: set[str] = set()
    for spec in specs:
        if spec.runtime_id in seen_runtime_ids:
            raise ExpansionError(f"Entity expansion produced duplicate runtime task id '{spec.runtime_id}'")
        seen_runtime_ids.add(spec.runtime_id)
        by_template.setdefault(spec.template_id, {})[spec.entity_key or ""] = spec.runtime_id

    rewritten: list[RuntimeTaskTemplate] = []
    for spec in specs:
        raw_task = copy.deepcopy(spec.raw_task)
        dependencies = [str(item) for item in raw_task.get("depends_on") or []]
        rewritten_dependencies: list[str] = []
        for dependency in dependencies:
            dependency_map = by_template.get(dependency)
            if not dependency_map:
                rewritten_dependencies.append(dependency)
                continue
            dependency_ids: list[str]
            current_key = spec.entity_key or ""
            if current_key and current_key in dependency_map:
                dependency_ids = [dependency_map[current_key]]
            elif "" in dependency_map:
                dependency_ids = [dependency_map[""]]
            else:
                dependency_ids = list(dependency_map.values())
            for dependency_id in dependency_ids:
                if dependency_id not in rewritten_dependencies:
                    rewritten_dependencies.append(dependency_id)
        if rewritten_dependencies:
            raw_task["depends_on"] = rewritten_dependencies
        else:
            raw_task.pop("depends_on", None)
        rewritten.append(
            RuntimeTaskTemplate(
                template_id=spec.template_id,
                runtime_id=spec.runtime_id,
                raw_task=raw_task,
                entity_key=spec.entity_key,
                entity_values=spec.entity_values,
                variables=spec.variables,
            )
        )

    return tuple(rewritten)
