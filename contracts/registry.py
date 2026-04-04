from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_registry(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    registry_path = Path(path)
    if not registry_path.exists():
        return {}
    return yaml.safe_load(registry_path.read_text(encoding="utf-8")) or {}


def registry_blast_radius(registry: dict[str, Any], dataset_name: str, column_name: str | None) -> list[dict[str, Any]]:
    dataset_entry = (registry.get("datasets") or {}).get(dataset_name, {})
    subscribers = dataset_entry.get("subscribers", [])
    if not column_name:
        return subscribers
    matches: list[dict[str, Any]] = []
    for subscriber in subscribers:
        fields = subscriber.get("depends_on_fields", [])
        prefixes = subscriber.get("depends_on_prefixes", [])
        if column_name in fields or any(column_name.startswith(prefix) for prefix in prefixes):
            matches.append(subscriber)
    return matches


def registry_migration_gate(
    registry: dict[str, Any],
    dataset_name: str,
    breaking_changes: list[dict[str, Any]],
) -> dict[str, Any]:
    dataset_entry = (registry.get("datasets") or {}).get(dataset_name, {})
    plans = dataset_entry.get("migration_plans", [])
    if not breaking_changes:
        return {"status": "PASS", "missing_plans": []}

    missing: list[dict[str, Any]] = []
    for change in breaking_changes:
        matched = False
        for plan in plans:
            status = str(plan.get("status", "")).lower()
            if status not in {"approved", "active", "completed"}:
                continue
            if plan.get("field_name") == change["field_name"] and plan.get("change_type") == change["change_type"]:
                matched = True
                break
        if not matched:
            missing.append(change)
    return {
        "status": "FAIL" if missing else "PASS",
        "missing_plans": missing,
    }
