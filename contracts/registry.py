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


def registry_contract_subscribers(registry: dict[str, Any], contract_id: str) -> list[dict[str, Any]]:
    subscriptions = registry.get("subscriptions")
    if isinstance(subscriptions, list):
        return [item for item in subscriptions if item.get("contract_id") == contract_id]

    # Backward-compatible fallback for older dataset-keyed registries.
    dataset_entry = (registry.get("datasets") or {}).get(contract_id, {})
    subscribers = []
    for subscriber in dataset_entry.get("subscribers", []):
        subscribers.append(
            {
                "contract_id": contract_id,
                "subscriber_id": subscriber.get("consumer"),
                "fields_consumed": subscriber.get("depends_on_fields", []),
                "breaking_fields": [
                    {
                        "field": field,
                        "reason": f"Declared dependency for {subscriber.get('consumer', 'unknown consumer')}.",
                    }
                    for field in subscriber.get("depends_on_fields", [])
                ]
                + [
                    {
                        "field": prefix,
                        "reason": f"Declared prefix dependency for {subscriber.get('consumer', 'unknown consumer')}.",
                    }
                    for prefix in subscriber.get("depends_on_prefixes", [])
                ],
                "validation_mode": subscriber.get("severity", "AUDIT").upper(),
                "contact": subscriber.get("owner"),
                "registered_at": None,
            }
        )
    return subscribers


def registry_blast_radius(registry: dict[str, Any], contract_id: str, column_name: str | None) -> list[dict[str, Any]]:
    subscribers = registry_contract_subscribers(registry, contract_id)
    if not column_name:
        return subscribers
    matches: list[dict[str, Any]] = []
    for subscriber in subscribers:
        for breaking_field in subscriber.get("breaking_fields", []):
            field_path = breaking_field.get("field")
            if not field_path:
                continue
            if column_name == field_path or column_name.startswith(field_path):
                matches.append(
                    {
                        **subscriber,
                        "name": subscriber.get("subscriber_id"),
                        "reason": breaking_field.get("reason"),
                    }
                )
                break
    return matches


def registry_migration_gate(
    registry: dict[str, Any],
    contract_id: str,
    breaking_changes: list[dict[str, Any]],
) -> dict[str, Any]:
    plans = _contract_migration_plans(registry, contract_id)
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


def _contract_migration_plans(registry: dict[str, Any], contract_id: str) -> list[dict[str, Any]]:
    migration_plans = registry.get("migration_plans")
    if isinstance(migration_plans, list):
        return [item for item in migration_plans if item.get("contract_id") == contract_id]

    dataset_entry = (registry.get("datasets") or {}).get(contract_id, {})
    plans = []
    for plan in dataset_entry.get("migration_plans", []):
        plans.append({**plan, "contract_id": contract_id})
    return plans
