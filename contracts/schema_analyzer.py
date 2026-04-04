from __future__ import annotations

from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import yaml

from .models import SchemaChange
from .registry import registry_contract_subscribers, registry_migration_gate


WIDENING_TYPES = {
    ("integer", "number"),
}


def select_snapshot_pair(snapshot_dir: Path, since: str | None = None) -> tuple[Path | None, Path | None]:
    snapshots = sorted(path for path in snapshot_dir.glob("*.yaml") if path.name != "latest.yaml")
    if since:
        snapshots = [path for path in snapshots if path.stem >= since]
    if not snapshots:
        return None, None
    current_path = snapshots[-1]
    previous_candidates = [path for path in snapshots if path != current_path]
    previous_path = previous_candidates[-1] if previous_candidates else None
    return current_path, previous_path


def compare_contract_snapshots(dataset: str, current_path: Path, previous_path: Path | None) -> list[SchemaChange]:
    if previous_path is None or not previous_path.exists():
        return []
    current = yaml.safe_load(current_path.read_text(encoding="utf-8"))
    previous = yaml.safe_load(previous_path.read_text(encoding="utf-8"))
    current_fields = {field["name"]: field for field in current.get("fields", [])}
    previous_fields = {field["name"]: field for field in previous.get("fields", [])}

    changes: list[SchemaChange] = []
    removed = set(previous_fields) - set(current_fields)
    added = set(current_fields) - set(previous_fields)

    rename_pairs = _detect_renames(previous_fields, current_fields, removed, added)
    renamed_removed = {old for old, _ in rename_pairs}
    renamed_added = {new for _, new in rename_pairs}
    for old_name, new_name in rename_pairs:
        changes.append(
            SchemaChange(
                dataset=dataset,
                field_name=f"{old_name}->{new_name}",
                compatibility="BREAKING",
                change_type="rename",
                message=f"Probable rename detected from {old_name} to {new_name}.",
                severity="HIGH",
            )
        )

    for field_name in sorted(removed - renamed_removed):
        changes.append(
            SchemaChange(
                dataset=dataset,
                field_name=field_name,
                compatibility="BREAKING",
                change_type="remove_field",
                message=f"Field {field_name} was removed.",
                severity="HIGH",
            )
        )

    for field_name in sorted(added - renamed_added):
        compatibility = "BREAKING" if current_fields[field_name].get("required") else "COMPATIBLE"
        change_type = "add_non_nullable_field" if compatibility == "BREAKING" else "add_nullable_field"
        severity = "HIGH" if compatibility == "BREAKING" else "LOW"
        changes.append(
            SchemaChange(
                dataset=dataset,
                field_name=field_name,
                compatibility=compatibility,
                change_type=change_type,
                message=f"Field {field_name} was added.",
                severity=severity,
            )
        )

    for field_name in sorted(set(current_fields) & set(previous_fields)):
        changes.extend(_compare_field(dataset, field_name, previous_fields[field_name], current_fields[field_name]))
    return changes


def build_migration_report(
    dataset: str,
    current_path: Path,
    previous_path: Path | None,
    registry: dict[str, Any],
) -> dict[str, Any]:
    changes = compare_contract_snapshots(dataset, current_path, previous_path)
    compatibility_verdict = "BREAKING" if any(change.compatibility == "BREAKING" for change in changes) else "COMPATIBLE"
    consumer_failure_modes = build_consumer_failure_modes(dataset, changes, registry)
    report = {
        "contract_id": dataset,
        "current_snapshot": str(current_path),
        "previous_snapshot": str(previous_path) if previous_path else None,
        "diff": [change.to_dict() for change in changes],
        "compatibility_verdict": compatibility_verdict,
        "blast_radius": {
            "subscriber_count": len(consumer_failure_modes),
            "subscribers": [item["subscriber_id"] for item in consumer_failure_modes],
        },
        "migration_checklist": build_migration_checklist(dataset, changes, consumer_failure_modes),
        "rollback_plan": build_rollback_plan(dataset, current_path, previous_path),
        "consumer_failure_modes": consumer_failure_modes,
    }
    report["registry_gate"] = evaluate_registry_gate(dataset, changes, registry)
    return report


def build_consumer_failure_modes(
    dataset: str,
    changes: list[SchemaChange],
    registry: dict[str, Any],
) -> list[dict[str, Any]]:
    subscribers = registry_contract_subscribers(registry, dataset)
    failure_modes: list[dict[str, Any]] = []
    for subscriber in subscribers:
        impacted_fields = []
        for change in changes:
            impacted = _change_impacts_subscriber(change, subscriber)
            if impacted:
                impacted_fields.append(
                    {
                        "field_name": change.field_name,
                        "change_type": change.change_type,
                        "compatibility": change.compatibility,
                        "reason": impacted,
                    }
                )
        if not impacted_fields:
            continue
        failure_modes.append(
            {
                "subscriber_id": subscriber.get("subscriber_id"),
                "validation_mode": subscriber.get("validation_mode"),
                "contact": subscriber.get("contact"),
                "impacted_fields": impacted_fields,
                "failure_mode": _failure_mode_summary(subscriber, impacted_fields),
            }
        )
    return failure_modes


def build_migration_checklist(
    dataset: str,
    changes: list[SchemaChange],
    consumer_failure_modes: list[dict[str, Any]],
) -> list[str]:
    checklist = [
        f"Regenerate the producer contract for {dataset} and publish the updated snapshot before deployment.",
        f"Validate downstream readers for {dataset} against the new contract snapshot in a staging environment.",
    ]
    for change in changes:
        if change.compatibility == "BREAKING":
            checklist.append(f"Create or approve a migration plan for {change.field_name} ({change.change_type}).")
    for consumer in consumer_failure_modes:
        checklist.append(f"Notify {consumer['subscriber_id']} about the impacted fields before rollout.")
    return list(dict.fromkeys(checklist))


def build_rollback_plan(dataset: str, current_path: Path, previous_path: Path | None) -> list[str]:
    rollback = [
        f"Pause deployments for {dataset} at the producer boundary if validation or migration gates fail.",
        f"Restore the previous contract snapshot from {previous_path}." if previous_path else f"Restore the last known good contract for {dataset}.",
        f"Regenerate consumer-facing artifacts from {current_path.parent / 'latest.yaml'} after rollback verification.",
    ]
    return rollback


def evaluate_registry_gate(dataset: str, changes: list[SchemaChange], registry: dict[str, Any]) -> dict[str, Any]:
    breaking_changes = [
        {
            "dataset": change.dataset,
            "field_name": change.field_name,
            "change_type": change.change_type,
            "message": change.message,
        }
        for change in changes
        if change.compatibility == "BREAKING"
    ]
    gate = registry_migration_gate(registry, dataset, breaking_changes)
    gate["dataset"] = dataset
    gate["breaking_change_count"] = len(breaking_changes)
    gate["enforcement_location"] = "producer_predeploy_gate"
    if gate["status"] == "FAIL":
        gate["message"] = "Breaking schema changes were detected without approved migration plans in the contract registry."
    else:
        gate["message"] = "Registry gate satisfied for detected schema changes."
    return gate


def _detect_renames(
    previous_fields: dict[str, dict[str, Any]],
    current_fields: dict[str, dict[str, Any]],
    removed: set[str],
    added: set[str],
) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for old_name in removed:
        old_type = previous_fields[old_name].get("type")
        best: tuple[str, float] | None = None
        for new_name in added:
            new_type = current_fields[new_name].get("type")
            if old_type != new_type:
                continue
            similarity = SequenceMatcher(a=old_name, b=new_name).ratio()
            if similarity < 0.6:
                continue
            if best is None or similarity > best[1]:
                best = (new_name, similarity)
        if best:
            pairs.append((old_name, best[0]))
    return pairs


def _compare_field(dataset: str, field_name: str, previous: dict[str, Any], current: dict[str, Any]) -> list[SchemaChange]:
    changes: list[SchemaChange] = []
    previous_type = previous.get("type")
    current_type = current.get("type")
    if previous_type != current_type:
        compatibility = "COMPATIBLE" if (previous_type, current_type) in WIDENING_TYPES else "BREAKING"
        change_type = "widen_type" if compatibility == "COMPATIBLE" else "narrow_type"
        severity = "LOW" if compatibility == "COMPATIBLE" else "HIGH"
        if _is_critical_narrow_scale_change(previous, current):
            compatibility = "BREAKING"
            change_type = "narrow_type"
            severity = "CRITICAL"
        changes.append(
            SchemaChange(
                dataset=dataset,
                field_name=field_name,
                compatibility=compatibility,
                change_type=change_type,
                message=f"Type changed from {previous_type} to {current_type}.",
                severity=severity,
            )
        )
    if previous.get("required") != current.get("required"):
        compatibility = "BREAKING" if current.get("required") else "COMPATIBLE"
        change_type = "tighten_nullability" if compatibility == "BREAKING" else "relax_nullability"
        severity = "HIGH" if compatibility == "BREAKING" else "LOW"
        changes.append(
            SchemaChange(
                dataset=dataset,
                field_name=field_name,
                compatibility=compatibility,
                change_type=change_type,
                message=f"Required changed from {previous.get('required')} to {current.get('required')}.",
                severity=severity,
            )
        )
    previous_enum = set(previous.get("enum") or [])
    current_enum = set(current.get("enum") or [])
    if previous_enum and current_enum and previous_enum != current_enum:
        compatibility = "COMPATIBLE" if previous_enum <= current_enum else "BREAKING"
        change_type = "add_enum_value" if compatibility == "COMPATIBLE" else "remove_enum_value"
        severity = "LOW" if compatibility == "COMPATIBLE" else "HIGH"
        changes.append(
            SchemaChange(
                dataset=dataset,
                field_name=field_name,
                compatibility=compatibility,
                change_type=change_type,
                message="Enum domain changed.",
                severity=severity,
            )
        )
    previous_constraints = previous.get("constraints") or {}
    current_constraints = current.get("constraints") or {}
    if previous_constraints != current_constraints:
        compatibility = "COMPATIBLE"
        change_type = "metadata_change"
        severity = "LOW"
        for bound in ("minimum", "maximum"):
            if bound in previous_constraints and bound in current_constraints:
                if bound == "minimum" and current_constraints[bound] > previous_constraints[bound]:
                    compatibility = "BREAKING"
                    change_type = "narrow_range"
                    severity = "HIGH"
                if bound == "maximum" and current_constraints[bound] < previous_constraints[bound]:
                    compatibility = "BREAKING"
                    change_type = "narrow_range"
                    severity = "HIGH"
        changes.append(
            SchemaChange(
                dataset=dataset,
                field_name=field_name,
                compatibility=compatibility,
                change_type=change_type,
                message="Constraint metadata changed.",
                severity=severity,
            )
        )
    return changes


def _is_critical_narrow_scale_change(previous: dict[str, Any], current: dict[str, Any]) -> bool:
    previous_type = previous.get("type")
    current_type = current.get("type")
    if previous_type != "number" or current_type != "integer":
        return False
    previous_constraints = previous.get("constraints") or {}
    current_constraints = current.get("constraints") or {}
    return (
        previous_constraints.get("minimum") == 0.0
        and previous_constraints.get("maximum") == 1.0
        and current_constraints.get("minimum") == 0
        and current_constraints.get("maximum") == 100
    )


def _change_impacts_subscriber(change: SchemaChange, subscriber: dict[str, Any]) -> str | None:
    for breaking_field in subscriber.get("breaking_fields", []):
        field_path = breaking_field.get("field")
        if not field_path:
            continue
        if change.field_name == field_path or str(change.field_name).startswith(field_path):
            return breaking_field.get("reason") or "Declared breaking dependency."
        if "->" in change.field_name:
            old_name, _, new_name = change.field_name.partition("->")
            if old_name == field_path or new_name == field_path:
                return breaking_field.get("reason") or "Declared rename-sensitive dependency."
    return None


def _failure_mode_summary(subscriber: dict[str, Any], impacted_fields: list[dict[str, Any]]) -> str:
    fields = ", ".join(item["field_name"] for item in impacted_fields[:3])
    return (
        f"{subscriber.get('subscriber_id')} may fail ingestion or misinterpret data for {fields} "
        f"under {subscriber.get('validation_mode', 'AUDIT')} mode."
    )
