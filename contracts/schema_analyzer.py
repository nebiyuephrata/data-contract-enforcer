from __future__ import annotations

from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import yaml

from .models import SchemaChange


WIDENING_TYPES = {
    ("integer", "number"),
}


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
            )
        )

    for field_name in sorted(added - renamed_added):
        compatibility = "BREAKING" if current_fields[field_name].get("required") else "COMPATIBLE"
        change_type = "add_non_nullable_field" if compatibility == "BREAKING" else "add_nullable_field"
        changes.append(
            SchemaChange(
                dataset=dataset,
                field_name=field_name,
                compatibility=compatibility,
                change_type=change_type,
                message=f"Field {field_name} was added.",
            )
        )

    for field_name in sorted(set(current_fields) & set(previous_fields)):
        changes.extend(_compare_field(dataset, field_name, previous_fields[field_name], current_fields[field_name]))
    return changes


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
        changes.append(
            SchemaChange(
                dataset=dataset,
                field_name=field_name,
                compatibility=compatibility,
                change_type=change_type,
                message=f"Type changed from {previous_type} to {current_type}.",
            )
        )
    if previous.get("required") != current.get("required"):
        compatibility = "BREAKING" if current.get("required") else "COMPATIBLE"
        change_type = "tighten_nullability" if compatibility == "BREAKING" else "relax_nullability"
        changes.append(
            SchemaChange(
                dataset=dataset,
                field_name=field_name,
                compatibility=compatibility,
                change_type=change_type,
                message=f"Required changed from {previous.get('required')} to {current.get('required')}.",
            )
        )
    previous_enum = set(previous.get("enum") or [])
    current_enum = set(current.get("enum") or [])
    if previous_enum and current_enum and previous_enum != current_enum:
        compatibility = "COMPATIBLE" if previous_enum <= current_enum else "BREAKING"
        change_type = "widen_enum" if compatibility == "COMPATIBLE" else "narrow_enum"
        changes.append(
            SchemaChange(
                dataset=dataset,
                field_name=field_name,
                compatibility=compatibility,
                change_type=change_type,
                message="Enum domain changed.",
            )
        )
    previous_constraints = previous.get("constraints") or {}
    current_constraints = current.get("constraints") or {}
    if previous_constraints != current_constraints:
        compatibility = "COMPATIBLE"
        change_type = "metadata_change"
        for bound in ("minimum", "maximum"):
            if bound in previous_constraints and bound in current_constraints:
                if bound == "minimum" and current_constraints[bound] > previous_constraints[bound]:
                    compatibility = "BREAKING"
                    change_type = "narrow_range"
                if bound == "maximum" and current_constraints[bound] < previous_constraints[bound]:
                    compatibility = "BREAKING"
                    change_type = "narrow_range"
        changes.append(
            SchemaChange(
                dataset=dataset,
                field_name=field_name,
                compatibility=compatibility,
                change_type=change_type,
                message="Constraint metadata changed.",
            )
        )
    return changes
