from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import re
from typing import Any

import yaml

from .generator import profile_records
from .models import DatasetConfig, Violation
from .utils import (
    append_jsonl,
    clamp,
    coerce_scalar,
    dump_json,
    infer_value_type,
    is_iso8601_date,
    is_iso8601_datetime,
    is_uuid,
    timestamp_slug,
    utc_now,
)


def load_contract(path: str) -> dict[str, Any]:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def build_row_locator(dataset: DatasetConfig, record: dict[str, Any]) -> str:
    parts: list[str] = []
    for field in dataset.primary_key_fields:
        value = record.get(field)
        if value is not None:
            parts.append(f"{field}={value}")
    if not parts:
        parts.append(f"source_line={record.get('__source_line')}")
    return ", ".join(parts)


def validate_dataset(
    dataset: DatasetConfig,
    contract: dict[str, Any],
    records: list[dict[str, Any]],
    baselines: dict[str, Any],
    validation_config: dict[str, Any],
) -> tuple[dict[str, Any], list[Violation]]:
    violations: list[Violation] = []
    field_specs = {field["name"]: field for field in contract["fields"]}
    min_observations = validation_config.get("min_observations_for_drift", 20)
    for record in records:
        row_locator = build_row_locator(dataset, record)
        for field_name, spec in field_specs.items():
            try:
                value = record.get(field_name)
                if spec.get("required") and value is None:
                    violations.append(
                        Violation(
                            dataset=dataset.name,
                            column=field_name,
                            status="FAIL",
                            severity="CRITICAL",
                            category="structural",
                            message="Required field is missing.",
                            row_locator=row_locator,
                        )
                    )
                    continue
                if value is None:
                    continue
                _validate_type(dataset.name, field_name, value, spec, row_locator, violations)
                _validate_constraints(dataset.name, field_name, value, spec, row_locator, violations)
                _validate_enum(dataset.name, field_name, value, spec, row_locator, violations)
            except Exception as exc:  # pragma: no cover - defensive by design
                violations.append(
                    Violation(
                        dataset=dataset.name,
                        column=field_name,
                        status="ERROR",
                        severity="HIGH",
                        category="operational",
                        message=f"Validation error: {exc}",
                        row_locator=row_locator,
                    )
                )

    current_profile = profile_records(records)
    for column, baseline in baselines.get(dataset.name, {}).items():
        current_stats = current_profile.get(column, {}).get("stats")
        if not current_stats:
            continue
        if baseline.get("sample_size", 0) < min_observations:
            continue
        stddev = float(baseline.get("stddev", 0.0))
        current_mean = float(current_stats["mean"])
        baseline_mean = float(baseline["mean"])
        delta = abs(current_mean - baseline_mean)
        if stddev == 0.0:
            multiplier = float("inf") if delta > 0 else 0.0
        else:
            multiplier = delta / stddev
        if multiplier > 3:
            violations.append(
                Violation(
                    dataset=dataset.name,
                    column=column,
                    status="FAIL",
                    severity="HIGH",
                    category="drift",
                    message=f"Mean drift exceeded 3 sigma: delta={delta:.4f}, stddev={stddev:.4f}",
                )
            )
        elif multiplier > 2:
            violations.append(
                Violation(
                    dataset=dataset.name,
                    column=column,
                    status="WARN",
                    severity="MEDIUM",
                    category="drift",
                    message=f"Mean drift exceeded 2 sigma: delta={delta:.4f}, stddev={stddev:.4f}",
                )
            )

    summary = {
        "dataset": dataset.name,
        "record_count": len(records),
        "status": _overall_status(violations, dataset.name),
        "violation_count": sum(1 for violation in violations if violation.dataset == dataset.name),
        "generated_at": utc_now().isoformat(),
    }
    return summary, violations


def _validate_type(
    dataset_name: str,
    field_name: str,
    value: Any,
    spec: dict[str, Any],
    row_locator: str,
    violations: list[Violation],
) -> None:
    expected_type = spec.get("type")
    coerced = coerce_scalar(value)
    actual_type = infer_value_type(coerced)
    if expected_type == "number" and actual_type in {"integer", "number"}:
        pass
    elif expected_type == actual_type:
        pass
    else:
        violations.append(
            Violation(
                dataset=dataset_name,
                column=field_name,
                status="FAIL",
                severity="CRITICAL",
                category="type",
                message=f"Type mismatch. Expected {expected_type}, got {actual_type}.",
                row_locator=row_locator,
                expected=expected_type,
                actual=actual_type,
            )
        )
        return

    field_format = spec.get("format")
    if field_format == "uuid" and not is_uuid(value):
        violations.append(
            Violation(
                dataset=dataset_name,
                column=field_name,
                status="FAIL",
                severity="CRITICAL",
                category="format",
                message="Value does not match UUID format.",
                row_locator=row_locator,
            )
        )
    elif field_format == "date-time" and not is_iso8601_datetime(value):
        violations.append(
            Violation(
                dataset=dataset_name,
                column=field_name,
                status="FAIL",
                severity="CRITICAL",
                category="format",
                message="Value does not match ISO-8601 date-time format.",
                row_locator=row_locator,
            )
        )
    elif field_format == "date" and not is_iso8601_date(value):
        violations.append(
            Violation(
                dataset=dataset_name,
                column=field_name,
                status="FAIL",
                severity="CRITICAL",
                category="format",
                message="Value does not match ISO-8601 date format.",
                row_locator=row_locator,
            )
        )


def _validate_constraints(
    dataset_name: str,
    field_name: str,
    value: Any,
    spec: dict[str, Any],
    row_locator: str,
    violations: list[Violation],
) -> None:
    constraints = spec.get("constraints") or {}
    if not constraints:
        return
    numeric = coerce_scalar(value)
    if not isinstance(numeric, (int, float)) or isinstance(numeric, bool):
        violations.append(
            Violation(
                dataset=dataset_name,
                column=field_name,
                status="ERROR",
                severity="HIGH",
                category="operational",
                message="Numeric constraint could not be evaluated against a non-numeric value.",
                row_locator=row_locator,
            )
        )
        return
    minimum = constraints.get("minimum")
    maximum = constraints.get("maximum")
    if minimum is not None and numeric < minimum:
        violations.append(
            Violation(
                dataset=dataset_name,
                column=field_name,
                status="FAIL",
                severity="CRITICAL",
                category="structural",
                message=f"Value {numeric} is below minimum {minimum}.",
                row_locator=row_locator,
                expected={"minimum": minimum},
                actual=numeric,
            )
        )
    if maximum is not None and numeric > maximum:
        violations.append(
            Violation(
                dataset=dataset_name,
                column=field_name,
                status="FAIL",
                severity="CRITICAL",
                category="structural",
                message=f"Value {numeric} is above maximum {maximum}.",
                row_locator=row_locator,
                expected={"maximum": maximum},
                actual=numeric,
            )
        )


def _validate_enum(
    dataset_name: str,
    field_name: str,
    value: Any,
    spec: dict[str, Any],
    row_locator: str,
    violations: list[Violation],
) -> None:
    enum_values = spec.get("enum")
    if enum_values and value not in enum_values:
        violations.append(
            Violation(
                dataset=dataset_name,
                column=field_name,
                status="FAIL",
                severity="CRITICAL",
                category="structural",
                message=f"Value {value!r} is not in the accepted enum set.",
                row_locator=row_locator,
                expected=enum_values,
                actual=value,
            )
        )


def _overall_status(violations: list[Violation], dataset_name: str) -> str:
    relevant = [violation for violation in violations if violation.dataset == dataset_name]
    if any(violation.status == "FAIL" for violation in relevant):
        return "FAIL"
    if any(violation.status == "ERROR" for violation in relevant):
        return "ERROR"
    if any(violation.status == "WARN" for violation in relevant):
        return "WARN"
    return "PASS"


def write_validation_outputs(
    report_path: Path,
    violation_log_path: Path,
    run_payload: dict[str, Any],
    violations: list[Violation],
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    violation_log_path.parent.mkdir(parents=True, exist_ok=True)
    dump_json(report_path, run_payload)
    append_jsonl(violation_log_path, [violation.to_dict() for violation in violations])
