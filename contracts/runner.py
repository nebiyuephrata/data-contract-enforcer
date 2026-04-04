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
    mode: str = "AUDIT",
) -> tuple[dict[str, Any], list[Violation]]:
    violations: list[Violation] = []
    field_specs = {field["name"]: field for field in contract["fields"]}
    clauses = contract.get("clauses", [])
    min_observations = validation_config.get("min_observations_for_drift", 20)
    observed_columns = {column for record in records for column in record}
    for field_name in field_specs:
        if field_name in observed_columns:
            continue
        violations.append(
            Violation(
                dataset=dataset.name,
                column=field_name,
                status="ERROR",
                severity="HIGH",
                category="operational",
                message="Contract field is missing from the evaluated dataset snapshot.",
                check_id=f"{field_name}.missing_column",
            )
        )
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
                            check_id=f"{field_name}.required",
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
                        check_id=f"{field_name}.validation_error",
                        row_locator=row_locator,
                    )
                )

        for clause in clauses:
            try:
                violations.extend(_evaluate_clause(dataset.name, clause, record, row_locator))
            except Exception as exc:  # pragma: no cover - defensive by design
                violations.append(
                    Violation(
                        dataset=dataset.name,
                        column=clause.get("id"),
                        status="ERROR",
                        severity="HIGH",
                        category="operational",
                        message=f"Clause evaluation error: {exc}",
                        check_id=clause.get("id"),
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
                    check_id=f"{column}.drift_3sigma",
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
                    check_id=f"{column}.drift_2sigma",
                )
            )

    results = _build_result_entries(dataset.name, contract, violations)
    summary = {
        "report_id": f"{dataset.name}-{timestamp_slug()}",
        "contract_id": contract.get("id", dataset.name),
        "snapshot_id": contract.get("generated_at", "latest"),
        "run_timestamp": utc_now().isoformat(),
        "dataset": dataset.name,
        "contract_path": dataset.contract_path,
        "source_path": dataset.source,
        "record_count": len(records),
        "mode": mode,
        "pipeline_action": _pipeline_action(mode, violations, dataset.name),
        "total_checks": len(results),
        "passed": sum(1 for result in results if result["status"] == "PASS"),
        "failed": sum(1 for result in results if result["status"] == "FAIL"),
        "warned": sum(1 for result in results if result["status"] == "WARNING"),
        "errored": sum(1 for result in results if result["status"] == "ERROR"),
        "results": results,
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
                check_id=f"{field_name}.type",
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
                check_id=f"{field_name}.uuid_format",
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
                check_id=f"{field_name}.datetime_format",
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
                check_id=f"{field_name}.date_format",
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
                check_id=f"{field_name}.range_evaluation",
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
                check_id=f"{field_name}.minimum",
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
                check_id=f"{field_name}.maximum",
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
                check_id=f"{field_name}.enum",
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


def _pipeline_action(mode: str, violations: list[Violation], dataset_name: str) -> str:
    relevant = [violation for violation in violations if violation.dataset == dataset_name]
    normalized_mode = mode.upper()
    if normalized_mode == "AUDIT":
        return "LOG"
    if normalized_mode == "WARN":
        return "BLOCK" if any(v.severity == "CRITICAL" and v.status == "FAIL" for v in relevant) else "WARN"
    if normalized_mode == "ENFORCE":
        return (
            "BLOCK"
            if any(v.status in {"FAIL", "ERROR"} and v.severity in {"CRITICAL", "HIGH"} for v in relevant)
            else "PASS"
        )
    return "LOG"


def _build_result_entries(dataset_name: str, contract: dict[str, Any], violations: list[Violation]) -> list[dict[str, Any]]:
    relevant = [violation for violation in violations if violation.dataset == dataset_name]
    results = [
        {
            "check_id": violation.check_id or _infer_check_id(violation),
            "status": "WARNING" if violation.status == "WARN" else violation.status,
            "severity": violation.severity,
            "actual_value": violation.actual,
            "expected": violation.expected,
            "message": violation.message,
            "column_name": violation.column,
            "row_locator": violation.row_locator,
        }
        for violation in relevant
    ]
    existing_check_ids = {result["check_id"] for result in results}
    for clause in contract.get("clauses", []):
        if clause["id"] in existing_check_ids:
            continue
        results.append(
            {
                "check_id": clause["id"],
                "status": "PASS",
                "severity": "LOW",
                "actual_value": None,
                "expected": clause.get("check"),
                "message": clause.get("description", "Clause passed for all evaluated records."),
                "column_name": clause.get("check", {}).get("field"),
                "row_locator": None,
            }
        )
    return results


def _infer_check_id(violation: Violation) -> str:
    if violation.check_id:
        return violation.check_id
    if violation.column:
        return f"{violation.column}.{violation.category}"
    return f"{violation.dataset}.{violation.category}"


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


def _evaluate_clause(
    dataset_name: str,
    clause: dict[str, Any],
    record: dict[str, Any],
    row_locator: str,
) -> list[Violation]:
    check = clause.get("check", {})
    check_type = check.get("type")
    if check_type == "conditional_composite":
        if record.get(check["when_field"]) != check["when_equals"]:
            return []
        output: list[Violation] = []
        for subcheck in check.get("checks", []):
            output.extend(_evaluate_clause(dataset_name, {"id": clause["id"], "check": subcheck}, record, row_locator))
        return output
    if check_type == "conditional_min_pair":
        if record.get(check["when_field"]) != check["when_equals"]:
            return []
        output = []
        for subcheck in check.get("checks", []):
            output.extend(
                _evaluate_clause(
                    dataset_name,
                    {
                        "id": clause["id"],
                        "check": {"type": "min_value", "field": subcheck["field"], "minimum": subcheck["minimum"]},
                    },
                    record,
                    row_locator,
                )
            )
        return output
    if check_type == "conditional_datetime_order":
        if record.get(check["when_field"]) != check["when_equals"]:
            return []
        return _evaluate_clause(
            dataset_name,
            {
                "id": clause["id"],
                "check": {
                    "type": "datetime_order",
                    "earlier_field": check["earlier_field"],
                    "later_field": check["later_field"],
                },
            },
            record,
            row_locator,
        )
    if check_type == "conditional_numeric_floor_requires_value":
        if record.get(check["when_field"]) != check["when_equals"]:
            return []
        numeric_value = coerce_scalar(record.get(check["numeric_field"]))
        if not isinstance(numeric_value, (int, float)) or isinstance(numeric_value, bool):
            return []
        if float(numeric_value) < float(check["less_than"]) and record.get(check["required_field"]) != check["required_value"]:
            return [
                Violation(
                    dataset=dataset_name,
                    column=check["required_field"],
                    status="FAIL",
                    severity="CRITICAL",
                    category="structural",
                    message=f"Clause {clause['id']} failed: {check['required_field']} must equal {check['required_value']} when {check['numeric_field']} is below {check['less_than']}.",
                    check_id=clause["id"],
                    row_locator=row_locator,
                )
            ]
        return []
    if check_type == "conditional_equals":
        if record.get(check["when_field"]) != check["when_equals"]:
            return []
        return _evaluate_clause(
            dataset_name,
            {"id": clause["id"], "check": {"type": "equals", "field": check["field"], "value": check["value"]}},
            record,
            row_locator,
        )

    field = check.get("field")
    if check_type == "accepted_values":
        value = record.get(field)
        if value not in check["values"]:
            return [_clause_violation(dataset_name, clause["id"], field, row_locator, f"Value {value!r} is outside accepted values {check['values']}.")]
        return []
    if check_type == "accepted_values_if_present":
        value = record.get(field)
        if value is None:
            return []
        return _evaluate_clause(dataset_name, {"id": clause["id"], "check": {"type": "accepted_values", "field": field, "values": check["values"]}}, record, row_locator)
    if check_type == "range_if_present":
        value = record.get(field)
        if value is None:
            return []
        return _evaluate_clause(
            dataset_name,
            {
                "id": clause["id"],
                "check": {"type": "range", "field": field, "minimum": check["minimum"], "maximum": check["maximum"]},
            },
            record,
            row_locator,
        )
    if check_type == "range":
        numeric = coerce_scalar(record.get(field))
        if not isinstance(numeric, (int, float)) or isinstance(numeric, bool):
            return [_clause_violation(dataset_name, clause["id"], field, row_locator, "Expected numeric value for range check.", status="ERROR", severity="HIGH", category="operational")]
        if numeric < check["minimum"] or numeric > check["maximum"]:
            return [_clause_violation(dataset_name, clause["id"], field, row_locator, f"Value {numeric} is outside range [{check['minimum']}, {check['maximum']}].")]
        return []
    if check_type == "prefix_range":
        violations: list[Violation] = []
        for record_field, record_value in record.items():
            if not record_field.startswith(check["field_prefix"]):
                continue
            numeric = coerce_scalar(record_value)
            if not isinstance(numeric, (int, float)) or isinstance(numeric, bool):
                violations.append(_clause_violation(dataset_name, clause["id"], record_field, row_locator, "Expected numeric value for prefixed range check.", status="ERROR", severity="HIGH", category="operational"))
                continue
            if numeric < check["minimum"] or numeric > check["maximum"]:
                violations.append(_clause_violation(dataset_name, clause["id"], record_field, row_locator, f"Value {numeric} is outside range [{check['minimum']}, {check['maximum']}]."))
        return violations
    if check_type == "min_value":
        numeric = coerce_scalar(record.get(field))
        if not isinstance(numeric, (int, float)) or isinstance(numeric, bool):
            return [_clause_violation(dataset_name, clause["id"], field, row_locator, "Expected numeric value for minimum check.", status="ERROR", severity="HIGH", category="operational")]
        if numeric < check["minimum"]:
            return [_clause_violation(dataset_name, clause["id"], field, row_locator, f"Value {numeric} is below minimum {check['minimum']}.")]
        return []
    if check_type == "field_gte":
        left = coerce_scalar(record.get(check["left_field"]))
        right = coerce_scalar(record.get(check["right_field"]))
        if not isinstance(left, (int, float)) or isinstance(left, bool) or not isinstance(right, (int, float)) or isinstance(right, bool):
            return []
        if float(left) < float(right):
            return [_clause_violation(dataset_name, clause["id"], check["left_field"], row_locator, f"{check['left_field']} must be >= {check['right_field']}.")]
        return []
    if check_type == "datetime_order":
        earlier = record.get(check["earlier_field"])
        later = record.get(check["later_field"])
        if earlier is None or later is None:
            return []
        if not is_iso8601_datetime(str(earlier)) and not is_iso8601_date(str(earlier)):
            return []
        if not is_iso8601_datetime(str(later)) and not is_iso8601_date(str(later)):
            return []
        earlier_dt = _parse_temporal(str(earlier))
        later_dt = _parse_temporal(str(later))
        if earlier_dt > later_dt:
            return [_clause_violation(dataset_name, clause["id"], check["later_field"], row_locator, f"{check['later_field']} must not occur before {check['earlier_field']}.")]
        return []
    if check_type == "equals":
        value = record.get(field)
        if value != check["value"]:
            return [_clause_violation(dataset_name, clause["id"], field, row_locator, f"Value {value!r} must equal {check['value']!r}.")]
        return []
    if check_type == "startswith_any":
        value = record.get(field)
        if not isinstance(value, str) or not any(value.startswith(prefix) for prefix in check["prefixes"]):
            return [_clause_violation(dataset_name, clause["id"], field, row_locator, f"Value {value!r} does not start with an approved prefix.")]
        return []
    if check_type == "format":
        value = record.get(field)
        if check["format"] == "date-time" and not is_iso8601_datetime(value):
            return [_clause_violation(dataset_name, clause["id"], field, row_locator, "Value is not a valid ISO-8601 date-time.")]
        if check["format"] == "date" and not is_iso8601_date(value):
            return [_clause_violation(dataset_name, clause["id"], field, row_locator, "Value is not a valid ISO-8601 date.")]
        return []
    return []


def _parse_temporal(value: str) -> datetime:
    if is_iso8601_datetime(value):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.fromisoformat(f"{value}T00:00:00+00:00")


def _clause_violation(
    dataset_name: str,
    clause_id: str,
    field_name: str | None,
    row_locator: str,
    message: str,
    status: str = "FAIL",
    severity: str = "CRITICAL",
    category: str = "structural",
) -> Violation:
    return Violation(
        dataset=dataset_name,
        column=field_name or clause_id,
        status=status,
        severity=severity,
        category=category,
        message=f"Clause {clause_id} failed: {message}",
        check_id=clause_id,
        row_locator=row_locator,
    )
