from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import AttributionResult, SchemaChange, Violation
from .utils import dump_json, utc_now


def load_reporting_inputs(
    validation_reports_dir: Path,
    violation_log_path: Path,
    attribution_path: Path,
    schema_evolution_path: Path,
) -> dict[str, Any]:
    validation_runs = sorted(
        path for path in validation_reports_dir.glob("validation-*.json") if path.name != "validation-latest.json"
    )
    latest_validation = validation_reports_dir / "validation-latest.json"
    validation_payload = json.loads(latest_validation.read_text(encoding="utf-8")) if latest_validation.exists() else {}
    violations = [Violation(**row) for row in _read_jsonl_rows(violation_log_path)] if violation_log_path.exists() else []
    attribution_payload = json.loads(attribution_path.read_text(encoding="utf-8")) if attribution_path.exists() else {}
    schema_payload = json.loads(schema_evolution_path.read_text(encoding="utf-8")) if schema_evolution_path.exists() else {}
    return {
        "validation_payload": validation_payload,
        "validation_report_paths": [str(path) for path in validation_runs],
        "violations": violations,
        "attributions": [AttributionResult(**item) for item in attribution_payload.get("attributions", [])],
        "schema_reports": schema_payload.get("reports", []),
        "schema_changes": [SchemaChange(**item) for item in schema_payload.get("changes", [])],
        "registry_gate": schema_payload.get("registry_gate", []),
        "source_paths": {
            "violation_log": str(violation_log_path),
            "validation_reports": str(validation_reports_dir),
            "attribution": str(attribution_path),
            "schema_evolution": str(schema_evolution_path),
        },
    }


def compute_data_health_score(validation_payload: dict[str, Any] | list[Violation], violations: list[Violation] | None = None) -> int:
    if violations is None:
        legacy_violations = validation_payload if isinstance(validation_payload, list) else []
        critical_failures = sum(
            1 for violation in legacy_violations if violation.status == "FAIL" and violation.severity == "CRITICAL"
        )
        return max(100 - (critical_failures * 20), 0)
    assert isinstance(validation_payload, dict)
    summaries = validation_payload.get("dataset_summaries", [])
    total_checks = sum(int(item.get("total_checks", 0)) for item in summaries)
    checks_passed = sum(int(item.get("passed", 0)) for item in summaries)
    base_score = 100 if total_checks == 0 else int((checks_passed / total_checks) * 100)
    critical_failures = sum(1 for violation in violations if violation.status == "FAIL" and violation.severity == "CRITICAL")
    return max(base_score - (critical_failures * 20), 0)


def build_report_payload(
    validation_payload: dict[str, Any],
    violations: list[Violation],
    attributions: list[AttributionResult],
    schema_changes: list[SchemaChange],
    registry_gate: list[dict[str, Any]] | None = None,
    schema_reports: list[dict[str, Any]] | None = None,
    source_paths: dict[str, str] | None = None,
) -> dict[str, Any]:
    score = compute_data_health_score(validation_payload, violations)
    sections = {
        "Data Health Score": {
            "score": score,
            "formula": "(checks_passed / total_checks) * 100 minus 20 points per CRITICAL failure",
            "dataset_summaries": validation_payload.get("dataset_summaries", []),
        },
        "Violations this week": build_violation_descriptions(violations, attributions),
        "Schema changes detected": build_schema_change_section(schema_changes, schema_reports or [], registry_gate or []),
        "AI system risk assessment": build_ai_risk_section(validation_payload.get("ai_checks", []), violations),
        "Recommended actions": build_recommended_actions(
            validation_payload.get("dataset_summaries", []),
            violations,
            attributions,
            schema_changes,
            registry_gate or [],
        ),
    }
    return {
        "generated_at": utc_now().isoformat(),
        "source_paths": source_paths or {},
        "data_health_score": score,
        "sections": sections,
        "business_risks": sections["Violations this week"],
        "summary": validation_payload.get("dataset_summaries", []),
        "violations": [violation.to_dict() for violation in violations],
        "attributions": [attribution.to_dict() for attribution in attributions],
        "schema_changes": [change.to_dict() for change in schema_changes],
        "schema_reports": schema_reports or [],
        "registry_gate": registry_gate or [],
        "ai_checks": validation_payload.get("ai_checks", []),
        "recommended_actions": sections["Recommended actions"],
    }


def build_violation_descriptions(
    violations: list[Violation],
    attributions: list[AttributionResult],
) -> list[str]:
    descriptions: list[str] = []
    for violation in violations:
        impact = _downstream_impact(violation, attributions)
        field_name = violation.column or "unknown field"
        if "confidence" in field_name.lower():
            descriptions.append(
                f"System {violation.dataset} failed on field {field_name} with status {violation.status}; "
                f"downstream impact: {impact}; this can distort confidence-driven decisions and raise hallucination risk."
            )
        else:
            descriptions.append(
                f"System {violation.dataset} failed on field {field_name} with status {violation.status}; "
                f"downstream impact: {impact}."
            )
    return list(dict.fromkeys(descriptions))


def build_schema_change_section(
    schema_changes: list[SchemaChange],
    schema_reports: list[dict[str, Any]],
    registry_gate: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "change_count": len(schema_changes),
        "breaking_changes": [change.to_dict() for change in schema_changes if change.compatibility == "BREAKING"],
        "reports": schema_reports,
        "registry_gate": registry_gate,
    }


def build_ai_risk_section(ai_checks: list[dict[str, Any]], violations: list[Violation]) -> dict[str, Any]:
    ai_violations = [violation.to_dict() for violation in violations if violation.category == "ai"]
    drift_violations = [violation.to_dict() for violation in violations if violation.category == "drift"]
    return {
        "checks": ai_checks,
        "ai_violations": ai_violations,
        "drift_alerts": drift_violations,
    }


def build_recommended_actions(
    dataset_summaries: list[dict[str, Any]],
    violations: list[Violation],
    attributions: list[AttributionResult],
    schema_changes: list[SchemaChange],
    registry_gate: list[dict[str, Any]],
) -> list[str]:
    actions: list[str] = []
    summaries_by_dataset = {item["dataset"]: item for item in dataset_summaries if item.get("dataset")}
    critical_violations = [item for item in violations if item.status == "FAIL"]
    for violation in critical_violations:
        attribution = _matching_attribution(violation, attributions)
        summary = summaries_by_dataset.get(violation.dataset, {})
        blamed_file = _best_file_path(attribution)
        contract_path = summary.get("contract_path", "generated_contracts/unknown.odcs.yaml")
        clause = violation.check_id or violation.column or "unknown_clause"
        actions.append(
            f"Update {blamed_file} and review contract clause {clause} in {contract_path} for dataset {violation.dataset}."
        )
        if len(actions) >= 2:
            break
    for gate in registry_gate:
        if gate.get("status") == "FAIL":
            dataset = gate.get("dataset", "unknown")
            summary = summaries_by_dataset.get(dataset, {})
            contract_path = summary.get("contract_path", "generated_contracts/unknown.odcs.yaml")
            actions.append(
                f"Add migration plans for breaking changes in {contract_path} before deploying producer updates for {dataset}."
            )
    for change in schema_changes:
        if change.compatibility == "BREAKING":
            summary = summaries_by_dataset.get(change.dataset, {})
            contract_path = summary.get("contract_path", "generated_contracts/unknown.odcs.yaml")
            actions.append(
                f"Coordinate consumers on clause {change.change_type} affecting {change.field_name} in {contract_path}."
            )
            break
    deduped = list(dict.fromkeys(actions))
    if len(deduped) < 3:
        for dataset, summary in summaries_by_dataset.items():
            contract_path = summary.get("contract_path", "generated_contracts/unknown.odcs.yaml")
            deduped.append(
                f"Re-run validation against {contract_path} and review clause set latest_snapshot for dataset {dataset}."
            )
            if len(deduped) >= 3:
                break
    if len(deduped) < 3:
        deduped.append(
            "Review contracts/config.yaml and contract clause llm_output_violation_rate before the next scheduled governance run."
        )
    return deduped[:5]


def write_report_outputs(report_json_path: Path, report_pdf_path: Path, payload: dict[str, Any]) -> None:
    report_json_path.parent.mkdir(parents=True, exist_ok=True)
    report_pdf_path.parent.mkdir(parents=True, exist_ok=True)
    dump_json(report_json_path, payload)
    _write_simple_pdf(report_pdf_path, _pdf_lines(payload))


def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                rows.append(json.loads(stripped))
    return rows


def _downstream_impact(violation: Violation, attributions: list[AttributionResult]) -> str:
    attribution = _matching_attribution(violation, attributions)
    if attribution is None:
        return "consumer impact not yet attributed"
    subscribers = [item.get("subscriber_id") for item in attribution.blast_radius.get("subscribers", []) if item.get("subscriber_id")]
    if subscribers:
        return ", ".join(subscribers)
    pipelines = attribution.blast_radius.get("affected_pipelines") or []
    if pipelines:
        return ", ".join(pipelines)
    return "blast radius unresolved"


def _matching_attribution(violation: Violation, attributions: list[AttributionResult]) -> AttributionResult | None:
    for attribution in attributions:
        if attribution.violation_id == violation.violation_id:
            return attribution
    for attribution in attributions:
        if attribution.dataset == violation.dataset and attribution.column == violation.column:
            return attribution
    return None


def _best_file_path(attribution: AttributionResult | None) -> str:
    if attribution and attribution.blame_chain:
        top = attribution.blame_chain[0]
        if top.get("file_path"):
            return str(top["file_path"])
    return "contracts/runner.py"


def _pdf_lines(payload: dict[str, Any]) -> list[str]:
    sections = payload.get("sections", {})
    lines = [
        "Data Contract Enforcer Report",
        f"Generated: {payload['generated_at']}",
        f"Data Health Score: {payload['data_health_score']}",
        "",
        "Data Health Score:",
        json.dumps(sections.get("Data Health Score", {}), default=str),
        "",
        "Violations this week:",
    ]
    lines.extend(sections.get("Violations this week", []) or ["No weekly violations recorded."])
    lines.append("")
    lines.append("Schema changes detected:")
    lines.append(json.dumps(sections.get("Schema changes detected", {}), default=str))
    lines.append("")
    lines.append("AI system risk assessment:")
    lines.append(json.dumps(sections.get("AI system risk assessment", {}), default=str))
    lines.append("")
    lines.append("Recommended actions:")
    lines.extend(sections.get("Recommended actions", []) or ["No immediate actions required."])
    return lines


def _write_simple_pdf(path: Path, lines: list[str]) -> None:
    def escape(text: str) -> str:
        return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    text_lines = ["BT", "/F1 12 Tf", "72 760 Td"]
    for index, line in enumerate(lines):
        if index == 0:
            text_lines.append(f"({escape(line)}) Tj")
        else:
            text_lines.append("0 -16 Td")
            text_lines.append(f"({escape(line)}) Tj")
    text_lines.append("ET")
    stream = "\n".join(text_lines).encode("utf-8")

    objects: list[bytes] = []
    objects.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    objects.append(b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    objects.append(
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>"
    )
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    objects.append(f"<< /Length {len(stream)} >>\nstream\n".encode("utf-8") + stream + b"\nendstream")

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{index} 0 obj\n".encode("utf-8"))
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")

    xref_offset = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("utf-8"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("utf-8"))
    pdf.extend(
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n".encode("utf-8")
    )
    path.write_bytes(pdf)
