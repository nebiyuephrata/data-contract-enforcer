from __future__ import annotations

from pathlib import Path
from typing import Any

from .models import AttributionResult, SchemaChange, Violation
from .utils import dump_json, utc_now


def compute_data_health_score(violations: list[Violation]) -> int:
    score = 100
    for violation in violations:
        if violation.status == "FAIL" and violation.severity == "CRITICAL":
            score -= 20
        elif violation.status == "FAIL" and violation.category == "drift":
            score -= 10
        elif violation.status == "WARN" and violation.category == "drift":
            score -= 5
        elif violation.status == "ERROR":
            score -= 15
    return max(score, 0)


def build_business_narratives(violations: list[Violation]) -> list[str]:
    narratives: list[str] = []
    for violation in violations:
        message = violation.message.lower()
        column = violation.column or "unknown field"
        if "confidence" in column.lower() and ("maximum" in message or "minimum" in message):
            narratives.append(
                f"The {column} field failed its confidence range check, which can distort downstream decision confidence and raise hallucination risk."
            )
        elif violation.category == "structural":
            narratives.append(
                f"The {column} field violated a structural promise, which can break dependent systems expecting stable records."
            )
        elif violation.category == "drift":
            narratives.append(
                f"The {column} field drifted away from its historical baseline, which can indicate upstream process changes or data quality regression."
            )
        elif violation.status == "ERROR":
            narratives.append(
                f"The enforcer encountered an operational error around {column}, which reduces confidence in monitoring coverage until the underlying issue is fixed."
            )
    return list(dict.fromkeys(narratives))


def build_report_payload(
    validation_payload: dict[str, Any],
    violations: list[Violation],
    attributions: list[AttributionResult],
    schema_changes: list[SchemaChange],
    registry_gate: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    score = compute_data_health_score(violations)
    recommendations = build_recommended_actions(violations, attributions, schema_changes, registry_gate or [])
    return {
        "generated_at": utc_now().isoformat(),
        "data_health_score": score,
        "validation_enforcement_location": validation_payload.get("enforcement_location", "consumer_ingestion_boundary"),
        "summary": validation_payload.get("dataset_summaries", []),
        "violations": [violation.to_dict() for violation in violations],
        "attributions": [attribution.to_dict() for attribution in attributions],
        "schema_changes": [change.to_dict() for change in schema_changes],
        "registry_gate": registry_gate or [],
        "business_risks": build_business_narratives(violations),
        "ai_checks": validation_payload.get("ai_checks", []),
        "recommended_actions": recommendations,
    }


def build_recommended_actions(
    violations: list[Violation],
    attributions: list[AttributionResult],
    schema_changes: list[SchemaChange],
    registry_gate: list[dict[str, Any]],
) -> list[str]:
    actions: list[str] = []
    critical_violations = [item for item in violations if item.status == "FAIL" and item.category in {"structural", "type", "format"}]
    if critical_violations:
        top = critical_violations[0]
        blamed_file = next((item.file_path for item in attributions if item.column == top.column), "contracts/runner.py")
        actions.append(
            f"Update {blamed_file} to resolve check {top.check_id or top.column or 'unknown'} for {top.dataset} before the next downstream load."
        )
    failing_gate = next((item for item in registry_gate if item.get("status") == "FAIL"), None)
    if failing_gate:
        actions.append(
            f"Add approved migration plans in the contract registry for {failing_gate['dataset']} before allowing producer-side schema deployment."
        )
    confidence_owner = next((item for item in attributions if item.confidence > 0), None)
    if confidence_owner:
        actions.append(
            f"Review {confidence_owner.file_path} for column {confidence_owner.column} and fix the blamed path before rerunning contract enforcement."
        )
    drift_violation = next((item for item in violations if item.category == "drift" and item.status in {"WARN", "FAIL"}), None)
    if drift_violation and len(actions) < 3:
        actions.append(
            f"Rebaseline or investigate the upstream process feeding {drift_violation.column or drift_violation.dataset} because silent drift has exceeded expected variance."
        )
    if len(actions) < 3:
        actions.append("Increase contract registry coverage for high-value fields so blast radius is explicit before the next release.")
    if len(actions) < 3:
        actions.append("Confirm all consumer-ingestion validators are using the latest generated contracts and baselines before the next scheduled run.")
    if len(actions) < 3:
        actions.append("Review quarantine outputs and clear recurring schema failures before promoting new producer or model changes.")
    return actions[:3]


def write_report_outputs(report_json_path: Path, report_pdf_path: Path, payload: dict[str, Any]) -> None:
    report_json_path.parent.mkdir(parents=True, exist_ok=True)
    report_pdf_path.parent.mkdir(parents=True, exist_ok=True)
    dump_json(report_json_path, payload)
    _write_simple_pdf(report_pdf_path, _pdf_lines(payload))


def _pdf_lines(payload: dict[str, Any]) -> list[str]:
    lines = [
        "Data Contract Enforcer Report",
        f"Generated: {payload['generated_at']}",
        f"Data Health Score: {payload['data_health_score']}",
        "",
        "Business Risks:",
    ]
    lines.extend(payload.get("business_risks", []) or ["No major business risks detected."])
    lines.append("")
    lines.append("Dataset Status:")
    for summary in payload.get("summary", []):
        lines.append(f"{summary['dataset']}: {summary['status']} ({summary['violation_count']} issues)")
    lines.append("")
    lines.append("Recommended Actions:")
    lines.extend(payload.get("recommended_actions", []) or ["No immediate actions required."])
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
