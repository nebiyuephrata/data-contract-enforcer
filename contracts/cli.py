from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import shutil
import tempfile
from typing import Any

from .ai_extensions import run_embedding_drift, run_llm_schema_checks
from .attributor import attribute_violations
from .config import DEFAULT_CONFIG_PATH, load_config
from .generator import build_baselines, build_contract, build_dbt_schema, load_dataset_records, write_contract_outputs
from .models import AttributionResult, SchemaChange, Violation
from .registry import load_registry
from .report_generator import build_report_payload, write_report_outputs
from .runner import load_contract, validate_dataset, write_validation_outputs
from .schema_analyzer import compare_contract_snapshots, evaluate_registry_gate
from .utils import dump_json, ensure_artifact_dirs, latest_file, project_root, timestamp_slug


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    config = load_config(Path(args.config) if getattr(args, "config", None) else DEFAULT_CONFIG_PATH)
    ensure_artifact_dirs(project_root())
    return args.func(args, config)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Data Contract Enforcer CLI")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to contracts/config.yaml")
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap = subparsers.add_parser("bootstrap-pilot")
    bootstrap.set_defaults(func=cmd_bootstrap_pilot)

    generate = subparsers.add_parser("generate-contracts")
    generate.set_defaults(func=cmd_generate_contracts)

    baselines = subparsers.add_parser("snapshot-baselines")
    baselines.set_defaults(func=cmd_snapshot_baselines)

    validate = subparsers.add_parser("validate")
    validate.set_defaults(func=cmd_validate)

    analyze = subparsers.add_parser("analyze-schema")
    analyze.set_defaults(func=cmd_analyze_schema)

    attribute = subparsers.add_parser("attribute")
    attribute.set_defaults(func=cmd_attribute)

    report = subparsers.add_parser("report")
    report.set_defaults(func=cmd_report)

    stress = subparsers.add_parser("stress-test")
    stress.set_defaults(func=cmd_stress_test)
    return parser


def cmd_bootstrap_pilot(args: argparse.Namespace, config: dict[str, Any]) -> int:
    ensure_artifact_dirs(project_root())
    print("Pilot directories and config are ready.")
    return 0


def cmd_generate_contracts(args: argparse.Namespace, config: dict[str, Any]) -> int:
    outputs = []
    for dataset in config["datasets"]:
        _, records = load_dataset_records(dataset)
        contract = build_contract(dataset, records, config["paths"].get("registry"))
        dbt_schema = build_dbt_schema(contract)
        outputs.append(write_contract_outputs(dataset, contract, dbt_schema))
    print(json.dumps(outputs, indent=2))
    return 0


def cmd_snapshot_baselines(args: argparse.Namespace, config: dict[str, Any]) -> int:
    baseline_payload: dict[str, Any] = {}
    for dataset in config["datasets"]:
        _, records = load_dataset_records(dataset)
        baseline_payload[dataset.name] = build_baselines(dataset, records)
    dump_json(Path(config["paths"]["baselines"]), baseline_payload)
    print(f"Wrote baselines to {config['paths']['baselines']}")
    return 0


def cmd_validate(args: argparse.Namespace, config: dict[str, Any]) -> int:
    baselines_path = Path(config["paths"]["baselines"])
    baselines = json.loads(baselines_path.read_text(encoding="utf-8")) if baselines_path.exists() else {}
    all_violations: list[Violation] = []
    dataset_summaries: list[dict[str, Any]] = []
    ai_checks: list[dict[str, Any]] = []
    for dataset in config["datasets"]:
        raw_records, records = load_dataset_records(dataset)
        contract = load_contract(dataset.contract_path)
        summary, violations = validate_dataset(dataset, contract, records, baselines, config["validation"])
        dataset_summaries.append(summary)
        all_violations.extend(violations)
        if dataset.text_fields:
            checks, ai_violations = run_embedding_drift(
                dataset.name,
                records,
                dataset.text_fields,
                config["ai"],
                Path(config["paths"]["embedding_baselines"]),
            )
            ai_checks.extend(checks)
            all_violations.extend(ai_violations)
        llm_checks, llm_violations = run_llm_schema_checks(
            dataset.name,
            raw_records,
            Path(config["paths"]["llm_violation_rates"]),
            Path(config["paths"]["quarantine"]),
        )
        ai_checks.extend(llm_checks)
        all_violations.extend(llm_violations)

    run_payload = {
        "run_id": timestamp_slug(),
        "generated_at": __import__("datetime").datetime.now(__import__("datetime").UTC).isoformat(),
        "enforcement_location": "consumer_ingestion_boundary",
        "dataset_summaries": dataset_summaries,
        "violations": [violation.to_dict() for violation in all_violations],
        "ai_checks": ai_checks,
    }
    report_path = Path(config["paths"]["validation_reports"]) / f"validation-{run_payload['run_id']}.json"
    write_validation_outputs(report_path, Path(config["paths"]["violation_log"]), run_payload, all_violations)
    latest_path = Path(config["paths"]["validation_reports"]) / "validation-latest.json"
    dump_json(latest_path, run_payload)
    print(f"Wrote validation report to {report_path}")
    return 0


def cmd_analyze_schema(args: argparse.Namespace, config: dict[str, Any]) -> int:
    all_changes: list[SchemaChange] = []
    registry_gate: list[dict[str, Any]] = []
    registry = load_registry(config["paths"].get("registry"))
    for dataset in config["datasets"]:
        snapshot_dir = project_root() / "schema_snapshots" / "contracts" / dataset.name
        snapshots = sorted(path for path in snapshot_dir.glob("*.yaml") if path.name != "latest.yaml")
        current_path = Path(dataset.contract_path)
        previous_path = snapshots[-2] if len(snapshots) >= 2 else None
        changes = compare_contract_snapshots(dataset.name, current_path, previous_path)
        all_changes.extend(changes)
        registry_gate.append(evaluate_registry_gate(dataset.name, changes, registry))
    payload = {
        "generated_at": __import__("datetime").datetime.now(__import__("datetime").UTC).isoformat(),
        "enforcement_location": "producer_predeploy_gate",
        "changes": [change.to_dict() for change in all_changes],
        "registry_gate": registry_gate,
    }
    dump_json(Path(config["paths"]["schema_evolution"]), payload)
    print(f"Wrote schema evolution analysis to {config['paths']['schema_evolution']}")
    return 0


def cmd_attribute(args: argparse.Namespace, config: dict[str, Any]) -> int:
    validation_path = Path(config["paths"]["validation_reports"]) / "validation-latest.json"
    payload = json.loads(validation_path.read_text(encoding="utf-8"))
    violations = [Violation(**violation) for violation in payload.get("violations", [])]
    lineage_snapshot = next((dataset.lineage_snapshot for dataset in config["datasets"] if dataset.lineage_snapshot), None)
    attributions = attribute_violations(
        violations,
        config["repositories"],
        lineage_snapshot,
        config["paths"].get("registry"),
    )
    attribution_payload = {
        "generated_at": __import__("datetime").datetime.now(__import__("datetime").UTC).isoformat(),
        "attributions": [result.to_dict() for result in attributions],
    }
    dump_json(Path(config["paths"]["attribution"]), attribution_payload)
    print(f"Wrote attribution report to {config['paths']['attribution']}")
    return 0


def cmd_report(args: argparse.Namespace, config: dict[str, Any]) -> int:
    validation_payload = json.loads((Path(config["paths"]["validation_reports"]) / "validation-latest.json").read_text(encoding="utf-8"))
    attribution_payload = _load_optional_json(Path(config["paths"]["attribution"]))
    schema_payload = _load_optional_json(Path(config["paths"]["schema_evolution"]))
    violations = [Violation(**violation) for violation in validation_payload.get("violations", [])]
    attributions = [AttributionResult(**item) for item in attribution_payload.get("attributions", [])]
    changes = [SchemaChange(**item) for item in schema_payload.get("changes", [])]
    report_payload = build_report_payload(
        validation_payload,
        violations,
        attributions,
        changes,
        schema_payload.get("registry_gate", []),
    )
    write_report_outputs(Path(config["paths"]["report_json"]), Path(config["paths"]["report_pdf"]), report_payload)
    print(f"Wrote report JSON to {config['paths']['report_json']}")
    print(f"Wrote report PDF to {config['paths']['report_pdf']}")
    return 0


def cmd_stress_test(args: argparse.Namespace, config: dict[str, Any]) -> int:
    temp_dir = Path(tempfile.mkdtemp(prefix="enforcer-stress-"))
    try:
        temp_source = temp_dir / "seed_events_stress.jsonl"
        _inject_confidence_scale_violation(Path(config["datasets"][0].source), temp_source)
        stress_config = copy.deepcopy(config)
        for dataset in stress_config["datasets"]:
            dataset.source = str(temp_source)
        cmd_validate(args, stress_config)
        print(f"Stress test used source {temp_source}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    return 0


def _inject_confidence_scale_violation(source: Path, destination: Path) -> None:
    lines = source.read_text(encoding="utf-8").splitlines()
    mutated: list[str] = []
    extraction_mutated = False
    decision_mutated = False
    for line in lines:
        record = json.loads(line)
        if record.get("event_type") == "ExtractionCompleted" and not extraction_mutated:
            field_confidence = record.get("payload", {}).get("facts", {}).get("field_confidence", {})
            for key in list(field_confidence)[:1]:
                field_confidence[key] = 92.0
            extraction_mutated = True
        if record.get("event_type") == "DecisionGenerated" and not decision_mutated:
            if "payload" in record and "confidence" in record["payload"]:
                record["payload"]["confidence"] = 64.0
                decision_mutated = True
        mutated.append(json.dumps(record))
    destination.write_text("\n".join(mutated) + "\n", encoding="utf-8")


def _load_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    raise SystemExit(main())
