from __future__ import annotations

from contracts.models import DatasetConfig
from contracts.runner import validate_dataset


def test_drift_thresholds_warn_and_fail():
    dataset = DatasetConfig(name="demo", source="demo.jsonl")
    contract = {
        "fields": [
            {"name": "metric", "type": "number", "required": True, "nullable": False, "null_fraction": 0.0},
        ]
    }
    warn_summary, warn_violations = validate_dataset(
        dataset,
        contract,
        [{"metric": 14.5}],
        {"demo": {"metric": {"mean": 10.0, "stddev": 2.0, "sample_size": 30}}},
        {"min_observations_for_drift": 20},
    )
    assert any(item.status == "WARN" for item in warn_violations)

    fail_summary, fail_violations = validate_dataset(
        dataset,
        contract,
        [{"metric": 17.0}],
        {"demo": {"metric": {"mean": 10.0, "stddev": 2.0, "sample_size": 30}}},
        {"min_observations_for_drift": 20},
    )
    assert any(item.status == "FAIL" and item.category == "drift" for item in fail_violations)


def test_clause_logic_detects_invalid_confidence_scale():
    dataset = DatasetConfig(name="week3_extractions", source="demo.jsonl")
    contract = {
        "fields": [
            {"name": "payload.facts.field_confidence.total_revenue", "type": "number", "required": True, "nullable": False, "null_fraction": 0.0},
        ],
        "clauses": [
            {
                "id": "w3_field_confidence_range",
                "check": {
                    "type": "prefix_range",
                    "field_prefix": "payload.facts.field_confidence.",
                    "minimum": 0.0,
                    "maximum": 1.0,
                },
            }
        ],
    }
    summary, violations = validate_dataset(
        dataset,
        contract,
        [{"payload.facts.field_confidence.total_revenue": 92.0, "__source_line": 1}],
        {},
        {"min_observations_for_drift": 20},
    )
    assert any("w3_field_confidence_range" in item.message for item in violations)


def test_validation_summary_includes_report_fields_and_warn_mode_action():
    dataset = DatasetConfig(name="demo", source="demo.jsonl")
    contract = {
        "id": "demo",
        "generated_at": "2026-04-04T00:00:00Z",
        "fields": [
            {"name": "required_metric", "type": "number", "required": True, "nullable": False, "null_fraction": 0.0},
        ],
        "clauses": [{"id": "demo_clause", "description": "demo", "check": {"type": "min_value", "field": "required_metric", "minimum": 1}}],
    }
    summary, violations = validate_dataset(
        dataset,
        contract,
        [{"required_metric": None, "__source_line": 1}],
        {},
        {"min_observations_for_drift": 20},
        mode="WARN",
    )
    assert violations
    assert summary["contract_id"] == "demo"
    assert summary["snapshot_id"] == "2026-04-04T00:00:00Z"
    assert summary["pipeline_action"] == "BLOCK"
    assert summary["total_checks"] >= 1
    assert any(result["check_id"] == "required_metric.required" for result in summary["results"])
