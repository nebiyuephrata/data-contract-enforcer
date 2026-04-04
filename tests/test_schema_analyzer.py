from __future__ import annotations

from pathlib import Path

import yaml

from contracts.models import SchemaChange
from contracts.schema_analyzer import build_migration_report, compare_contract_snapshots, evaluate_registry_gate


def test_schema_analyzer_detects_breaking_non_nullable_addition(tmp_path: Path):
    previous = tmp_path / "previous.yaml"
    current = tmp_path / "current.yaml"
    previous.write_text(
        yaml.safe_dump({"fields": [{"name": "a", "type": "string", "required": False}]}),
        encoding="utf-8",
    )
    current.write_text(
        yaml.safe_dump(
            {
                "fields": [
                    {"name": "a", "type": "string", "required": False},
                    {"name": "b", "type": "string", "required": True},
                ]
            }
        ),
        encoding="utf-8",
    )
    changes = compare_contract_snapshots("demo", current, previous)
    assert any(change.change_type == "add_non_nullable_field" for change in changes)


def test_registry_gate_fails_without_migration_plan():
    registry = {"migration_plans": []}
    gate = evaluate_registry_gate(
        "demo",
        [
            SchemaChange(
                dataset="demo",
                field_name="payload.amount",
                compatibility="BREAKING",
                change_type="narrow_type",
                message="Type changed from number to integer.",
            )
        ],
        registry,
    )
    assert gate["status"] == "FAIL"
    assert gate["enforcement_location"] == "producer_predeploy_gate"


def test_schema_analyzer_builds_migration_report_with_failure_modes(tmp_path: Path):
    previous = tmp_path / "previous.yaml"
    current = tmp_path / "current.yaml"
    previous.write_text(
        yaml.safe_dump(
            {
                "fields": [
                    {
                        "name": "payload.confidence",
                        "type": "number",
                        "required": False,
                        "constraints": {"minimum": 0.0, "maximum": 1.0},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    current.write_text(
        yaml.safe_dump(
            {
                "fields": [
                    {
                        "name": "payload.confidence",
                        "type": "integer",
                        "required": False,
                        "constraints": {"minimum": 0, "maximum": 100},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    registry = {
        "subscriptions": [
            {
                "contract_id": "demo",
                "subscriber_id": "week7_contract_enforcer",
                "breaking_fields": [{"field": "payload.confidence", "reason": "Scale drift breaks validators."}],
                "validation_mode": "ENFORCE",
                "contact": "gov@example.com",
            }
        ],
        "migration_plans": [],
    }
    report = build_migration_report("demo", current, previous, registry)
    assert report["compatibility_verdict"] == "BREAKING"
    assert report["rollback_plan"]
    assert report["consumer_failure_modes"][0]["subscriber_id"] == "week7_contract_enforcer"
    assert any(change["severity"] == "CRITICAL" for change in report["diff"])
