from __future__ import annotations

from pathlib import Path

import yaml

from contracts.models import SchemaChange
from contracts.schema_analyzer import compare_contract_snapshots, evaluate_registry_gate


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
    registry = {"datasets": {"demo": {"migration_plans": []}}}
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
