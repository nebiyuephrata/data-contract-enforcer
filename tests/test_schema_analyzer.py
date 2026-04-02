from __future__ import annotations

from pathlib import Path

import yaml

from contracts.schema_analyzer import compare_contract_snapshots


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
