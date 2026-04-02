from __future__ import annotations

from contracts.generator import build_contract
from contracts.models import DatasetConfig


def test_contract_marks_required_and_confidence_range():
    dataset = DatasetConfig(name="demo", source="demo.jsonl")
    records = [
        {"stream_id": "a", "payload.confidence": 0.5, "__source_line": 1},
        {"stream_id": "b", "payload.confidence": 0.8, "__source_line": 2},
    ]
    contract = build_contract(dataset, records)
    fields = {field["name"]: field for field in contract["fields"]}
    assert fields["stream_id"]["required"] is True
    assert fields["payload.confidence"]["constraints"] == {"minimum": 0.0, "maximum": 1.0}
