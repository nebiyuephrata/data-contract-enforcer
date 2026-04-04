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


def test_week3_contract_contains_meaningful_clauses():
    dataset = DatasetConfig(name="week3_extractions", source="demo.jsonl")
    contract = build_contract(dataset, [{"event_type": "ExtractionCompleted", "__source_line": 1}])
    assert len(contract["clauses"]) >= 8
    assert any(clause["id"] == "w3_field_confidence_range" for clause in contract["clauses"])


def test_generator_flags_suspicious_distribution_warning():
    dataset = DatasetConfig(name="demo", source="demo.jsonl")
    records = [
        {"almost_one": 1.0, "__source_line": 1},
        {"almost_one": 1.0, "__source_line": 2},
    ]
    contract = build_contract(dataset, records)
    field = next(item for item in contract["fields"] if item["name"] == "almost_one")
    assert any("Suspicious distribution warning" in warning for warning in field["warnings"])
