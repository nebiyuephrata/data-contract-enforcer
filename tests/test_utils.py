from __future__ import annotations

from contracts.utils import flatten_record


def test_flatten_record_expands_nested_dicts_and_keeps_arrays():
    payload = {
        "payload": {
            "facts": {
                "field_confidence": {"total_revenue": 0.91},
                "tags": ["a", "b"],
            }
        }
    }
    flattened = flatten_record(payload)
    assert flattened["payload.facts.field_confidence.total_revenue"] == 0.91
    assert flattened["payload.facts.tags"] == ["a", "b"]
