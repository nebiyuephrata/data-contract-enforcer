from __future__ import annotations

import json

from contracts.lineage_loader import downstream_consumers, lineage_candidate_files


def test_lineage_loader_returns_downstream_consumers_and_candidate_files():
    snapshot = {
        "nodes": [
            {"id": "dataset:week3_extractions", "name": "week3_extractions", "metadata": {"dataset": "week3_extractions"}, "evidence": []},
            {
                "id": "file:document_processing_agent",
                "name": "document_processing",
                "metadata": {
                    "datasets": ["week3_extractions"],
                    "columns": ["field_confidence", "processing_ms"],
                    "column_prefixes": ["payload.facts.field_confidence."],
                },
                "evidence": [{"file_path": "/tmp/document_processing_agent.py"}],
            },
            {"id": "dataset:week5_events", "name": "week5_events", "metadata": {"dataset": "week5_events"}, "evidence": []},
        ],
        "edges": [
            {"source": "dataset:week3_extractions", "target": "file:document_processing_agent"},
            {"source": "dataset:week3_extractions", "target": "dataset:week5_events"},
        ],
    }
    consumers = downstream_consumers(snapshot, "week3_extractions")
    candidates = lineage_candidate_files(snapshot, "week3_extractions", "payload.facts.field_confidence.total_revenue")
    assert "dataset:week5_events" in consumers
    assert candidates[0]["file_path"] == "/tmp/document_processing_agent.py"
