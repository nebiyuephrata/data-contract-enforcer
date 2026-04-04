from __future__ import annotations

from pathlib import Path

from contracts.attributor import _confidence_from_blame, attribute_violations
from contracts.models import Violation


def test_attribution_confidence_penalizes_hops():
    fresh = _confidence_from_blame(4102444800, 1)
    older_more_hops = _confidence_from_blame(4102444800, 3)
    assert fresh > older_more_hops


def test_attributor_builds_ranked_blame_chain_and_blast_radius(tmp_path, monkeypatch):
    apex_root = tmp_path / "apexLedger"
    code_dir = apex_root / "ledger" / "schema"
    code_dir.mkdir(parents=True)
    file_path = code_dir / "events.py"
    file_path.write_text("payload_confidence = 1\n", encoding="utf-8")

    monkeypatch.setattr(
        "contracts.attributor._git_log_candidates",
        lambda path: [
            {
                "commit_hash": "abc123",
                "author": "A Developer",
                "commit_timestamp": "2026-04-04T00:00:00+00:00",
                "commit_message": "Adjust confidence handling",
            }
        ],
    )

    snapshot_path = tmp_path / "lineage.json"
    snapshot_path.write_text(
        """
{
  "nodes": [
    {"id": "dataset:week5_events", "name": "week5_events", "type": "dataset", "metadata": {"dataset": "week5_events"}, "evidence": []},
    {"id": "file:events_schema", "name": "ledger/schema/events.py", "type": "code", "metadata": {"datasets": ["week5_events"], "columns": ["confidence"], "column_prefixes": ["payload."]}, "evidence": [{"file_path": "%s"}]}
  ],
  "edges": [
    {"source": "dataset:week5_events", "target": "file:events_schema"}
  ]
}
        """
        % file_path,
        encoding="utf-8",
    )

    registry_path = tmp_path / "subscriptions.yaml"
    registry_path.write_text(
        """
subscriptions:
  - contract_id: week5_events
    subscriber_id: week7_contract_enforcer
    breaking_fields:
      - field: payload.confidence
        reason: Confidence scale changes break downstream checks.
    validation_mode: ENFORCE
    contact: governance@example.com
        """,
        encoding="utf-8",
    )

    results = attribute_violations(
        [
            Violation(
                dataset="week5_events",
                column="payload.confidence",
                status="FAIL",
                severity="CRITICAL",
                category="structural",
                message="out of range",
                check_id="w5_confidence_range",
            )
        ],
        {"apexLedger": str(apex_root)},
        str(snapshot_path),
        str(registry_path),
    )
    assert results[0].check_id == "w5_confidence_range"
    assert results[0].blame_chain[0]["commit_message"] == "Adjust confidence handling"
    assert results[0].blast_radius["subscribers"][0]["subscriber_id"] == "week7_contract_enforcer"
    assert "affected_nodes" in results[0].blast_radius
