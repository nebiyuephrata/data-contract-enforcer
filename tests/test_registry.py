from __future__ import annotations

from contracts.registry import registry_blast_radius, registry_migration_gate


def test_registry_blast_radius_prefers_declared_subscribers():
    registry = {
        "subscriptions": [
            {
                "contract_id": "week3_extractions",
                "subscriber_id": "credit_analysis_ingestion",
                "breaking_fields": [{"field": "payload.facts.field_confidence.total_revenue", "reason": "Exact dependency."}],
            },
            {
                "contract_id": "week3_extractions",
                "subscriber_id": "decision_orchestrator_ingestion",
                "breaking_fields": [{"field": "payload.facts.field_confidence", "reason": "Prefix dependency."}],
            },
        ]
    }
    matches = registry_blast_radius(
        registry,
        "week3_extractions",
        "payload.facts.field_confidence.total_revenue",
    )
    assert [item["subscriber_id"] for item in matches] == [
        "credit_analysis_ingestion",
        "decision_orchestrator_ingestion",
    ]


def test_registry_migration_gate_passes_with_approved_plan():
    registry = {
        "migration_plans": [
            {
                "contract_id": "week5_events",
                "field_name": "payload.confidence",
                "change_type": "narrow_range",
                "status": "approved",
            }
        ]
    }
    gate = registry_migration_gate(
        registry,
        "week5_events",
        [{"field_name": "payload.confidence", "change_type": "narrow_range"}],
    )
    assert gate["status"] == "PASS"
