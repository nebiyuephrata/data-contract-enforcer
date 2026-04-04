from __future__ import annotations

from contracts.registry import registry_blast_radius, registry_migration_gate


def test_registry_blast_radius_prefers_declared_subscribers():
    registry = {
        "datasets": {
            "week3_extractions": {
                "subscribers": [
                    {
                        "name": "credit_analysis_ingestion",
                        "depends_on_fields": ["payload.facts.field_confidence.total_revenue"],
                        "depends_on_prefixes": [],
                    },
                    {
                        "name": "decision_orchestrator_ingestion",
                        "depends_on_fields": [],
                        "depends_on_prefixes": ["payload.facts.field_confidence."],
                    },
                ]
            }
        }
    }
    matches = registry_blast_radius(
        registry,
        "week3_extractions",
        "payload.facts.field_confidence.total_revenue",
    )
    assert [item["name"] for item in matches] == [
        "credit_analysis_ingestion",
        "decision_orchestrator_ingestion",
    ]


def test_registry_migration_gate_passes_with_approved_plan():
    registry = {
        "datasets": {
            "week5_events": {
                "migration_plans": [
                    {
                        "field_name": "payload.confidence",
                        "change_type": "narrow_range",
                        "status": "approved",
                    }
                ]
            }
        }
    }
    gate = registry_migration_gate(
        registry,
        "week5_events",
        [{"field_name": "payload.confidence", "change_type": "narrow_range"}],
    )
    assert gate["status"] == "PASS"
