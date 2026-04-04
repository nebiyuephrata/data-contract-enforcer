from __future__ import annotations

import json

from contracts.ai_extensions import _embedding_client_settings, run_ai_extensions


def test_openrouter_embedding_settings_use_gemini_model(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")
    settings = _embedding_client_settings(
        {
            "embedding_provider": "openrouter",
            "embedding_model": "google/gemini-embedding-001",
            "embedding_base_url": "https://openrouter.ai/api/v1",
        }
    )
    assert settings["provider"] == "openrouter"
    assert settings["model"] == "google/gemini-embedding-001"
    assert settings["base_url"] == "https://openrouter.ai/api/v1"


def test_openrouter_embedding_settings_require_api_key(monkeypatch):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    settings = _embedding_client_settings({"embedding_provider": "openrouter"})
    assert settings["error_message"] == "OPENROUTER_API_KEY is not configured."


def test_ai_extensions_single_entry_point_runs_prompt_and_verdict_checks(tmp_path, monkeypatch):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    verdict_path = tmp_path / "verdict_records.jsonl"
    verdict_path.write_text(
        '{"dataset":"week5_events","status":"FAIL","schema_valid":false,"violation_count":1}\n',
        encoding="utf-8",
    )
    checks, violations = run_ai_extensions(
        "week5_events",
        [
            {
                "__source_line": 3,
                "event_type": "DocumentUploadRequested",
                "payload": {"required_document_types": ["income_statement"]},
                "recorded_at": "2026-04-04T10:00:00Z",
            }
        ],
        [],
        [],
        {"embedding_provider": "openrouter", "llm_violation_rate_threshold": 0.0},
        {
            "embedding_baselines": str(tmp_path / "embedding.json"),
            "quarantine": str(tmp_path / "quarantine"),
            "llm_violation_rates": str(tmp_path / "rates.json"),
            "week2_verdict_records": str(verdict_path),
            "violation_log": str(tmp_path / "violations.jsonl"),
        },
    )
    assert any(check["check"] == "prompt_input_schema_validation" for check in checks)
    assert any(check["check"] == "llm_output_violation_rate" for check in checks)
    assert any(item.status == "WARN" and item.check_id == "llm_output_violation_rate" for item in violations)
    assert json.loads((tmp_path / "rates.json").read_text(encoding="utf-8"))["week5_events"]
