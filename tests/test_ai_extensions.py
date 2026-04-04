from __future__ import annotations

from contracts.ai_extensions import _embedding_client_settings


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
