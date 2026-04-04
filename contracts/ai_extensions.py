from __future__ import annotations

from collections import defaultdict
import json
import os
from pathlib import Path
from typing import Any

try:
    from jsonschema import ValidationError, validate
except ImportError:  # pragma: no cover - fallback for partially provisioned environments
    class ValidationError(Exception):
        """Fallback validation error."""

    def validate(instance: dict[str, Any], schema: dict[str, Any]) -> None:
        required = schema.get("required", [])
        for key in required:
            if key not in instance:
                raise ValidationError(f"Missing required key: {key}")
        for key, subschema in schema.get("properties", {}).items():
            if key not in instance:
                continue
            value = instance[key]
            expected_type = subschema.get("type")
            if isinstance(expected_type, list):
                valid_types = tuple(_python_type(item) for item in expected_type)
            elif expected_type:
                valid_types = (_python_type(expected_type),)
            else:
                valid_types = ()
            if valid_types and not isinstance(value, valid_types):
                raise ValidationError(f"Key {key} expected {expected_type}, got {type(value).__name__}")
            minimum = subschema.get("minimum")
            maximum = subschema.get("maximum")
            if minimum is not None and isinstance(value, (int, float)) and value < minimum:
                raise ValidationError(f"Key {key} is below minimum {minimum}")
            if maximum is not None and isinstance(value, (int, float)) and value > maximum:
                raise ValidationError(f"Key {key} is above maximum {maximum}")
            if subschema.get("type") == "object":
                validate(value, subschema)

from .models import Violation
from .utils import clamp, cosine_distance, dump_json, utc_now


def _python_type(schema_type: str) -> type[Any]:
    return {
        "string": str,
        "number": (int, float),
        "integer": int,
        "boolean": bool,
        "object": dict,
        "array": list,
    }.get(schema_type, object)


LLM_EVENT_SCHEMAS: dict[str, dict[str, Any]] = {
    "CreditAnalysisCompleted": {
        "type": "object",
        "required": ["application_id", "session_id", "decision", "completed_at"],
        "properties": {
            "application_id": {"type": "string"},
            "session_id": {"type": "string"},
            "completed_at": {"type": "string"},
            "decision": {
                "type": "object",
                "required": ["risk_tier", "recommended_limit_usd", "confidence", "rationale"],
                "properties": {
                    "risk_tier": {"type": "string"},
                    "recommended_limit_usd": {"type": ["string", "number"]},
                    "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                    "rationale": {"type": "string"},
                },
            },
        },
    },
    "DecisionGenerated": {
        "type": "object",
        "required": ["application_id", "recommendation", "confidence", "generated_at"],
        "properties": {
            "application_id": {"type": "string"},
            "recommendation": {"type": "string"},
            "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
            "generated_at": {"type": "string"},
        },
    },
    "QualityAssessmentCompleted": {
        "type": "object",
        "required": ["package_id", "overall_confidence", "is_coherent", "assessed_at"],
        "properties": {
            "package_id": {"type": "string"},
            "overall_confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
            "is_coherent": {"type": "boolean"},
            "assessed_at": {"type": "string"},
        },
    },
}


def run_embedding_drift(
    dataset_name: str,
    records: list[dict[str, Any]],
    text_fields: list[str],
    ai_config: dict[str, Any],
    baseline_path: Path,
) -> tuple[list[dict[str, Any]], list[Violation]]:
    texts = _collect_text_samples(records, text_fields, ai_config.get("text_sample_size", 200))
    if not texts:
        return (
            [{"dataset": dataset_name, "check": "embedding_drift", "status": "ERROR", "message": "No text samples available."}],
            [
                Violation(
                    dataset=dataset_name,
                    column=None,
                    status="ERROR",
                    severity="HIGH",
                    category="ai",
                    message="Embedding drift could not run because no text samples were found.",
                )
            ],
        )
    provider_settings = _embedding_client_settings(ai_config)
    if provider_settings.get("error_message"):
        return (
            [
                {
                    "dataset": dataset_name,
                    "check": "embedding_drift",
                    "status": "ERROR",
                    "message": provider_settings["error_message"],
                }
            ],
            [
                Violation(
                    dataset=dataset_name,
                    column=None,
                    status="ERROR",
                    severity="HIGH",
                    category="ai",
                    message=f"Embedding drift skipped because {provider_settings['error_message'].rstrip('.')}.",
                )
            ],
        )
    try:
        from openai import OpenAI
    except ImportError:
        return (
            [{"dataset": dataset_name, "check": "embedding_drift", "status": "ERROR", "message": "openai package is not installed."}],
            [
                Violation(
                    dataset=dataset_name,
                    column=None,
                    status="ERROR",
                    severity="HIGH",
                    category="ai",
                    message="Embedding drift skipped because the openai package is unavailable.",
                )
            ],
        )

    client_kwargs: dict[str, Any] = {"api_key": provider_settings["api_key"]}
    if provider_settings.get("base_url"):
        client_kwargs["base_url"] = provider_settings["base_url"]
    if provider_settings.get("default_headers"):
        client_kwargs["default_headers"] = provider_settings["default_headers"]
    client = OpenAI(**client_kwargs)
    response = client.embeddings.create(model=provider_settings["model"], input=texts)
    vectors = [list(item.embedding) for item in response.data]
    centroid = [sum(values) / len(values) for values in zip(*vectors)]

    baseline_payload = {}
    if baseline_path.exists():
        baseline_payload = __import__("json").loads(baseline_path.read_text(encoding="utf-8"))
    baseline = baseline_payload.get(dataset_name)
    checks: list[dict[str, Any]] = []
    violations: list[Violation] = []
    if baseline:
        distance = cosine_distance(centroid, baseline["centroid"])
        threshold = float(ai_config.get("drift_threshold", 0.15))
        status = "FAIL" if distance > threshold else "PASS"
        checks.append(
            {
                "dataset": dataset_name,
                "check": "embedding_drift",
                "status": status,
                "distance": distance,
                "threshold": threshold,
            }
        )
        if status == "FAIL":
            violations.append(
                Violation(
                    dataset=dataset_name,
                    column=None,
                    status="FAIL",
                    severity="HIGH",
                    category="drift",
                    message=f"Embedding drift {distance:.4f} exceeded threshold {threshold:.4f}.",
                )
            )
    else:
        checks.append(
            {
                "dataset": dataset_name,
                "check": "embedding_drift",
                "status": "PASS",
                "message": "Baseline initialized.",
            }
        )
    baseline_payload[dataset_name] = {"centroid": centroid, "sample_count": len(texts), "generated_at": utc_now().isoformat()}
    dump_json(baseline_path, baseline_payload)
    return checks, violations


def run_llm_schema_checks(
    dataset_name: str,
    raw_records: list[dict[str, Any]],
    rate_path: Path,
    quarantine_dir: Path,
) -> tuple[list[dict[str, Any]], list[Violation]]:
    counts = defaultdict(lambda: {"valid": 0, "invalid": 0})
    violations: list[Violation] = []
    checks: list[dict[str, Any]] = []
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    for record in raw_records:
        event_type = record.get("event_type")
        schema = LLM_EVENT_SCHEMAS.get(event_type)
        if schema is None:
            continue
        payload = record.get("payload", {})
        try:
            validate(payload, schema)
            counts[event_type]["valid"] += 1
        except ValidationError as exc:
            counts[event_type]["invalid"] += 1
            quarantine_path = quarantine_dir / f"{dataset_name}-{event_type}-{record.get('__source_line', 'unknown')}.json"
            quarantine_path.write_text(json.dumps(record, indent=2, sort_keys=True), encoding="utf-8")
            error_message = getattr(exc, "message", str(exc))
            violations.append(
                Violation(
                    dataset=dataset_name,
                    column=f"payload ({event_type})",
                    status="FAIL",
                    severity="HIGH",
                    category="ai",
                    message=f"LLM payload schema violation: {error_message}",
                    row_locator=f"event_type={event_type}, source_line={record.get('__source_line')}",
                )
            )
    history = {}
    if rate_path.exists():
        history = json.loads(rate_path.read_text(encoding="utf-8"))
    for event_type, totals in counts.items():
        total = totals["valid"] + totals["invalid"]
        rate = 0.0 if total == 0 else totals["invalid"] / total
        previous_rate = ((history.get(dataset_name) or {}).get(event_type) or {}).get("violation_rate")
        is_rising = previous_rate is not None and rate > previous_rate
        checks.append(
            {
                "dataset": dataset_name,
                "check": f"llm_schema_{event_type}",
                "status": "PASS" if totals["invalid"] == 0 else "FAIL",
                "violation_rate": rate,
                "previous_violation_rate": previous_rate,
                "trend": "rising" if is_rising else "stable",
                "total": total,
            }
        )
        if is_rising:
            violations.append(
                Violation(
                    dataset=dataset_name,
                    column=f"payload ({event_type})",
                    status="WARN",
                    severity="MEDIUM",
                    category="ai",
                    message=(
                        f"LLM schema violation rate for {event_type} is rising from "
                        f"{previous_rate:.2%} to {rate:.2%}."
                    ),
                )
            )
        history.setdefault(dataset_name, {})[event_type] = {
            "violation_rate": rate,
            "generated_at": utc_now().isoformat(),
        }
    dump_json(rate_path, history)
    return checks, violations


def _embedding_client_settings(ai_config: dict[str, Any]) -> dict[str, Any]:
    provider = str(ai_config.get("embedding_provider", "openai")).lower()
    if provider == "openrouter":
        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            return {"error_message": "OPENROUTER_API_KEY is not configured."}
        headers = {}
        http_referer = os.getenv("OPENROUTER_HTTP_REFERER")
        app_title = os.getenv("OPENROUTER_APP_TITLE")
        if http_referer:
            headers["HTTP-Referer"] = http_referer
        if app_title:
            headers["X-Title"] = app_title
        return {
            "provider": provider,
            "api_key": api_key,
            "base_url": ai_config.get("embedding_base_url", "https://openrouter.ai/api/v1"),
            "model": ai_config.get("embedding_model", "google/gemini-embedding-001"),
            "default_headers": headers or None,
        }

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {"error_message": "OPENAI_API_KEY is not configured."}
    return {
        "provider": "openai",
        "api_key": api_key,
        "base_url": ai_config.get("embedding_base_url"),
        "model": ai_config.get("embedding_model", "text-embedding-3-small"),
    }


def _collect_text_samples(records: list[dict[str, Any]], text_fields: list[str], limit: int) -> list[str]:
    samples: list[str] = []
    for record in records:
        for field in text_fields:
            value = record.get(field)
            if isinstance(value, list):
                text = " ".join(str(item) for item in value if item)
            else:
                text = str(value).strip() if value else ""
            if text:
                samples.append(text)
            if len(samples) >= limit:
                return samples[:limit]
    return samples[:limit]
