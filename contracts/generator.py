from __future__ import annotations

from datetime import UTC
import os
from pathlib import Path
from typing import Any

import yaml

from .lineage_loader import downstream_consumers, load_lineage_snapshot
from .models import DatasetConfig
from .registry import load_registry, registry_contract_subscribers
from .utils import (
    dump_json,
    flatten_record,
    infer_column_type,
    is_iso8601_date,
    is_iso8601_datetime,
    is_uuid,
    load_jsonl,
    numeric_values,
    project_root,
    safe_stats,
    timestamp_slug,
    unique_values,
    utc_now,
)


def load_dataset_records(dataset: DatasetConfig) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    raw_records = load_jsonl(Path(dataset.source))
    filtered: list[dict[str, Any]] = []
    flattened: list[dict[str, Any]] = []
    for record in raw_records:
        if not _matches_filter(record, dataset.filter_equals):
            continue
        filtered.append(record)
        flattened_record = flatten_record(record)
        flattened_record["__source_line"] = record.get("__source_line")
        flattened.append(flattened_record)
    return filtered, flattened


def _matches_filter(record: dict[str, Any], expected: dict[str, Any]) -> bool:
    for key, value in expected.items():
        if record.get(key) != value:
            return False
    return True


def profile_records(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    columns = sorted({column for record in records for column in record})
    profile: dict[str, dict[str, Any]] = {}
    for column in columns:
        values = [record.get(column) for record in records]
        non_null = [value for value in values if value is not None]
        inferred_type = infer_column_type(non_null)
        column_profile: dict[str, Any] = {
            "sample_size": len(values),
            "non_null_count": len(non_null),
            "null_fraction": 0.0 if not values else (len(values) - len(non_null)) / len(values),
            "type": inferred_type,
            "required": bool(values) and all(value is not None for value in values),
            "cardinality_estimate": len({repr(value) for value in non_null}),
            "sample_values": [str(value) for value in non_null[:5]],
        }
        if non_null:
            sample_value = next((value for value in non_null if value is not None), None)
            if is_uuid(sample_value):
                column_profile["format"] = "uuid"
            elif is_iso8601_datetime(sample_value):
                column_profile["format"] = "date-time"
            elif is_iso8601_date(sample_value):
                column_profile["format"] = "date"
        if inferred_type in {"integer", "number"}:
            column_profile["stats"] = safe_stats(non_null)
        enums = unique_values(non_null)
        if enums and inferred_type == "string":
            column_profile["enum"] = enums
        if "confidence" in column.lower():
            column_profile["constraints"] = {"minimum": 0.0, "maximum": 1.0}
        profile[column] = column_profile
    return profile


def build_contract(
    dataset: DatasetConfig,
    records: list[dict[str, Any]],
    registry_path: str | None = None,
) -> dict[str, Any]:
    profile = profile_records(records)
    snapshot = load_lineage_snapshot(dataset.lineage_snapshot)
    registry = load_registry(registry_path or str(project_root() / "contract_registry" / "subscriptions.yaml"))
    annotations = annotate_ambiguous_columns(dataset.name, profile)
    fields: list[dict[str, Any]] = []
    for name, metadata in profile.items():
        field: dict[str, Any] = {
            "name": name,
            "type": metadata["type"],
            "required": metadata["required"],
            "nullable": not metadata["required"],
            "null_fraction": metadata["null_fraction"],
            "description": annotations.get(name, f"Profiled field {name} for dataset {dataset.name}."),
        }
        warnings: list[str] = []
        if "format" in metadata:
            field["format"] = metadata["format"]
        if "enum" in metadata:
            field["enum"] = metadata["enum"]
        if "constraints" in metadata:
            field["constraints"] = metadata["constraints"]
        if metadata.get("stats"):
            field["stats"] = metadata["stats"]
            warning = suspicious_distribution_warning(name, metadata["stats"])
            if warning:
                warnings.append(warning)
        if warnings:
            field["warnings"] = warnings
        fields.append(field)
    clauses = build_contract_clauses(dataset)
    return {
        "id": dataset.name,
        "standard": "odcs",
        "kind": "data_contract",
        "version": "1.0",
        "dataset": dataset.name,
        "source": dataset.source,
        "generated_at": utc_now().isoformat(),
        "lineage": {
            "snapshot_path": dataset.lineage_snapshot,
            "downstream_consumers": downstream_consumers(snapshot, dataset.name),
            "registry_subscribers": [
                subscriber.get("subscriber_id")
                for subscriber in registry_contract_subscribers(registry, dataset.name)
                if subscriber.get("subscriber_id")
            ],
            "note": "Blast radius uses registry_subscribers as the primary source. Lineage consumers are enrichment only.",
        },
        "clauses": clauses,
        "fields": fields,
    }


def build_dbt_schema(contract: dict[str, Any]) -> dict[str, Any]:
    columns = []
    for field in contract["fields"]:
        tests = []
        if field["required"]:
            tests.append("not_null")
        if "enum" in field:
            tests.append({"accepted_values": {"values": field["enum"]}})
        if field.get("constraints"):
            tests.append({"range": field["constraints"]})
        columns.append(
            {
                "name": field["name"],
                "description": f"Inferred {field['type']} field from {contract['dataset']}",
                "tests": tests,
            }
        )
    return {
        "version": 2,
        "models": [
            {
                "name": contract["dataset"],
                "tests": build_dbt_model_tests(contract["dataset"]),
                "columns": columns,
            }
        ],
    }


def write_contract_outputs(dataset: DatasetConfig, contract: dict[str, Any], dbt_schema: dict[str, Any]) -> dict[str, str]:
    contract_path = Path(dataset.contract_path)
    dbt_path = Path(dataset.dbt_schema_path)
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    dbt_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    dbt_path.write_text(yaml.safe_dump(dbt_schema, sort_keys=False), encoding="utf-8")

    snapshot_dir = project_root() / "schema_snapshots" / "contracts" / dataset.name
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = snapshot_dir / f"{timestamp_slug()}.yaml"
    snapshot_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    latest_path = snapshot_dir / "latest.yaml"
    latest_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    return {
        "contract_path": str(contract_path),
        "dbt_schema_path": str(dbt_path),
        "snapshot_path": str(snapshot_path),
    }


def build_baselines(dataset: DatasetConfig, records: list[dict[str, Any]]) -> dict[str, Any]:
    profile = profile_records(records)
    baselines: dict[str, Any] = {}
    for column, metadata in profile.items():
        stats = metadata.get("stats")
        if not stats:
            continue
        numeric_count = len(numeric_values([record.get(column) for record in records]))
        baselines[column] = {
            "dataset": dataset.name,
            "column": column,
            "mean": stats["mean"],
            "stddev": stats["stddev"],
            "sample_size": numeric_count,
            "generated_at": utc_now().astimezone(UTC).isoformat(),
        }
    return baselines


def write_baselines(dataset: DatasetConfig, records: list[dict[str, Any]], output_path: Path) -> dict[str, Any]:
    baselines = build_baselines(dataset, records)
    payload = {dataset.name: baselines}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dump_json(output_path, payload)
    return payload


def build_contract_clauses(dataset: DatasetConfig) -> list[dict[str, Any]]:
    if dataset.name == "week3_extractions":
        return [
            {
                "id": "w3_event_type_fixed",
                "description": "The extraction dataset must only contain ExtractionCompleted events.",
                "check": {"type": "accepted_values", "field": "event_type", "values": ["ExtractionCompleted"]},
            },
            {
                "id": "w3_document_type_enum",
                "description": "Only income statement and balance sheet extraction payloads are valid in the pilot.",
                "check": {
                    "type": "accepted_values",
                    "field": "payload.document_type",
                    "values": ["income_statement", "balance_sheet"],
                },
            },
            {
                "id": "w3_currency_usd",
                "description": "Extracted financial facts must use USD in the pilot baseline.",
                "check": {"type": "accepted_values", "field": "payload.facts.currency", "values": ["USD"]},
            },
            {
                "id": "w3_field_confidence_range",
                "description": "All field confidence scores must remain on the 0.0 to 1.0 scale.",
                "check": {
                    "type": "prefix_range",
                    "field_prefix": "payload.facts.field_confidence.",
                    "minimum": 0.0,
                    "maximum": 1.0,
                },
            },
            {
                "id": "w3_processing_time_positive",
                "description": "Extraction processing time must be a positive value.",
                "check": {"type": "min_value", "field": "payload.processing_ms", "minimum": 1},
            },
            {
                "id": "w3_raw_text_positive",
                "description": "Completed extractions must include non-zero raw text length.",
                "check": {"type": "min_value", "field": "payload.raw_text_length", "minimum": 1},
            },
            {
                "id": "w3_tables_extracted_nonnegative",
                "description": "Completed extractions must report at least one extracted table in this pilot corpus.",
                "check": {"type": "min_value", "field": "payload.tables_extracted", "minimum": 1},
            },
            {
                "id": "w3_assets_cover_cash",
                "description": "Current assets must be greater than or equal to cash and equivalents.",
                "check": {
                    "type": "field_gte",
                    "left_field": "payload.facts.current_assets",
                    "right_field": "payload.facts.cash_and_equivalents",
                },
            },
            {
                "id": "w3_completion_after_recorded",
                "description": "Extraction completion time cannot occur before the event recorded time.",
                "check": {
                    "type": "datetime_order",
                    "earlier_field": "recorded_at",
                    "later_field": "payload.completed_at",
                },
            },
            {
                "id": "w3_balance_sheet_integrity",
                "description": "Balance sheet extractions must explicitly report that the balance sheet balances.",
                "check": {
                    "type": "conditional_equals",
                    "when_field": "payload.document_type",
                    "when_equals": "balance_sheet",
                    "field": "payload.facts.balance_sheet_balances",
                    "value": True,
                },
            },
        ]
    if dataset.name == "week5_events":
        return [
        {
            "id": "w5_stream_prefix",
            "description": "Event stream ids must use one of the known aggregate prefixes.",
            "check": {
                "type": "startswith_any",
                "field": "stream_id",
                "prefixes": ["loan-", "docpkg-", "agent-", "credit-", "compliance-", "fraud-", "audit-"],
            },
        },
        {
            "id": "w5_event_version_positive",
            "description": "All event versions must be at least 1.",
            "check": {"type": "min_value", "field": "event_version", "minimum": 1},
        },
        {
            "id": "w5_recorded_at_iso",
            "description": "Every event must have an ISO-8601 recorded timestamp.",
            "check": {"type": "format", "field": "recorded_at", "format": "date-time"},
        },
        {
            "id": "w5_agent_type_enum",
            "description": "Agent events must use a recognized agent type when present.",
            "check": {
                "type": "accepted_values_if_present",
                "field": "payload.agent_type",
                "values": ["document_processing", "credit_analysis", "fraud_detection", "compliance", "decision_orchestrator"],
            },
        },
        {
            "id": "w5_confidence_range",
            "description": "Top-level confidence values must remain on the 0.0 to 1.0 scale.",
            "check": {"type": "range_if_present", "field": "payload.confidence", "minimum": 0.0, "maximum": 1.0},
        },
        {
            "id": "w5_decision_confidence_range",
            "description": "Nested decision confidence values must remain on the 0.0 to 1.0 scale.",
            "check": {
                "type": "range_if_present",
                "field": "payload.decision.confidence",
                "minimum": 0.0,
                "maximum": 1.0,
            },
        },
        {
            "id": "w5_document_format_enum",
            "description": "Document format values must stay within the known format set.",
            "check": {
                "type": "accepted_values_if_present",
                "field": "payload.document_format",
                "values": ["pdf", "xlsx", "csv"],
            },
        },
        {
            "id": "w5_decision_confidence_floor",
            "description": "DecisionGenerated events below the confidence floor must resolve to REFER.",
            "check": {
                "type": "conditional_numeric_floor_requires_value",
                "when_field": "event_type",
                "when_equals": "DecisionGenerated",
                "numeric_field": "payload.confidence",
                "less_than": 0.60,
                "required_field": "payload.recommendation",
                "required_value": "REFER",
            },
        },
        {
            "id": "w5_extraction_runtime_positive",
            "description": "ExtractionCompleted events must report positive runtime and text volume.",
            "check": {
                "type": "conditional_min_pair",
                "when_field": "event_type",
                "when_equals": "ExtractionCompleted",
                "checks": [
                    {"field": "payload.processing_ms", "minimum": 1},
                    {"field": "payload.raw_text_length", "minimum": 1},
                ],
            },
        },
        {
            "id": "w5_deadline_after_request",
            "description": "Document upload deadlines must occur after the event recorded timestamp.",
            "check": {
                "type": "conditional_datetime_order",
                "when_field": "event_type",
                "when_equals": "DocumentUploadRequested",
                "earlier_field": "recorded_at",
                "later_field": "payload.deadline",
            },
        },
        {
            "id": "w5_credit_decision_enum",
            "description": "CreditAnalysisCompleted events must use a recognized risk tier and positive limit.",
            "check": {
                "type": "conditional_composite",
                "when_field": "event_type",
                "when_equals": "CreditAnalysisCompleted",
                "checks": [
                    {
                        "type": "accepted_values",
                        "field": "payload.decision.risk_tier",
                        "values": ["LOW", "MEDIUM", "HIGH"],
                    },
                    {
                        "type": "min_value",
                        "field": "payload.decision.recommended_limit_usd",
                        "minimum": 1,
                    },
                ],
            },
        },
        ]
    return []


def suspicious_distribution_warning(column_name: str, stats: dict[str, Any]) -> str | None:
    mean_value = stats.get("mean")
    if mean_value is None:
        return None
    normalized_like = (stats.get("min", 0.0) >= 0.0 and stats.get("max", 1.0) <= 1.0) or any(
        token in column_name.lower() for token in ("confidence", "ratio", "rate", "score")
    )
    if not normalized_like:
        return None
    if mean_value > 0.99:
        return f"Suspicious distribution warning: {column_name} mean is above 0.99 and may indicate saturation or scale drift."
    if mean_value < 0.01:
        return f"Suspicious distribution warning: {column_name} mean is below 0.01 and may indicate sparse or mis-scaled values."
    return None


def annotate_ambiguous_columns(dataset_name: str, profile: dict[str, dict[str, Any]]) -> dict[str, str]:
    annotations: dict[str, str] = {}
    for column_name, metadata in profile.items():
        if not _is_ambiguous_column(column_name, metadata):
            continue
        annotations[column_name] = _llm_or_heuristic_annotation(dataset_name, column_name, metadata)
    return annotations


def _is_ambiguous_column(column_name: str, metadata: dict[str, Any]) -> bool:
    if metadata.get("type") != "string":
        return False
    if metadata.get("enum"):
        return False
    suffixes = ("_id", "_at", "_date")
    return not column_name.endswith(suffixes)


def _llm_or_heuristic_annotation(dataset_name: str, column_name: str, metadata: dict[str, Any]) -> str:
    provider = "openrouter" if os.getenv("OPENROUTER_API_KEY") else "openai" if os.getenv("OPENAI_API_KEY") else None
    if provider:
        try:
            return _llm_annotation_call(dataset_name, column_name, metadata, provider)
        except Exception:
            pass
    return (
        f"Ambiguous free-text field inferred from {dataset_name}; sample values include "
        f"{metadata.get('sample_values', [])[:3]}. Review semantic meaning with the domain owner."
    )


def _llm_annotation_call(dataset_name: str, column_name: str, metadata: dict[str, Any], provider: str) -> str:
    from openai import OpenAI

    if provider == "openrouter":
        client = OpenAI(
            api_key=os.getenv("OPENROUTER_API_KEY"),
            base_url="https://openrouter.ai/api/v1",
        )
        model = "google/gemini-2.5-flash"
    else:
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        model = "gpt-4.1-mini"

    response = client.responses.create(
        model=model,
        input=[
            {
                "role": "system",
                "content": "You summarize ambiguous contract columns in one short sentence for a data contract.",
            },
            {
                "role": "user",
                "content": (
                    f"Dataset: {dataset_name}\n"
                    f"Column: {column_name}\n"
                    f"Type: {metadata.get('type')}\n"
                    f"Sample values: {metadata.get('sample_values', [])[:5]}\n"
                    "Return one plain-English sentence explaining what this field likely represents."
                ),
            },
        ],
    )
    return getattr(response, "output_text", "").strip() or f"Ambiguous field {column_name} in dataset {dataset_name}."


def build_dbt_model_tests(dataset_name: str) -> list[dict[str, Any]]:
    if dataset_name == "week3_extractions":
        return [
            {
                "dbt_utils.unique_combination_of_columns": {
                    "combination_of_columns": ["stream_id", "payload.package_id", "payload.document_id"],
                }
            },
            {"dbt_utils.expression_is_true": {"expression": "payload_processing_ms > 0"}},
            {"dbt_utils.expression_is_true": {"expression": "payload_raw_text_length > 0"}},
            {"dbt_utils.expression_is_true": {"expression": "payload_tables_extracted >= 1"}},
            {
                "dbt_utils.expression_is_true": {
                    "expression": "payload_facts_current_assets >= payload_facts_cash_and_equivalents"
                }
            },
            {
                "dbt_utils.expression_is_true": {
                    "expression": "payload_facts_field_confidence_total_revenue between 0 and 1"
                }
            },
            {
                "dbt_utils.expression_is_true": {
                    "expression": "payload_facts_field_confidence_net_income between 0 and 1"
                }
            },
            {
                "dbt_utils.expression_is_true": {
                    "expression": "payload_facts_field_confidence_total_assets between 0 and 1"
                }
            },
        ]
    return [
        {"unique": {"column_name": "__source_line"}},
        {
            "dbt_utils.unique_combination_of_columns": {
                "combination_of_columns": ["stream_id", "event_type", "recorded_at", "__source_line"],
            }
        },
        {"dbt_utils.expression_is_true": {"expression": "event_version >= 1"}},
        {
            "dbt_utils.expression_is_true": {
                "expression": "case when payload_confidence is null then true else payload_confidence between 0 and 1 end"
            }
        },
        {
            "dbt_utils.expression_is_true": {
                "expression": "case when payload_decision_confidence is null then true else payload_decision_confidence between 0 and 1 end"
            }
        },
        {
            "dbt_utils.expression_is_true": {
                "expression": "case when event_type = 'DecisionGenerated' and payload_confidence < 0.60 then payload_recommendation = 'REFER' else true end"
            }
        },
        {
            "dbt_utils.expression_is_true": {
                "expression": "case when event_type = 'ExtractionCompleted' then payload_processing_ms > 0 and payload_raw_text_length > 0 else true end"
            }
        },
        {
            "dbt_utils.expression_is_true": {
                "expression": "case when event_type = 'DocumentUploadRequested' then payload_deadline > recorded_at else true end"
            }
        },
    ]
