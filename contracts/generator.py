from __future__ import annotations

from datetime import UTC
from pathlib import Path
from typing import Any

import yaml

from .lineage_loader import downstream_consumers, load_lineage_snapshot
from .models import DatasetConfig
from .utils import (
    coerce_scalar,
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


def build_contract(dataset: DatasetConfig, records: list[dict[str, Any]]) -> dict[str, Any]:
    profile = profile_records(records)
    snapshot = load_lineage_snapshot(dataset.lineage_snapshot)
    fields: list[dict[str, Any]] = []
    for name, metadata in profile.items():
        field: dict[str, Any] = {
            "name": name,
            "type": metadata["type"],
            "required": metadata["required"],
            "nullable": not metadata["required"],
            "null_fraction": metadata["null_fraction"],
        }
        if "format" in metadata:
            field["format"] = metadata["format"]
        if "enum" in metadata:
            field["enum"] = metadata["enum"]
        if "constraints" in metadata:
            field["constraints"] = metadata["constraints"]
        if metadata.get("stats"):
            field["stats"] = metadata["stats"]
        fields.append(field)
    return {
        "standard": "odcs",
        "kind": "data_contract",
        "version": "1.0",
        "dataset": dataset.name,
        "source": dataset.source,
        "generated_at": utc_now().isoformat(),
        "lineage": {
            "snapshot_path": dataset.lineage_snapshot,
            "downstream_consumers": downstream_consumers(snapshot, dataset.name),
        },
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
    return {"version": 2, "models": [{"name": contract["dataset"], "columns": columns}]}


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
