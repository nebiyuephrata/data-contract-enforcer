from __future__ import annotations

from datetime import UTC, date, datetime
import json
import math
from pathlib import Path
import re
from statistics import mean, pstdev
from typing import Any, Iterable


UUID_RE = re.compile(r"^[0-9a-f-]{36}$", re.IGNORECASE)
NUMERIC_RE = re.compile(r"^-?\d+(?:\.\d+)?$")


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def utc_now() -> datetime:
    return datetime.now(UTC)


def timestamp_slug() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ")


def ensure_artifact_dirs(root: Path | None = None) -> None:
    base = root or project_root()
    for relative in (
        "generated_contracts",
        "validation_reports",
        "violation_log",
        "schema_snapshots",
        "schema_snapshots/contracts",
        "schema_snapshots/lineage",
        "contract_registry",
        "enforcer_report",
        "outputs",
        "outputs/quarantine",
        "outputs/week2",
    ):
        (base / relative).mkdir(parents=True, exist_ok=True)


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            payload["__source_line"] = line_number
            records.append(payload)
    return records


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def dump_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=json_default), encoding="utf-8")


def append_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=json_default) + "\n")


def flatten_record(data: Any, prefix: str = "") -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    if isinstance(data, dict):
        for key, value in data.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            flattened.update(flatten_record(value, next_prefix))
    elif isinstance(data, list):
        flattened[prefix] = list(data)
    else:
        flattened[prefix] = data
    return flattened


def get_nested_value(record: dict[str, Any], dot_path: str) -> Any:
    current: Any = record
    for part in dot_path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


def coerce_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, list, dict)):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return None
        lowered = stripped.lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
        if NUMERIC_RE.fullmatch(stripped):
            if "." in stripped:
                return float(stripped)
            return int(stripped)
    return value


def is_uuid(value: Any) -> bool:
    return isinstance(value, str) and UUID_RE.fullmatch(value.strip()) is not None


def is_iso8601_datetime(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def is_iso8601_date(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def infer_value_type(value: Any) -> str:
    coerced = coerce_scalar(value)
    if coerced is None:
        return "null"
    if isinstance(coerced, bool):
        return "boolean"
    if isinstance(coerced, int) and not isinstance(coerced, bool):
        return "integer"
    if isinstance(coerced, float):
        return "number"
    if isinstance(coerced, list):
        return "array"
    if isinstance(coerced, dict):
        return "object"
    return "string"


def infer_column_type(values: list[Any]) -> str:
    seen = {infer_value_type(value) for value in values if value is not None}
    seen.discard("null")
    if not seen:
        return "string"
    if seen == {"integer"}:
        return "integer"
    if seen <= {"integer", "number"}:
        return "number"
    if "array" in seen:
        return "array"
    if "object" in seen:
        return "object"
    if "boolean" in seen and len(seen) == 1:
        return "boolean"
    return "string"


def numeric_values(values: list[Any]) -> list[float]:
    numbers: list[float] = []
    for value in values:
        coerced = coerce_scalar(value)
        if isinstance(coerced, bool) or coerced is None:
            continue
        if isinstance(coerced, (int, float)):
            numbers.append(float(coerced))
    return numbers


def safe_stats(values: list[Any]) -> dict[str, Any]:
    numbers = numeric_values(values)
    if not numbers:
        return {}
    stats: dict[str, Any] = {
        "min": min(numbers),
        "max": max(numbers),
        "mean": mean(numbers),
        "stddev": pstdev(numbers) if len(numbers) > 1 else 0.0,
    }
    return stats


def unique_values(values: list[Any], max_values: int = 10) -> list[Any] | None:
    cleaned = [value for value in values if value is not None]
    if not cleaned:
        return None
    deduped: list[Any] = []
    for value in cleaned:
        if value not in deduped:
            deduped.append(value)
        if len(deduped) > max_values:
            return None
    return deduped


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def cosine_distance(vec_a: list[float], vec_b: list[float]) -> float:
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 1.0
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 1.0
    cosine_similarity = dot / (norm_a * norm_b)
    return 1.0 - cosine_similarity


def latest_file(directory: Path, pattern: str) -> Path | None:
    matches = sorted(directory.glob(pattern))
    return matches[-1] if matches else None
