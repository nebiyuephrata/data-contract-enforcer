from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any
from uuid import uuid4

from .utils import utc_now


def _violation_id() -> str:
    return f"violation-{uuid4().hex[:12]}"


def _detected_at() -> str:
    return utc_now().isoformat()


@dataclass(slots=True)
class DatasetConfig:
    name: str
    source: str
    filter_equals: dict[str, Any] = field(default_factory=dict)
    baseline_namespace: str = ""
    primary_key_fields: list[str] = field(default_factory=list)
    contract_path: str = ""
    dbt_schema_path: str = ""
    text_fields: list[str] = field(default_factory=list)
    lineage_snapshot: str | None = None


@dataclass(slots=True)
class Violation:
    dataset: str
    column: str | None
    status: str
    severity: str
    category: str
    message: str
    violation_id: str = field(default_factory=_violation_id)
    detected_at: str = field(default_factory=_detected_at)
    check_id: str | None = None
    row_locator: str | None = None
    expected: Any | None = None
    actual: Any | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AttributionResult:
    violation_id: str
    check_id: str
    detected_at: str
    dataset: str
    column: str
    blame_chain: list[dict[str, Any]] = field(default_factory=list)
    blast_radius: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SchemaChange:
    dataset: str
    field_name: str
    compatibility: str
    change_type: str
    message: str
    severity: str = "MEDIUM"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
