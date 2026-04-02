from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


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
    row_locator: str | None = None
    expected: Any | None = None
    actual: Any | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AttributionResult:
    dataset: str
    column: str
    file_path: str
    line_number: int | None
    commit_hash: str | None
    author: str | None
    confidence: float
    lineage_hops: int
    rationale: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SchemaChange:
    dataset: str
    field_name: str
    compatibility: str
    change_type: str
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
