from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .models import DatasetConfig
from .utils import ensure_artifact_dirs, project_root


DEFAULT_CONFIG_PATH = project_root() / "contracts" / "config.yaml"


def _resolve_path(root: Path, value: str | None) -> str | None:
    if value is None:
        return None
    path = Path(value)
    if path.is_absolute():
        return str(path)
    return str((root / path).resolve())


def load_config(path: Path | None = None) -> dict[str, Any]:
    config_path = path or DEFAULT_CONFIG_PATH
    root = project_root()
    ensure_artifact_dirs(root)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    datasets: list[DatasetConfig] = []
    for name, raw in payload["datasets"].items():
        datasets.append(
            DatasetConfig(
                name=name,
                source=_resolve_path(root, raw["source"]) or "",
                filter_equals=raw.get("filter_equals", {}),
                baseline_namespace=raw.get("baseline_namespace", name),
                primary_key_fields=raw.get("primary_key_fields", []),
                contract_path=_resolve_path(root, raw["contract_path"]) or "",
                dbt_schema_path=_resolve_path(root, raw["dbt_schema_path"]) or "",
                text_fields=raw.get("text_fields", []),
                lineage_snapshot=_resolve_path(root, raw.get("lineage_snapshot")),
            )
        )
    payload["datasets"] = datasets
    payload["project_root"] = str(root)
    payload["paths"] = {
        key: _resolve_path(root, value) for key, value in payload.get("paths", {}).items()
    }
    payload["repositories"] = {
        key: _resolve_path(root, value) for key, value in payload.get("repositories", {}).items()
    }
    return payload
