from __future__ import annotations

from collections import deque
import json
from pathlib import Path
from typing import Any


def load_lineage_snapshot(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    snapshot = Path(path)
    if not snapshot.exists():
        return None
    return json.loads(snapshot.read_text(encoding="utf-8"))


def downstream_consumers(snapshot: dict[str, Any] | None, dataset_name: str) -> list[str]:
    if not snapshot:
        return []
    nodes = snapshot.get("nodes", [])
    edges = snapshot.get("edges", [])
    matching_ids = {
        node["id"]
        for node in nodes
        if dataset_name in node.get("id", "") or dataset_name in node.get("name", "")
    }
    if not matching_ids:
        return []
    adjacency: dict[str, list[str]] = {}
    for edge in edges:
        adjacency.setdefault(edge.get("source_dataset_id") or edge.get("source") or "", []).append(
            edge.get("target_dataset_id") or edge.get("target") or ""
        )
    consumers: set[str] = set()
    queue = deque(matching_ids)
    seen = set(matching_ids)
    while queue:
        current = queue.popleft()
        for target in adjacency.get(current, []):
            if not target or target in seen:
                continue
            seen.add(target)
            consumers.add(target)
            queue.append(target)
    return sorted(consumers)


def evidence_files(snapshot: dict[str, Any] | None, keywords: list[str]) -> list[str]:
    if not snapshot:
        return []
    matches: list[str] = []
    for node in snapshot.get("nodes", []):
        for evidence in node.get("evidence", []):
            path = evidence.get("file_path")
            if not path:
                continue
            normalized = path.lower()
            if any(keyword.lower() in normalized for keyword in keywords):
                matches.append(path)
    return sorted(dict.fromkeys(matches))
