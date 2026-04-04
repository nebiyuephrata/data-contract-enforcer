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


def lineage_candidate_files(
    snapshot: dict[str, Any] | None,
    dataset_name: str,
    column_name: str | None = None,
) -> list[dict[str, Any]]:
    if not snapshot:
        return []
    nodes = {node["id"]: node for node in snapshot.get("nodes", [])}
    adjacency: dict[str, list[str]] = {}
    for edge in snapshot.get("edges", []):
        source = edge.get("source_dataset_id") or edge.get("source") or ""
        target = edge.get("target_dataset_id") or edge.get("target") or ""
        if not source or not target:
            continue
        adjacency.setdefault(source, []).append(target)

    start_nodes = [node_id for node_id, node in nodes.items() if _matches_dataset_node(node, dataset_name)]
    column_tokens = _column_tokens(column_name)
    ranked: list[dict[str, Any]] = []
    seen_files: set[str] = set()

    queue = deque((node_id, 0) for node_id in start_nodes)
    seen_nodes = set(start_nodes)
    while queue:
        node_id, hops = queue.popleft()
        node = nodes.get(node_id, {})
        for file_path in _node_evidence_matches(node, dataset_name, column_tokens):
            if file_path in seen_files:
                continue
            ranked.append(
                {
                    "file_path": file_path,
                    "lineage_hops": hops,
                    "source_node": node_id,
                }
            )
            seen_files.add(file_path)
        for target in adjacency.get(node_id, []):
            if target in seen_nodes:
                continue
            seen_nodes.add(target)
            queue.append((target, hops + 1))
    return ranked


def traverse_lineage(snapshot: dict[str, Any] | None, dataset_name: str) -> list[dict[str, Any]]:
    if not snapshot:
        return []
    nodes = {node["id"]: node for node in snapshot.get("nodes", [])}
    adjacency: dict[str, list[str]] = {}
    for edge in snapshot.get("edges", []):
        source = edge.get("source_dataset_id") or edge.get("source") or ""
        target = edge.get("target_dataset_id") or edge.get("target") or ""
        if not source or not target:
            continue
        adjacency.setdefault(source, []).append(target)

    start_nodes = [node_id for node_id, node in nodes.items() if _matches_dataset_node(node, dataset_name)]
    traversed: list[dict[str, Any]] = []
    queue = deque((node_id, 0, [node_id]) for node_id in start_nodes)
    seen = set(start_nodes)
    while queue:
        node_id, depth, path = queue.popleft()
        if depth > 0:
            node = nodes.get(node_id, {})
            traversed.append(
                {
                    "node_id": node_id,
                    "depth": depth,
                    "path": path,
                    "type": node.get("type"),
                    "name": node.get("name", node_id),
                    "metadata": node.get("metadata", {}),
                }
            )
        for target in adjacency.get(node_id, []):
            if target in seen:
                continue
            seen.add(target)
            queue.append((target, depth + 1, [*path, target]))
    return traversed


def lineage_blast_radius(snapshot: dict[str, Any] | None, dataset_name: str) -> dict[str, Any]:
    traversed = traverse_lineage(snapshot, dataset_name)
    affected_nodes = [
        {
            "node_id": item["node_id"],
            "name": item["name"],
            "type": item["type"],
            "depth": item["depth"],
        }
        for item in traversed
    ]
    affected_pipelines = sorted(
        {
            item["name"]
            for item in traversed
            if item["type"] in {"code", "pipeline", "job"}
        }
    )
    contamination_depth = max((item["depth"] for item in traversed), default=0)
    return {
        "affected_nodes": affected_nodes,
        "affected_pipelines": affected_pipelines,
        "contamination_depth": contamination_depth,
    }


def _matches_dataset_node(node: dict[str, Any], dataset_name: str) -> bool:
    metadata = node.get("metadata", {})
    if metadata.get("dataset") == dataset_name:
        return True
    node_id = node.get("id", "")
    node_name = node.get("name", "")
    return dataset_name in node_id or dataset_name in node_name


def _column_tokens(column_name: str | None) -> list[str]:
    if not column_name:
        return []
    ignored = {"payload", "facts", "decision"}
    return [part.lower() for part in column_name.split(".") if part and part.lower() not in ignored]


def _node_evidence_matches(node: dict[str, Any], dataset_name: str, column_tokens: list[str]) -> list[str]:
    metadata = node.get("metadata", {})
    node_columns = [value.lower() for value in metadata.get("columns", [])]
    column_prefixes = [value.lower() for value in metadata.get("column_prefixes", [])]
    node_datasets = [value.lower() for value in metadata.get("datasets", [])]
    dataset_match = not node_datasets or dataset_name.lower() in node_datasets
    token_match = True
    if column_tokens:
        token_match = any(token in node_columns for token in column_tokens) or any(
            any(token.startswith(prefix.rstrip(".")) or prefix in token for prefix in column_prefixes)
            for token in column_tokens
        )
    if not dataset_match or not token_match:
        return []
    return [evidence.get("file_path") for evidence in node.get("evidence", []) if evidence.get("file_path")]
