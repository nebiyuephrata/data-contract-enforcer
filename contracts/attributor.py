from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import subprocess
from typing import Any

from .lineage_loader import (
    downstream_consumers,
    lineage_blast_radius,
    lineage_candidate_files,
    load_lineage_snapshot,
)
from .models import AttributionResult, Violation
from .registry import load_registry, registry_blast_radius
from .utils import clamp


DEFAULT_APEX_FILES = [
    "ledger/schema/events.py",
    "datagen/event_simulator.py",
]

CONFIDENCE_FILES = [
    "ledger/agents/document_processing_agent.py",
    "ledger/agents/credit_analysis_agent.py",
    "ledger/agents/decision_orchestrator_agent.py",
]


def attribute_violations(
    violations: list[Violation],
    repositories: dict[str, str],
    lineage_snapshot_path: str | None = None,
    registry_path: str | None = None,
) -> list[AttributionResult]:
    results: list[AttributionResult] = []
    registry = load_registry(registry_path)
    snapshot = load_lineage_snapshot(lineage_snapshot_path)
    apex_root = Path(repositories["apexLedger"])

    for violation in violations:
        if violation.status not in {"FAIL", "ERROR"} or not violation.column:
            continue

        # Registry-first sourcing ensures subscriber impact is established before graph traversal.
        registry_matches = registry_blast_radius(registry, violation.dataset, violation.column)
        candidate_files = _candidate_files(violation, apex_root, snapshot)
        ranked_candidates = _ranked_blame_chain(candidate_files, violation.column)
        traversal_radius = lineage_blast_radius(snapshot, violation.dataset)
        blast_radius = {
            "subscribers": [
                {
                    "subscriber_id": item.get("subscriber_id"),
                    "validation_mode": item.get("validation_mode"),
                    "reason": item.get("reason"),
                    "contact": item.get("contact"),
                }
                for item in registry_matches
            ],
            "affected_nodes": traversal_radius["affected_nodes"],
            "affected_pipelines": traversal_radius["affected_pipelines"],
            "contamination_depth": traversal_radius["contamination_depth"],
            "transitive_consumers": downstream_consumers(snapshot, violation.dataset),
        }
        results.append(
            AttributionResult(
                violation_id=violation.violation_id,
                check_id=violation.check_id or _infer_check_id(violation),
                detected_at=violation.detected_at,
                dataset=violation.dataset,
                column=violation.column,
                blame_chain=ranked_candidates[:5],
                blast_radius=blast_radius,
            )
        )
    return results


def _ranked_blame_chain(candidate_files: list[dict[str, Any]], column_name: str) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for candidate_info in candidate_files:
        candidate = candidate_info["path"]
        hops = candidate_info["lineage_hops"]
        line_number = _find_line_number(candidate, column_name)
        log_entries = _git_log_candidates(candidate)
        if not log_entries:
            ranked.append(
                {
                    "commit_hash": None,
                    "author": None,
                    "commit_timestamp": None,
                    "commit_message": f"No git history found for {candidate.name}.",
                    "confidence_score": _confidence_from_commit_timestamp(None, hops),
                    "file_path": str(candidate),
                    "line_number": line_number,
                    "lineage_hops": hops,
                    "rationale": candidate_info["rationale"],
                }
            )
            continue
        for log_entry in log_entries:
            ranked.append(
                {
                    "commit_hash": log_entry["commit_hash"],
                    "author": log_entry["author"],
                    "commit_timestamp": log_entry["commit_timestamp"],
                    "commit_message": log_entry["commit_message"],
                    "confidence_score": _confidence_from_commit_timestamp(log_entry["commit_timestamp"], hops),
                    "file_path": str(candidate),
                    "line_number": line_number,
                    "lineage_hops": hops,
                    "rationale": candidate_info["rationale"],
                }
            )
    ranked.sort(key=lambda item: item["confidence_score"], reverse=True)
    return ranked


def _candidate_files(
    violation: Violation,
    apex_root: Path,
    snapshot: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    snapshot_candidates = lineage_candidate_files(snapshot, violation.dataset, violation.column)
    for item in snapshot_candidates:
        path_obj = Path(item["file_path"])
        if path_obj.exists():
            candidates.append(
                {
                    "path": path_obj,
                    "lineage_hops": max(1, item["lineage_hops"]),
                    "rationale": f"Matched {violation.column} through lineage node {item['source_node']}.",
                }
            )
    for relative in DEFAULT_APEX_FILES:
        path = apex_root / relative
        if path.exists():
            candidates.append(
                {
                    "path": path,
                    "lineage_hops": 1,
                    "rationale": f"Matched {violation.column} to {path.name} using fallback evidence mapping.",
                }
            )
    if "confidence" in violation.column.lower():
        for relative in CONFIDENCE_FILES:
            path = apex_root / relative
            if path.exists():
                candidates.append(
                    {
                        "path": path,
                        "lineage_hops": 2,
                        "rationale": f"Matched {violation.column} to {path.name} using confidence-specific fallback evidence.",
                    }
                )
    if violation.dataset == "week3_extractions":
        path = apex_root / "ledger/agents/extraction_api_client.py"
        if path.exists():
            candidates.append(
                {
                    "path": path,
                    "lineage_hops": 2,
                    "rationale": f"Matched {violation.column} to {path.name} using extraction fallback evidence.",
                }
            )
    unique: list[dict[str, Any]] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate["path"])
        if key not in seen:
            unique.append(candidate)
            seen.add(key)
    return unique


def _find_line_number(file_path: Path, column: str) -> int | None:
    tokens = [part for part in column.split(".") if part and part not in {"payload", "facts", "decision"}]
    contents = file_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    for token in reversed(tokens):
        for index, line in enumerate(contents, start=1):
            if token in line:
                return index
    for index, line in enumerate(contents, start=1):
        if column in line:
            return index
    return 1 if contents else None


def _git_log_candidates(file_path: Path, limit: int = 5) -> list[dict[str, Any]]:
    repo_root = _git_root(file_path)
    if repo_root is None:
        return []
    try:
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(repo_root),
                "log",
                f"-n{limit}",
                "--format=%H%x1f%an%x1f%aI%x1f%s",
                "--",
                str(file_path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        return []
    entries: list[dict[str, Any]] = []
    for line in completed.stdout.splitlines():
        if not line.strip():
            continue
        commit_hash, author, commit_timestamp, commit_message = (line.split("\x1f", 3) + ["", "", "", ""])[:4]
        entries.append(
            {
                "commit_hash": commit_hash or None,
                "author": author or None,
                "commit_timestamp": commit_timestamp or None,
                "commit_message": commit_message or None,
            }
        )
    return entries


def _git_root(file_path: Path) -> Path | None:
    current = file_path.resolve().parent
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    return None


def _confidence_from_commit_timestamp(commit_timestamp: str | None, hops: int) -> float:
    if commit_timestamp is None:
        return clamp(0.2 - (0.2 * max(hops - 1, 0)), 0.0, 1.0)
    commit_time = datetime.fromisoformat(commit_timestamp.replace("Z", "+00:00")).astimezone(UTC)
    days_since_commit = max((datetime.now(UTC) - commit_time).days, 0)
    base = 1.0 - (days_since_commit * 0.1)
    return clamp(base - (0.2 * hops), 0.0, 1.0)


def _confidence_from_blame(author_time: int | None, hops: int) -> float:
    if author_time is None:
        return _confidence_from_commit_timestamp(None, hops)
    commit_timestamp = datetime.fromtimestamp(author_time, tz=UTC).isoformat()
    return _confidence_from_commit_timestamp(commit_timestamp, hops)


def _infer_check_id(violation: Violation) -> str:
    if violation.check_id:
        return violation.check_id
    if violation.column:
        return f"{violation.column}.{violation.category}"
    return f"{violation.dataset}.{violation.category}"
