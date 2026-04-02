from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import subprocess
from typing import Any

from .lineage_loader import evidence_files, load_lineage_snapshot
from .models import AttributionResult, Violation
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
) -> list[AttributionResult]:
    results: list[AttributionResult] = []
    snapshot = load_lineage_snapshot(lineage_snapshot_path)
    apex_root = Path(repositories["apexLedger"])
    for violation in violations:
        if violation.status not in {"FAIL", "ERROR"} or not violation.column:
            continue
        candidate_files = _candidate_files(violation, apex_root, snapshot)
        best_result = None
        for hops, candidate in enumerate(candidate_files, start=1):
            line_number = _find_line_number(candidate, violation.column)
            blame = _git_blame(candidate, line_number)
            confidence = _confidence_from_blame(blame.get("author_time"), hops)
            result = AttributionResult(
                dataset=violation.dataset,
                column=violation.column,
                file_path=str(candidate),
                line_number=line_number,
                commit_hash=blame.get("commit_hash"),
                author=blame.get("author"),
                confidence=confidence,
                lineage_hops=hops,
                rationale=f"Matched {violation.column} to {candidate.name} using fallback evidence mapping.",
            )
            if best_result is None or result.confidence > best_result.confidence:
                best_result = result
        if best_result:
            results.append(best_result)
    return results


def _candidate_files(
    violation: Violation,
    apex_root: Path,
    snapshot: dict[str, Any] | None,
) -> list[Path]:
    keywords = [violation.column or "", violation.dataset]
    candidates: list[Path] = []
    for relative in DEFAULT_APEX_FILES:
        candidates.append(apex_root / relative)
    if violation.column and "confidence" in violation.column.lower():
        candidates.extend(apex_root / relative for relative in CONFIDENCE_FILES)
    if violation.dataset == "week3_extractions":
        candidates.append(apex_root / "ledger/agents/extraction_api_client.py")
    if snapshot:
        for path in evidence_files(snapshot, keywords):
            path_obj = Path(path)
            if path_obj.exists():
                candidates.append(path_obj)
    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key not in seen and candidate.exists():
            unique.append(candidate)
            seen.add(key)
    return unique


def _find_line_number(file_path: Path, column: str) -> int | None:
    tokens = [part for part in column.split(".") if part and part not in {"payload", "facts"}]
    contents = file_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    for token in reversed(tokens):
        for index, line in enumerate(contents, start=1):
            if token in line:
                return index
    for index, line in enumerate(contents, start=1):
        if column in line:
            return index
    return 1 if contents else None


def _git_blame(file_path: Path, line_number: int | None) -> dict[str, Any]:
    repo_root = _git_root(file_path)
    if repo_root is None or line_number is None:
        return {"commit_hash": None, "author": None, "author_time": None}
    try:
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(repo_root),
                "blame",
                "-L",
                f"{line_number},{line_number}",
                "--porcelain",
                str(file_path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        return {"commit_hash": None, "author": None, "author_time": None}
    author = None
    author_time = None
    lines = completed.stdout.splitlines()
    commit_hash = lines[0].split()[0] if lines else None
    for line in lines:
        if line.startswith("author "):
            author = line.removeprefix("author ").strip()
        elif line.startswith("author-time "):
            author_time = int(line.removeprefix("author-time ").strip())
    return {"commit_hash": commit_hash, "author": author, "author_time": author_time}


def _git_root(file_path: Path) -> Path | None:
    current = file_path.resolve().parent
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    return None


def _confidence_from_blame(author_time: int | None, hops: int) -> float:
    if author_time is None:
        return clamp(0.2 - (0.2 * max(hops - 1, 0)), 0.0, 1.0)
    commit_time = datetime.fromtimestamp(author_time, tz=UTC)
    days_since_commit = max((datetime.now(UTC) - commit_time).days, 0)
    base = 1.0 - (days_since_commit * 0.1)
    return clamp(base - (0.2 * hops), 0.0, 1.0)
