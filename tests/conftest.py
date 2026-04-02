from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture()
def apex_seed_events_path() -> Path:
    path = Path(__file__).resolve().parent.parent / "../apexLedger/data/seed_events.jsonl"
    if not path.exists():
        pytest.skip("apexLedger seed data is not available in the expected relative path.")
    return path.resolve()
