from __future__ import annotations

from contracts.models import DatasetConfig
from contracts.runner import validate_dataset


def test_drift_thresholds_warn_and_fail():
    dataset = DatasetConfig(name="demo", source="demo.jsonl")
    contract = {
        "fields": [
            {"name": "metric", "type": "number", "required": True, "nullable": False, "null_fraction": 0.0},
        ]
    }
    warn_summary, warn_violations = validate_dataset(
        dataset,
        contract,
        [{"metric": 14.5}],
        {"demo": {"metric": {"mean": 10.0, "stddev": 2.0, "sample_size": 30}}},
        {"min_observations_for_drift": 20},
    )
    assert any(item.status == "WARN" for item in warn_violations)

    fail_summary, fail_violations = validate_dataset(
        dataset,
        contract,
        [{"metric": 17.0}],
        {"demo": {"metric": {"mean": 10.0, "stddev": 2.0, "sample_size": 30}}},
        {"min_observations_for_drift": 20},
    )
    assert any(item.status == "FAIL" and item.category == "drift" for item in fail_violations)
