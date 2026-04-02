from __future__ import annotations

import copy
import json
from pathlib import Path

from contracts.ai_extensions import run_llm_schema_checks
from contracts.config import load_config
from contracts.generator import build_baselines, build_contract, build_dbt_schema, load_dataset_records
from contracts.report_generator import build_report_payload, compute_data_health_score
from contracts.runner import validate_dataset


def test_generate_contracts_for_apex_pilot(apex_seed_events_path: Path):
    config = load_config()
    week3 = next(dataset for dataset in config["datasets"] if dataset.name == "week3_extractions")
    week5 = next(dataset for dataset in config["datasets"] if dataset.name == "week5_events")
    raw3, records3 = load_dataset_records(week3)
    raw5, records5 = load_dataset_records(week5)
    assert len(records3) == 50
    assert len(records5) >= 1847

    contract3 = build_contract(week3, records3)
    contract5 = build_contract(week5, records5)
    assert any(field["name"] == "payload.facts.field_confidence.total_revenue" for field in contract3["fields"])
    assert any(field["name"] == "event_type" for field in contract5["fields"])
    assert build_dbt_schema(contract3)["models"][0]["name"] == "week3_extractions"


def test_clean_data_validates_with_low_penalty(apex_seed_events_path: Path, tmp_path: Path):
    config = load_config()
    dataset = next(item for item in config["datasets"] if item.name == "week3_extractions")
    raw_records, records = load_dataset_records(dataset)
    contract = build_contract(dataset, records)
    baselines = {dataset.name: build_baselines(dataset, records)}
    summary, violations = validate_dataset(dataset, contract, records, baselines, config["validation"])
    assert summary["status"] in {"PASS", "WARN"}
    assert compute_data_health_score(violations) >= 80


def test_malformed_data_emits_error_or_fail_without_crashing(apex_seed_events_path: Path):
    config = load_config()
    dataset = next(item for item in config["datasets"] if item.name == "week3_extractions")
    raw_records, records = load_dataset_records(dataset)
    contract = build_contract(dataset, records)
    broken = copy.deepcopy(records)
    broken[0]["payload.facts.field_confidence.total_revenue"] = "not-a-number"
    broken[0]["stream_id"] = None
    baselines = {dataset.name: build_baselines(dataset, records)}
    summary, violations = validate_dataset(dataset, contract, broken, baselines, config["validation"])
    assert violations
    assert any(item.status in {"FAIL", "ERROR"} for item in violations)


def test_llm_schema_checks_run_on_pilot_events(apex_seed_events_path: Path, tmp_path: Path):
    config = load_config()
    dataset = next(item for item in config["datasets"] if item.name == "week5_events")
    raw_records, _ = load_dataset_records(dataset)
    checks, violations = run_llm_schema_checks(dataset.name, raw_records, tmp_path / "rates.json")
    assert any(check["check"].startswith("llm_schema_") for check in checks)
    assert isinstance(violations, list)


def test_stress_violation_drops_score_and_mentions_business_impact(apex_seed_events_path: Path):
    config = load_config()
    dataset = next(item for item in config["datasets"] if item.name == "week3_extractions")
    raw_records, records = load_dataset_records(dataset)
    contract = build_contract(dataset, records)
    baselines = {dataset.name: build_baselines(dataset, records)}
    stressed = copy.deepcopy(records)
    stressed[0]["payload.facts.field_confidence.total_revenue"] = 92.0
    summary, violations = validate_dataset(dataset, contract, stressed, baselines, config["validation"])
    report = build_report_payload({"dataset_summaries": [summary], "ai_checks": []}, violations, [], [])
    assert any(item.column == "payload.facts.field_confidence.total_revenue" for item in violations)
    assert report["data_health_score"] < 100
    assert any("hallucination risk" in text.lower() for text in report["business_risks"])
