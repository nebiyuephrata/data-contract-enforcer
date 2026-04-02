# Data Contract Enforcer

Data Contract Enforcer is a centralized governance repository for turning inter-system JSONL flows into machine-checked promises.

The first pilot targets `../apexLedger/data/seed_events.jsonl` in two views:

- `week3_extractions`: `ExtractionCompleted` events projected as extraction output
- `week5_events`: the full event stream

## Repository Layout

```text
contracts/             executable logic
generated_contracts/   generated YAML contracts
validation_reports/    validation JSON outputs
violation_log/         appended violation JSONL
schema_snapshots/      baselines and versioned schema snapshots
enforcer_report/       stakeholder JSON and PDF reports
tests/                 unit and integration tests
```

## Install

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

## CLI

```bash
data-contract-enforcer bootstrap-pilot
data-contract-enforcer generate-contracts
data-contract-enforcer snapshot-baselines
data-contract-enforcer validate
data-contract-enforcer analyze-schema
data-contract-enforcer attribute
data-contract-enforcer report
data-contract-enforcer stress-test
```

## Notes

- The enforcer never crashes the run on malformed data. It records `ERROR` diagnostics and continues.
- Lineage is treated as cached/offline input. If no lineage snapshot is available, attribution falls back to file-level evidence and Git blame.
- OpenAI embeddings are optional. If `OPENAI_API_KEY` is not configured, embedding drift is recorded as an `ERROR` and the rest of the run still completes.
