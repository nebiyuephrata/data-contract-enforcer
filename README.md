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

## Embedding Providers

OpenAI remains supported, but the default embedding path is now OpenRouter with Gemini embeddings.

```bash
export OPENROUTER_API_KEY=...
export OPENROUTER_HTTP_REFERER=https://your-app.example
export OPENROUTER_APP_TITLE=data-contract-enforcer
```

The default embedding model in [contracts/config.yaml](/home/rata/Documents/Ephrata/work/10Acadamy/training/data-contract-enforcer/contracts/config.yaml) is `google/gemini-embedding-001` through the OpenRouter OpenAI-compatible embeddings API.

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
- Registry blast radius is `registry-first`: [contract_registry/subscriptions.yaml](/home/rata/Documents/Ephrata/work/10Acadamy/training/data-contract-enforcer/contract_registry/subscriptions.yaml) is the primary subscriber source, and lineage only enriches transitive contamination inside visible systems.
- Lineage is treated as cached/offline input. If no lineage snapshot is available, attribution falls back to file-level evidence and Git blame.
- Embedding drift is optional. If the configured provider key is missing, the run records an `ERROR` and continues.
