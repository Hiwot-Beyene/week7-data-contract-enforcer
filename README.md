# data-contract-enforcer

Week 7 Challenge — data contracts, migration, **ContractGenerator**, **ValidationRunner**, and **ViolationAttributor**.

## Production and CI

- **Registry**: Keep `contract_registry/subscriptions.yaml` accurate. Validate locally with `python scripts/validate_registry.py` (strict load + optional AUDIT staleness warnings). CI runs the same script when the registry or loader changes (see `.github/workflows/validate-registry.yml`).
- **ValidationRunner**: Use `--mode AUDIT` during burn-in, then `WARN` / `ENFORCE`. Numeric drift needs `schema_snapshots/baselines.json`; without it (and without `--reset-baselines`), drift checks emit `BASELINE_MISSING` and do not create the file. In `ENFORCE`, repeated failures on the same `check_id` within one hour can trip the circuit breaker (effective mode `WARN` for that run; see `validation_reports/mode_transitions.jsonl`).
- **Stakeholder report**: `python contracts/report_generator.py -r <report.json> -o <out.json>`; add `--fail-on-registry-gap` in CI if every failing field must appear in the registry.
- **AI extensions**: Embedding drift uses versioned files under `schema_snapshots/embedding_baselines/` (`latest.npz` plus timestamped archives). Set `OPENAI_API_KEY` for embedding calls (see `.env.example`).
