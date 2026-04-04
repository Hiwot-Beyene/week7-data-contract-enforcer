# Data Contract Enforcer — Implementation Report

> **Not the Enforcer Report (PDF).** The Week 7 **stakeholder Enforcer Report** is generated as **`enforcer_report/report_data.json`** and **`enforcer_report/report_latest.pdf`** (timestamped `report_*.pdf` archives) via **`contracts/report_generator.py`** (`write_enforcer_pdf`, ReportLab) or the **enforcer-ui** “Generate New Report” action when `reportlab` is installed. **This Markdown file** is only a separate implementation write-up for humans; it is not a substitute for the PDF deliverable.

**Author:** Implementation summary aligned with `contracts/runner.py`, generated Bitol contracts, and archived validation reports under `validation_reports/`.  
**Evidence:** `week3_20260402_0145.json`, `week4_20260402_0145.json`, `week5_20260402_0145.json`, `traces_20260402_0145.json`.

---

## 1. Contract coverage table

An *inter-system interface* here means a **stable JSONL (or snapshot) handoff** between a producer system and downstream consumers or tooling, as described in the Week 7 brief.

| Interface (producer → consumer) | Data artifact | Contract written? | Notes / if “No”, why |
|--------------------------------|---------------|-------------------|----------------------|
| Week 3 document refinery → analytics / Week 7 enforcement | `outputs/week3/extractions.jsonl` (raw); `outputs/migrate/migrated_extractions.jsonl` (migrated) | **Yes** | `generated_contracts/week3_extractions.yaml` and `week3-document-refinery-extractions.yaml` both target `week3-document-refinery-extractions`. Raw export predates the canonical schema (see validation results). |
| Week 4 brownfield cartographer → lineage consumers / blast radius | `outputs/week4/lineage_snapshots.jsonl` (also compact `migrated_lineage_snapshots.jsonl`) | **Yes** | `generated_contracts/week4_lineage.yaml` (`week4-brownfield-lineage-snapshot`). |
| Week 5 event-sourced stream → projections / audit | `outputs/week5/events.jsonl` (raw); `outputs/migrate/migrated_events.jsonl` | **Partial** | `generated_contracts/week5_events.yaml` models **canonical** event-sourcing fields (`event_id`, `aggregate_id`, …). Raw `events.jsonl` uses **`stream_id` + `payload` + `event_version`** — same *business* stream, different *serialization contract*. Enforcement passes only where field names overlap; full alignment requires running against **migrated** JSONL or evolving the contract to the raw shape. |
| LangSmith (or compatible) trace export → observability / AI governance | `outputs/traces/runs.jsonl` | **Yes** | `generated_contracts/langsmith_traces.yaml` (`langsmith-trace-record-migrated`). Coverage is **thin** when the file has very few rows (see drift and enum checks). |
| Week 1 intent records → downstream | `outputs/week1/…` (not present in this repo) | **No** | Explicitly **out of scope** for this repository; no sample JSONL to profile or enforce. |
| Week 2 LLM verdict / structured output | `outputs/week2/…` (not present) | **No** | Same as Week 1 — not included in submitted `outputs/`. |
| LLM prompt inputs / embedding payloads (AI extensions) | N/A in committed outputs | **No** | `contracts/ai_extensions.py` is a **stub**; no end-to-end contract + runner path committed for prompt schema or embedding drift on this dataset. |

**Summary:** Every **in-repo** primary export that the challenge centers on (Week 3–5 + traces) has at least a **generated Bitol contract + dbt sidecar**, except where the **on-disk shape** is intentionally legacy (**Partial** for Week 5 raw vs canonical contract). Week 1/2 and full AI-extension surfaces are **No** due to scope and missing artifacts, not lack of intent.

---

## 2. First validation run results (ValidationRunner on own data)

Runs reference the archived reports (SHA-256 `snapshot_id` inside each JSON). Summary **across all four datasets**:

| Dataset | Contract ID | Total checks | Passed | Failed | Warned | Errored |
|---------|-------------|-------------|--------|--------|--------|---------|
| Week 3 extractions (raw) | `week3-document-refinery-extractions` | 17 | 9 | 6 | 0 | 2 |
| Week 4 lineage snapshot | `week4-brownfield-lineage-snapshot` | 3 | 3 | 0 | 0 | 0 |
| Week 5 events (raw) | `week5-event-sourcing-events` | 16 | 8 | 8 | 0 | 0 |
| LangSmith traces | `langsmith-trace-record-migrated` | 2 | 0 | 2 | 0 | 0 |
| **Combined** | — | **38** | **20** | **16** | **0** | **2** |

### Violations and interpretation

**Week 3 (real structural debt, not injected):** The contract encodes the **post-migration** document refinery shape (`source_path`, `extracted_at`, UUID `doc_id`, …). The **raw** `outputs/week3/extractions.jsonl` still carries **legacy** identifiers (short hex `doc_id`, `timestamp` vs `extracted_at`, missing `source_path`). Failures include **required-field** gaps, **UUID format**, **duplicates**, and **`processing_time_ms` minimum** on a small subset. Two checks returned **ERROR** (`primary_fact_confidence` min/max) because the Soda-style quality lines expect a derived column that does not exist on this flattening — a **contract-vs-data expressiveness** gap, not silent green.

**Week 4:** **No violations** — snapshot id, git commit format, and edge referential integrity all **PASS**. This is the clearest signal that the lineage export matches the validator’s expectations.

**Week 5 (schema mismatch):** Failures cluster on **required keys** (`event_id`, `aggregate_id`, `aggregate_type`, `sequence_number`, etc.) **absent** from raw rows that use **`stream_id` / `payload`**. These are **real** mismatches between **canonical contract** and **current export shape**; migrated JSONL is the fair target for a “green” run.

**Traces (real + statistical artifact):** With **very low row count**, **`run_type`** failed the allowed enum for the sample row, and **`inputs_key_count` drift** **FAIL**ed with an extreme z-score because the **baseline standard deviation was floored** near zero — a known pitfall of **single-record or near-constant baselines**. Not an injected “demo” violation; it is an honest **statistical baseline hygiene** finding.

**ViolationAttributor:** Any **FAIL** drives append-only rows in `violation_log/violations.jsonl` (lineage + git context). That log reflects **automated attribution** on top of the JSON reports above.

---

## 3. Reflection (≤ 400 words)

Writing contracts first forced me to treat “my JSONL file” as a **published API** instead of a private log. The biggest surprise was how **quietly** two different truths can coexist: **Week 3** data still looked “fine” in ad hoc scripts — numeric confidences were in range, row counts were healthy — yet **half the structural clauses** failed because the **column names and keys** never matched the **canonical** refinery contract. I had assumed migration was a **value** problem; it was equally an **identity and field-name** problem (`doc_id` shape, `timestamp` vs `extracted_at`, missing `source_path`).

**Week 5** sharpened that lesson. The stream is clearly **event-sourced** in product terms, but the **on-wire JSON** I committed uses **`stream_id` and nested `payload`**, while the contract describes a **flattened event envelope** (`event_id`, `aggregate_id`, …). Without the runner, I would have said “we have events”; with it, I have to say “we have **two dialects** — raw export dialect versus contract dialect.” That is exactly the kind of ambiguity contracts are meant to remove.

**Week 4** was the counterexample: **three checks, three passes**. That told me the **cartography export** is already **internally consistent** (nodes, edges, commit metadata) in a way the older week exports were not.

**LangSmith** exposed a different blind spot: **baselines and sample size**. I established drift anchors from early runs; with **one or two rows**, statistical drift becomes **numerically unstable**. I assumed drift would behave like production traffic; it actually behaves like **variance math on a spreadsheet with one cell**.

Overall, the wrong assumption was “**if types look right, we’re compatible**.” The right question is “**does every required field exist on every row, under the names downstream code uses?**” The ValidationRunner answered that with **PASS/FAIL/ERROR**, not opinions — and showed where **partial contracts** (Week 5 raw vs migrated) need an explicit **version or migration story**, not silent hope.

---

*Word count (reflection section only): within the 400-word limit.*
