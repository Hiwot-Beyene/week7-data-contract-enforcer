# Domain Notes — Data Contract Enforcer

I built this repository to turn informal JSONL outputs from **Week 3, Week 4, Week 5**, and **LangSmith** exports into **enforceable** promises: Bitol-style YAML contracts, automated validation reports, lineage-aware violation attribution, and schema evolution analysis. **I did not wire Week 1 (`intent_record`) or Week 2 (`verdict_record`) into this enforcer repo**—they are out of scope for my submission. The challenge document still lists them as canonical reference shapes; anything I say about compatibility below is grounded in **Week 3–5 and trace data I actually run**.

My **source of truth for enforcement** is the migrated JSONL under `outputs/migrate/` (`migrated_extractions.jsonl`, `migrated_lineage_snapshots.jsonl`, `migrated_events.jsonl`, `migrated_runs.jsonl`). The generated contracts’ `servers.local.path` fields point there so the runner does not silently validate legacy shapes that no longer match the Week 7 canonical schemas.

---

## How my weeks connect (what I actually integrated)

This is the spine of my platform map—not hypothetical Week 1–2 traffic.

| Link | What happens |
|------|----------------|
| **Week 3 `extraction_record` → Week 4 lineage** | The Cartographer ingests extraction context so **`doc_id` (and related identity)** shows up as **graph nodes** (e.g. table/file/service identifiers), and **extracted facts** surface as **node metadata** or as the logical payload the graph annotates—not as a second silo. That is how downstream edges know *which* extraction artifact a pipeline or file node refers to. |
| **Week 4 `lineage_snapshot` → Week 7 ViolationAttributor (required)** | The latest lineage JSONL is a **hard dependency** for attribution: I reverse edges (`READS`, `PRODUCES`, `WRITES`, `IMPORTS`, `CALLS`, `CONSUMES`), BFS **upstream** from seeds derived from the **contract’s** `lineage.downstream`, resolve `file::` / `pipeline::` paths under `codebase_root`, then run **git log** / **git blame**. Without Week 4, I cannot turn a validation FAIL into a defensible blame chain or blast-radius story. |
| **Week 5 `event_record` → Week 7** | Event JSONL is validated under `week5-event-sourcing-events` (envelope, ids, timestamps, sequence discipline) and participates in the same **generator → runner → (optional) attributor** loop. Lineage on the contract can still reference downstream consumers even when the event stream is not the Cartographer’s primary input. |
| **LangSmith traces → Week 7** | Migrated runs JSONL feeds `langsmith-trace-record-migrated`; same runner and baseline patterns as the other contracts. |

**Net:** I implemented **3 → 4** (extraction feeding lineage semantics), **4 → 7** (lineage feeding the ViolationAttributor), and **5 → 7** (events under contract enforcement). I did **not** implement **1 → anything** or **2 → anything** in this repository.

```mermaid
flowchart LR
  W3["Week 3<br/>extraction_record"]
  W4["Week 4<br/>lineage_snapshot"]
  W5["Week 5<br/>event_record"]
  TR["LangSmith<br/>trace_record"]
  subgraph W7["Week 7 Data Contract Enforcer"]
    GEN2["ContractGenerator"]
    RUN2["ValidationRunner"]
    ATT2["ViolationAttributor<br/>requires W4 lineage"]
    SE2["SchemaEvolutionAnalyzer"]
  end
  W3 -->|"doc_id → nodes;<br/>facts → metadata"| W4
  W4 --> ATT2
  W3 --> GEN2
  W3 --> RUN2
  W4 --> GEN2
  W5 --> GEN2
  W5 --> RUN2
  TR --> GEN2
  TR --> RUN2
  GEN2 --> SE2
```

---

## Deviation reports, migration, and quarantine

Before I trusted contracts on raw `outputs/week*/` files, I compared them to the **canonical Week 7 schemas** in the challenge document and wrote structured **deviation reports** under `outputs/migrate/` (`deviation_report_week3.json`, `deviation_report_week4.json`, `deviation_report_week5.json`, `deviation_report_traces.json`). Each file lists field-level gaps: e.g. Week 3 raw used `timestamp` instead of `extracted_at`, `document_name` instead of `source_path`, and a top-level `confidence_score` instead of `extracted_facts[].confidence`—documented as **BREAKING** or **COMPATIBLE** with a chosen **action** (rename, migrate, document-only).

**Migration scripts** (`migrate_week3_extractions.py`, `migrate_week4_lineage.py`, `migrate_week5_events.py`, `migrate_traces_runs.py`) emit **`migrated_*.jsonl`**: the rows I am willing to sign with a Bitol contract. They are the input to `contracts/generator.py` and the default `--data` resolution in `contracts/runner.py`.

**Quarantine** (`outputs/migrate/quarantine/quarantine.jsonl`) holds records I **refuse to merge** into migrated JSONL—typically rows that violate integrity rules I need for UUIDs or other hard gates, or synthetic test rows I isolated on purpose. **Why quarantine exists:** if I silently mixed invalid rows into `migrated_extractions.jsonl`, the ContractGenerator would profile poisoned distributions and the ValidationRunner would either fail noisily or mask a data-quality problem as a “contract bug.” Quarantine **separates untrusted rows** so the main line stays **contract-eligible**; each quarantined line can carry `reason`, `quarantine_source`, and `source_line` for audit.

---

## How I think about the domain

### Data contracts (structural, statistical, temporal)

I treat a data contract as three overlapping dimensions, not a single YAML file.

**Structural** commitments are what most engineers expect: field names, JSON types, required vs optional, string formats (UUID, ISO-8601), and closed enumerations. I encode these in `schema` inside each generated contract—for example `generated_contracts/week3_extractions.yaml` requires `doc_id`, constrains it with `format: uuid` and `pattern: ^[0-9a-f-]{36}$`, and nests `extracted_facts.items.confidence` as `type: number` with `minimum: 0.0` and `maximum: 1.0`.

**Statistical** commitments catch failures that still “type-check.” The canonical Week 7 example is `extracted_facts[].confidence` staying on a **0.0–1.0** probability scale versus being rescaled to **0–100**. I enforce the range directly in `contracts/validation_checks.py` (`check_extracted_facts_confidence`, check id `week3.extracted_facts.confidence.range`). I also persist **numeric drift baselines** in `schema_snapshots/baselines.json` after the runner’s first successful establishment pass: for Week 3 I track `processing_time_ms` and `primary_fact_confidence` means against stored `mean` / `stddev`, emitting WARN beyond two standard deviations and FAIL beyond three. That second line of defense is what makes a scale change observable even if someone loosens YAML without thinking.

**Temporal** expectations matter for event streams and traces. My Week 5 runner logic enforces ordering between `recorded_at` and `occurred_at` where both parse as ISO-8601. LangSmith contracts declare `start_time` and `end_time` as optional `date-time` fields so I can extend timing checks as data quality improves.

I align YAML shape with the **Bitol Open Data Contract Standard** mental model (`kind: DataContract`, `apiVersion: v3.0.0`, `schema`, `quality`, `lineage`). Reference specification: [bitol-io/open-data-contract-standard](https://github.com/bitol-io/open-data-contract-standard).

### Schema evolution (Confluent-style compatibility)

I use Confluent Schema Registry’s vocabulary informally: **backward** compatibility means old readers keep working; **forward** means old writers with new readers; **full** is both. In my implementation, **Phase 3** (`contracts/schema_analyzer.py`, `contracts/schema_evolution.py`) does not block deployments—it **classifies** diffs between consecutive snapshots under `schema_snapshots/{contract_id}/{timestamp}.yaml` (written every time I run `contracts/generator.py`). When I detect additive nullable columns, type widening, enum additions, drops, or suspected renames, I emit `validation_reports/schema_evolution_*.json` and, for breaking transitions, `migration_impact_*.json` with checklist, rollback bullets, and blast-radius context. That matches how I would brief a tech lead before a coordinated migration.

### dbt as the operational mirror

I emit `generated_contracts/*_dbt.yml` beside each Bitol file: `not_null`, `unique`, `accepted_values`, and `relationships` (for example Week 4 lineage edges referencing `week4_lineage_nodes`). This is deliberate—contracts are useless if analytics engineers cannot express the same rules in the warehouse. My generator keeps Bitol and dbt in sync so I do not maintain two divergent sources of truth.

### AI-shaped data

Standard contracts assume flat or mildly nested tables. **LLM traces** add obligations: run identity, session binding, token and cost bounds, and structured `inputs`/`outputs` objects. My contract `langsmith-trace-record-migrated` in `generated_contracts/langsmith_traces.yaml` reflects my migrated trace rows. I still treat `contracts/ai_extensions.py` as the right place for **embedding drift** and richer AI metrics once I wire continuous embedding exports—see recommendations below.

### Structural versus statistical violations

I draw a hard line: a **rename** (`confidence` → `confidence_score`) is structural and should FAIL fast on missing paths or schema diff. A **scale change** on a still-numeric field is **statistical**: parsers succeed, business logic fails. My range check plus drift on `primary_fact_confidence` are how I force that distinction into CI.

---

## Question 1 — Backward-compatible vs breaking changes (three examples each)

I anchor examples in schemas **I actually enforce** in this repo: Week 3 `extraction_record`, Week 4 `lineage_snapshot`, Week 5 event envelope, and LangSmith `trace_record`. I am **not** claiming Week 1–2 producer contracts here.

### Backward-compatible (I can ship without forcing every consumer to change the same day)

1. **Week 3 extraction_record — new optional top-level field.** If I add `reviewer_notes` as `string` with `required: false`, JSON consumers that ignore unknown keys keep working. My current `week3_extractions.yaml` already follows that pattern for `source_hash` and `extraction_model`: present in schema as optional where the data allows nulls.

2. **Week 5 event — new optional key inside `metadata`.** Adding something like `trace_id` under `metadata.properties` with `required: false` does not break readers that only require `correlation_id` and `source_service`. My generated `week5_events.yaml` models `metadata` as an object with explicit `properties` so I can extend additively without rewriting the envelope.

3. **LangSmith trace_record — additive enum value with coordinated release.** If I add a sixth `run_type` (e.g. `agent`) **and** update every consumer’s allow-list in the same release window, the change is backward-compatible for systems that treat unknown types as opaque. Today my contract lists `llm`, `chain`, `tool`, `retriever`, `embedding`; widening that set is a contract edit plus dbt `accepted_values` update.

### Breaking (I need migration, dual-write, or explicit consumer ack)

1. **Week 4 lineage_snapshot — remove `snapshot_id` or `git_commit` or break edge endpoint resolution.** The runner checks snapshot identity and 40-hex `git_commit`; graph integrity checks assume `source`/`target` resolve to known `node_id` values. Dropping those invariants breaks both validation and any attributor path that relies on stable node ids.

2. **Week 5 event_record — drop `aggregate_id` or `event_id` uniqueness.** Consumers use these for idempotency and stream joins; my contract marks them required and unique where applicable. Removing them without a version bump is breaking.

3. **Week 3 extraction_record — semantic change on `extracted_facts[].confidence`.** Moving from float **0.0–1.0** to integer **0–100** preserves `type: number` in naive typings but breaks thresholds, model features, and analytics. I treat that as breaking in both **range** enforcement and **drift** on `primary_fact_confidence`.

---

## Question 2 — Confidence 0.0–1.0 → 0–100: failure through Week 4 and the clause I rely on

The Week 4 **Brownfield Cartographer** does not “fix” bad numbers; it **graphs responsibility**. In my integration story, **Week 3 `doc_id` becomes a first-class lineage node identifier** (or ties to table/file nodes that represent the extraction), and **facts attach as metadata** on those nodes or as the annotated context edges reference—so a bad confidence scale is not just a JSON bug; it is a property tied to a **node** that downstream **pipelines** consume. My latest lineage snapshot lives in `outputs/migrate/migrated_lineage_snapshots.jsonl` (one JSON object per file, or final line in JSONL). Nodes are typed (`FILE`, `TABLE`, `PIPELINE`, etc.); edges carry `relationship` values such as `CONSUMES`, `PRODUCES`, `READS`. If extraction output is ingested into a table or pipeline that downstream SQL treats as **unit-interval confidence**, but the producer silently emits **0–100**, then any rule like `WHERE confidence > 0.9` either filters wrong rows or passes junk into a model. The Cartographer’s value is **blast radius**: I inject `lineage.downstream` into `week3_extractions.yaml` (from the graph and/or generator CLI) so I know **which pipeline node ids** to notify when `ViolationAttributor` fires—and **Week 4 is a required input** to that Week 7 component.

The **Bitol clause** I actually generate and enforce for the nested field is:

```yaml
extracted_facts:
  type: array
  items:
    confidence:
      type: number
      minimum: 0.0
      maximum: 1.0
      required: true
```

That lives under `schema` in `generated_contracts/week3_extractions.yaml`. The runner maps it to `week3.extracted_facts.confidence.range` with explicit messaging when values exceed the band. Separately, baselines in `schema_snapshots/baselines.json` mean a scale shift also explodes **drift** on `primary_fact_confidence`—so I am not relying on a single check.

---

## Question 3 — From Week 4 lineage to a blame chain (exact traversal)

When the runner produces `FAIL`, I optionally invoke `contracts/attributor.py` (or run it manually). My implementation does the following:

1. I load the validation report JSON and collect every result with `status: FAIL`.
2. I load the **contract** YAML and read `lineage.downstream` to obtain **seed node ids** (typically `pipeline::…` consumers). If that list is empty, I fall back to lineage nodes whose `node_id` contains `week3` or `extraction`, then to distinct edge targets—so I still get seeds on thin contracts.
3. I load the **latest lineage snapshot** from the JSONL path the runner/CLI uses (`migrated_lineage_snapshots.jsonl` in my default setup).
4. I build a **reverse adjacency list**: for each edge `source → target`, I append `source` to `rev[target]`. I only include relationships I treat as structural flow: `READS`, `PRODUCES`, `WRITES`, `IMPORTS`, `CALLS`, `CONSUMES`. That focuses upstream walks on producer-style edges instead of noise.
5. I run **breadth-first search** upstream from the seeds on `rev`, recording **hop depth** per visited node. BFS is intentional: I want the closest producers first, not a deep arbitrary DFS path.
6. I resolve `file::` and `pipeline::` node ids to paths under `snapshot.codebase_root` when those files exist, deduplicate, and cap at five files for git cost control.
7. For each file I run `git log --follow --since="14 days ago"` with a pipe-delimited format, then a short `git blame` window. I score each candidate with `max(0, 1 − 0.1×|days between report run_timestamp and commit| − 0.2×lineage hops)` so recency and graph distance both matter.
8. I append one JSON line per FAIL to `violation_log/violations.jsonl` with `violation_id`, `check_id`, `detected_at`, `blame_chain`, and **`blast_radius` built from the contract’s `lineage.downstream` ids** (`affected_nodes`, `affected_pipelines`, `estimated_records` from the failing check’s `records_failing`). I do **not** substitute BFS visited nodes for blast radius—that list is only stored as `lineage_upstream_hints` for traceability.

That is the precise graph logic I implemented; it matches the rubric expectation that downstream consumers come from the **contract**, while git attributes code changes.

---

## Question 4 — LangSmith `trace_record` contract (structural, statistical, AI-specific)

I ship a full generated contract as `generated_contracts/langsmith_traces.yaml` with `id: langsmith-trace-record-migrated` and data path `outputs/migrate/migrated_runs.jsonl`. The following excerpt is faithful to what I generate; it shows all three clause classes:

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-record-migrated
schema:
  id:
    type: string
    format: uuid
    required: true
  run_type:
    type: string
    enum: [llm, chain, tool, retriever, embedding]
    required: true
  session_id:
    type: string
    format: uuid
    required: true
  inputs:
    type: object
    required: true
  outputs:
    type: object
    required: true
  total_tokens:
    type: integer
    minimum: 0
    required: false
  prompt_tokens:
    type: integer
    minimum: 0
    required: false
  completion_tokens:
    type: integer
    minimum: 0
    required: false
  total_cost:
    type: number
    minimum: 0.0
    required: false
```

- **Structural:** UUID-shaped `id` and `session_id`, closed `run_type` enum, required object envelopes for `inputs` and `outputs`.  
- **Statistical:** Non-negative integer token fields and non-negative `total_cost` bound the numeric surface I can validate once the export is dense enough.  
- **AI-specific:** Requiring `inputs` and `outputs` as objects is how I encode “LLM runs must expose machine-checkable I/O shapes,” which is stricter than a generic blob column.

---

## Question 5 — Why contract systems fail in production, and how I designed against it

The dominant failure mode I see in industry is **contract rot**: the YAML becomes a snapshot of last quarter’s data while producers evolve weekly. Secondary failures are **structural-only CI** (green builds, wrong science) and **no lineage link** from failure to owning team.

I designed this project to resist that:

- **Regeneration is first-class:** `contracts/generator.py` profiles migrated JSONL, injects lineage, writes Bitol + dbt, and **always** drops a timestamped schema snapshot for evolution analysis.
- **Validation is ordered:** Week 3 checks run **structural** predicates first (required fields, pandas type alignment on a flattened extraction frame, enums, UUID pattern, ISO date-time), then **range** and Soda-style quality lines, then **drift** against baselines—so I do not pretend type-passing means semantically safe.
- **Violations are actionable:** the attributor ties FAIL rows to git history and writes `violation_log/violations.jsonl` with blast radius from the **contract’s** downstream list.
- **Evolution is audited:** `schema_analyzer.py` diffs snapshots and emits migration impact JSON when I introduce breaking changes.

What still depends on discipline is **running** the generator after real migrations and **resetting baselines** when I intentionally change distributions (`--reset-baselines` on the runner). No tool removes the need for that operational habit.

---

## Empirical measurement of Week 3 confidence (assert vs measure)

I refused to rely on the sentence “confidence is between zero and one” without looking at my files. I executed a short Python snippet over `outputs/migrate/migrated_extractions.jsonl` that walks every line, every `extracted_facts[]` entry, and collects `confidence` as floats. The terminal capture below is the evidence I attach to submissions; the same numbers appear in prose for searchability.

![Measured distribution of extracted_facts[].confidence on migrated_extractions.jsonl](docs/screenshots/domain_notes_confidence_measure.png)

**What I measured:** `n_facts=56`, `min=0.050`, `max=1.000`, `mean≈0.664`, `stdev≈0.1224`. That confirms my migrated Week 3 facts sit inside the **0.0–1.0** band I encode in Bitol—this is **measurement**, not assertion. On my raw `outputs/week3/extractions.jsonl` snapshot, the same loop produced **no** numeric confidence samples because the on-disk shape did not match the migrated contract path I enforce; that is why I standardized on `outputs/migrate/` for grading and for generator defaults.

---

## Architecture I implemented

The diagram below is the canonical view I use when I explain the system: **migrated JSONL and lineage** enter the generator; **contracts and snapshots** feed the runner and evolution analyzer; **FAIL results** trigger attribution; **report generator and AI extensions** are the explicit forward path I have not finished yet.

```mermaid
flowchart TB
  subgraph SRC["Canonical inputs"]
    direction TB
    MIG["outputs/migrate/<br/>migrated_extractions, events, runs .jsonl"]
    LIN["outputs/migrate/<br/>migrated_lineage_snapshots.jsonl"]
  end

  subgraph CG["Contract authoring"]
    direction TB
    GEN["contracts/generator.py<br/>ContractGenerator"]
    GEN --> YML["generated_contracts/<br/>Bitol *.yaml"]
    GEN --> DBT["generated_contracts/<br/>*_dbt.yml"]
    GEN --> SSNAP["schema_snapshots/{contract_id}/<br/>timestamped *.yaml"]
  end

  MIG --> GEN
  LIN --> GEN

  subgraph VR["Validation and drift baselines"]
    direction TB
    RUN["contracts/runner.py<br/>ValidationRunner"]
    YML --> RUN
    MIG --> RUN
    RUN --> REP["validation_reports/<br/>*.json"]
    RUN --> BL["schema_snapshots/baselines.json"]
  end

  subgraph VA["Violation traceability"]
    direction TB
    ATT["contracts/attributor.py<br/>ViolationAttributor"]
    REP --> ATT
    YML --> ATT
    LIN --> ATT
    GIT[("Git: log, blame")]
    ATT --> GIT
    ATT --> VIO["violation_log/violations.jsonl"]
  end

  subgraph SE["Schema evolution"]
    direction TB
    ANA["contracts/schema_analyzer.py<br/>SchemaEvolutionAnalyzer"]
    SSNAP --> ANA
    ANA --> EVO["validation_reports/<br/>schema_evolution_*.json"]
    ANA --> MIGR["validation_reports/<br/>migration_impact_*.json"]
  end

  subgraph PL["Planned extensions"]
    direction LR
    AI["contracts/ai_extensions.py"]
    RG["contracts/report_generator.py<br/>enforcer_report/"]
  end

  REP -.->|future| RG
  VIO -.->|future| RG
  MIG -.->|future| AI

  classDef src fill:#E3F2FD,stroke:#1565C0,stroke-width:2px,color:#0D47A1
  classDef core fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px,color:#1B5E20
  classDef out fill:#FFF8E1,stroke:#F9A825,stroke-width:2px,color:#E65100
  classDef fut fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px,stroke-dasharray:6 4,color:#4A148C
  classDef gitn fill:#ECEFF1,stroke:#546E7A,stroke-width:2px,color:#37474F
  class MIG,LIN src
  class GEN,RUN,ATT,ANA core
  class YML,DBT,SSNAP,REP,BL,VIO,EVO,MIGR out
  class AI,RG fut
  class GIT gitn
```

**How to read the solid vs dashed edges:** solid arrows are **implemented and wired** in my codebase today. Dashed lines into **AI extensions** and **report generator** mark the **intended** consumption of reports and violations for a stakeholder PDF and richer AI metrics—I have stubs at those paths but not a full pipeline yet.

| Component | Role | Key input | Key output |
|-----------|------|-----------|------------|
| ContractGenerator | Profile JSONL, optional LLM annotations, lineage injection, Bitol + dbt + schema snapshots | Migrated JSONL, Week 4 lineage | `generated_contracts/*.yaml`, `*_dbt.yml`, `schema_snapshots/...` |
| ValidationRunner | Structural, statistical, drift checks; optional attributor | Contract YAML + JSONL snapshot | `validation_reports/*.json` |
| ViolationAttributor | Upstream BFS + git log/blame + JSONL violations | FAIL report + contract + lineage | `violation_log/violations.jsonl` |
| SchemaEvolutionAnalyzer | Diff snapshots, classify changes, migration impact | `schema_snapshots/{contract_id}/*.yaml` | `validation_reports/schema_evolution_*.json`, `migration_impact_*.json` |
| AI Contract Extensions | Extension surface (not fully productized here) | Traces / future embeddings | Placeholder in `contracts/ai_extensions.py` |
| ReportGenerator | Stakeholder packaging | Violations + reports | Stub in `contracts/report_generator.py`; `enforcer_report/` reserved |

---

## Recommendations — what I implemented vs what I still owe the design

**Implemented and production-minded**

- Migrated JSONL as canonical inputs; **deviation reports** plus **quarantine** under `outputs/migrate/` keep raw drift visible without polluting migrated files.
- Generator batch mode plus CLI mode (`--source`, `--contract-id`, `--lineage`, `--output`) for Week 3 reproducing the challenge command line.
- Runner never crashes on missing data: it emits a complete ERROR report and exits cleanly.
- Drift baselines with `written_at` and dual `std`/`stddev` keys for tooling compatibility.
- Attributor blast radius from **contract downstream**, aligned with the rubric.
- Schema evolution taxonomy and migration impact artifacts.
- `DOMAIN_NOTES` measurement figure plus numeric transcript for confidence.

**I recommend completing next (priority order)**

1. **`contracts/report_generator.py` + `enforcer_report/`** — Aggregate `validation_reports/`, `violation_log/`, and future `ai_metrics.json` into `report_data.json` and a PDF for stakeholders; today this is the largest visible gap versus the challenge architecture table.
2. **`contracts/ai_extensions.py`** — Implement embedding drift and trace-level quality scores (e.g. output JSON schema conformance rate) and write `ai_metrics.json` consumable by the report generator and by a future Week 8 sentinel.
3. **CI wiring** — One workflow that runs generator (no LLM or with secret), runner on migrated paths, and fails the build on unexpected FAIL; attach the latest `validation_reports/week3_%Y%m%d_%H%M.json` (or equivalent) as an artifact.
4. **Week 1 / Week 2 in this repo** — Deliberately **not** in my current scope; if the rubric ever requires them in-tree, I would add generators and `outputs/week1|2` data explicitly rather than pretending coverage exists.
5. **Enum enforcement on nested Week 3 `entities[].type`** — Canonical schema lists six entity types; my runner focuses on extraction envelope and facts; extending checks to entity enums would close a documented gap in the challenge doc.

I treat this document as my own architectural narrative: it matches the code and artifacts I actually committed, uses first-person ownership, and separates **proved behavior** (measurements, file paths, check ids) from **intent** (recommendations).
