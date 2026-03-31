TRP Week 7: The Data Contract Enforcer

## *Schema Integrity & Lineage Attribution System for Your Own Platform*

***Your five systems have been talking to each other without contracts. This week you write them — and enforce them.***

**Builds on:**  

Week 1 (Intent-Code Correlator)   

Week 2 (Digital Courtroom)   

Week 3 (Document Refinery)   

Week 4 (Brownfield Cartographer)   

Week 5 (Event Sourcing Platform)

# **Why This Project**

You have built five systems over six weeks. Each system produces structured data. Each system consumes structured data from prior weeks or other dependencies. At no point have you written down what you promised. 

 For example, the Week 3 Document Refinery outputs extracted\_facts as a list of objects with a confidence field in the range 0.0–1.0. Which consumers enforced it? When a refinery update changed confidence to a percentage (0–100), the consumer logic broke silently — it still ran, it still produced output, and the output was wrong.

The Data Contract Enforcer turns every arrow in your inter-system data flow diagram into a formal, machine-checked promise. When a promise is broken — by a schema change, a type drift, a statistical shift — the Enforcer catches it, traces it to the commit that caused it, and produces a blast radius report showing every downstream system affected. This week that blast radius is your own platform.

## **The FDE Connection**

The first question a data engineering client asks in week one is: "Can you make sure this never breaks silently again?" The Data Contract Enforcer answers that question with a deployable system and a demonstration. The second question — "How would I know if it did break?" — is answered with the violation report and the blame chain. An FDE who walks in and deploys this in 48 hours is not selling consulting. They are selling certainty.

# **The Inter-System Data Map**

This section is the architectural foundation of the week. Before implementing anything, you must produce a data-flow diagram of your five systems and annotate every arrow with the exact schema it carries. The schemas below are the canonical target — your implementations may differ and you must document its rationale in your DOMAIN\_NOTES.md.

Every schema below is defined as a JSON object. Each system must serialise its primary output to **outputs/{week\_name}/** in JSONL format (one JSON object per line). The contract enforcement in Phase 1 will read from these directories.

## **Week 1 — Intent-Code Correlator  (intent\_record)**

// File: outputs/week1/intent\_records.jsonl  
{  
  "intent\_id":    "uuid-v4",  
  "description":  "string — plain-English statement of intent",  
  "code\_refs": \[  
    {  
      "file":       "relative/path/from/repo/root.py",  
      "line\_start": 42,           // int, 1-indexed  
      "line\_end":   67,           // int \>= line\_start  
      "symbol":     "function\_or\_class\_name",  
      "confidence": 0.87         // float MUST be 0.0–1.0  
    }  
  \],  
  "governance\_tags": \["auth", "pii", "billing"\],  
  "created\_at":      "2025-01-15T14:23:00Z"  
}

**Contract enforcement targets:** confidence is float 0.0–1.0; created\_at is ISO 8601; code\_refs\[\] is non-empty; every file path exists in the repo.

## **Week 2 — Digital Courtroom  (verdict\_record)**

// File: outputs/week2/verdicts.jsonl  
{  
  "verdict\_id":      "uuid-v4",  
  "target\_ref":      "relative/path/or/doc\_id",  
  "rubric\_id":       "sha256\_hash\_of\_rubric\_yaml",  
  "rubric\_version":  "1.2.0",  // semver  
  "scores": {  
    "criterion\_name": {  
      "score":    3,            // int MUST be 1–5  
      "evidence": \["string excerpt..."\],  
      "notes":    "string"  
    }  
  },  
  "overall\_verdict":  "PASS",  // enum: PASS | FAIL | WARN  
  "overall\_score":    3.4,      // float, weighted average of scores  
  "confidence":       0.91,     // float 0.0–1.0  
  "evaluated\_at":     "2025-01-15T14:23:00Z"  
}

**Contract enforcement targets:** overall\_verdict is exactly one of {PASS, FAIL, WARN}; every score is integer 1–5; overall\_score equals weighted mean of scores dict; rubric\_id matches an existing rubric file SHA-256.

## **Week 3 — Document Refinery  (extraction\_record)**

// File: outputs/week3/extractions.jsonl  
{  
  "doc\_id":       "uuid-v4",  
  "source\_path":  "absolute/path/or/https://url",  
  "source\_hash":  "sha256\_of\_source\_file",  
  "extracted\_facts": \[  
    {  
      "fact\_id":        "uuid-v4",  
      "text":           "string — the extracted fact in plain English",  
      "entity\_refs":    \["entity\_id\_1", "entity\_id\_2"\],  
      "confidence":     0.93,  // float MUST be 0.0–1.0  
      "page\_ref":       4,     // nullable int  
      "source\_excerpt": "verbatim text the fact was derived from"  
    }  
  \],  
  "entities": \[  
    {  
      "entity\_id":       "uuid-v4",  
      "name":            "string",  
      "type":            "PERSON",  // PERSON|ORG|LOCATION|DATE|AMOUNT|OTHER  
      "canonical\_value": "string"  
    }  
  \],  
  "extraction\_model": "claude-3-5-sonnet-20241022",  
  "processing\_time\_ms": 1240,  
  "token\_count": { "input": 4200, "output": 890 },  
  "extracted\_at": "2025-01-15T14:23:00Z"  
}

**Contract enforcement targets:** confidence is float 0.0–1.0 (NOT 0–100); entity\_refs\[\] contains only IDs that exist in the entities\[\] of the same record; entity.type is one of the six enum values; processing\_time\_ms is a positive int.

## **Week 4 — Brownfield Cartographer  (lineage\_snapshot)**

// File: outputs/week4/lineage\_snapshots.jsonl  
{  
  "snapshot\_id":    "uuid-v4",  
  "codebase\_root":  "/absolute/path/to/repo",  
  "git\_commit":     "40-char-sha",  
  "nodes": \[  
    {  
      "node\_id":  "file::src/main.py",  // stable, colon-separated type::path  
      "type":     "FILE",  // FILE|TABLE|SERVICE|MODEL|PIPELINE|EXTERNAL  
      "label":    "main.py",  
      "metadata": {  
        "path":          "src/main.py",  
        "language":      "python",  
        "purpose":       "one-sentence LLM-inferred purpose",  
        "last\_modified": "2025-01-14T09:00:00Z"  
      }  
    }  
  \],  
  "edges": \[  
    {  
      "source":       "file::src/main.py",  
      "target":       "file::src/utils.py",  
      "relationship": "IMPORTS",  // IMPORTS|CALLS|READS|WRITES|PRODUCES|CONSUMES  
      "confidence":   0.95  
    }  
  \],  
  "captured\_at": "2025-01-15T14:23:00Z"  
}

**Contract enforcement targets:** every edge.source and edge.target must reference a node\_id in the nodes\[\] array of the same snapshot; edge.relationship is one of the six enum values; git\_commit is exactly 40 hex characters.

## **Week 5 — Event Sourcing Platform  (event\_record)**

// File: outputs/week5/events.jsonl  
{  
  "event\_id":        "uuid-v4",  
  "event\_type":      "DocumentProcessed",  // PascalCase, registered in schema registry  
  "aggregate\_id":    "uuid-v4",  
  "aggregate\_type":  "Document",           // PascalCase  
  "sequence\_number": 42,                   // int, monotonically increasing per aggregate  
  "payload": {},                            // event-type-specific, must pass event schema  
  "metadata": {  
    "causation\_id":   "uuid-v4 | null",  
    "correlation\_id": "uuid-v4",  
    "user\_id":        "string",  
    "source\_service": "week3-document-refinery"  
  },  
  "schema\_version":  "1.0",  
  "occurred\_at":     "2025-01-15T14:23:00Z",  
  "recorded\_at":     "2025-01-15T14:23:01Z"  // must be \>= occurred\_at  
}

**Contract enforcement targets:** recorded\_at \>= occurred\_at; sequence\_number is monotonically increasing per aggregate\_id (no gaps, no duplicates); event\_type is PascalCase and registered in your event schema registry; payload validates against the event\_type's JSON Schema.

## **LangSmith Trace Export  (trace\_record)**

// Export via: langsmith export \--project your\_project \--format jsonl \> outputs/traces/runs.jsonl  
{  
  "id":             "uuid-v4",  
  "name":           "string — chain or LLM name",  
  "run\_type":       "llm",  // llm|chain|tool|retriever|embedding  
  "inputs":         {},  
  "outputs":        {},  
  "error":          null,    // string | null  
  "start\_time":     "2025-01-15T14:23:00Z",  
  "end\_time":       "2025-01-15T14:23:02Z",  
  "total\_tokens":   5090,  
  "prompt\_tokens":  4200,  
  "completion\_tokens": 890,  
  "total\_cost":     0.0153,  // float USD  
  "tags":           \["week3", "extraction"\],  
  "parent\_run\_id":  "uuid-v4 | null",  
  "session\_id":     "uuid-v4"  
}

**Contract enforcement targets:** end\_time \> start\_time; total\_tokens \= prompt\_tokens \+ completion\_tokens; run\_type is one of the five enum values; total\_cost \>= 0\. This contract is enforced by the AI Contract Extension in Phase 4\.

## **The Dependency Graph — Which Schema Feeds Which**

Each arrow below is a contract. You will enforce at least two of these contracts explicitly in Phase 2\.

Week 1 intent\_record.code\_refs\[\]    ──►  Week 2 verdict: target\_ref is a code\_refs.file  
Week 3 extraction\_record            ──►  Week 4 lineage: doc\_id becomes a node, facts become metadata  
Week 4 lineage\_snapshot             ──►  Week 7 ViolationAttributor (REQUIRED DEPENDENCY)  
Week 5 event\_record                 ──►  Week 7 schema contract: payload validated against event schema  
LangSmith trace\_record              ──►  Week 7 AI Contract Extension: trace schema enforced  
Week 2 verdict\_record               ──►  Week 7 AI Contract Extension: LLM output schema validation

# **New Skills Introduced**

### **Technical Skills**

* **Data contract specification formats:** Bitol Open Data Contract Standard (bitol-io/open-data-contract-standard), dbt schema.yml test definitions, JSON Schema draft-07 for payload validation.

* **Statistical profiling at scale:** Distribution characterisation, outlier detection, column-level cardinality estimation using pandas-profiling / ydata-profiling. Knowing the difference between a structural violation and a statistical drift.

* **Schema evolution taxonomy:** Backward/forward/full compatibility model from Confluent Schema Registry; breaking-change detection; deprecation-with-alias patterns.

* **Lineage-based attribution:** Graph traversal for blame-chain construction using the Week 4 lineage graph; temporal ordering of upstream commits; confidence scoring for causal attribution.

* **AI-specific data contracts:** Embedding drift detection via cosine distance; prompt input schema validation with JSON Schema; structured LLM output enforcement; LangSmith trace schema contracts.

### **FDE Skills**

* **The 48-hour data audit:** Produce a baseline data quality assessment of any client's primary data sources within 48 hours of access. This week you do it on your own platform — the hardest kind, because you cannot claim ignorance.

* **Non-technical data quality communication:** Translating validation results and schema violations into business risk language that a product manager can act on without a glossary.

* **Contract negotiation:** Facilitating the conversation between upstream data producers (you, two weeks ago) and downstream AI consumers (you, now) about formal quality commitments. The discipline of treating past-you as a third party.

## **Compounding Architecture Note**

The Data Contract Enforcer's violation log and schema snapshots become first-class inputs for subsequent weeks. The Week 8 Sentinel consumes contract violation events as data quality signals alongside LLM trace quality signals. An FDE who builds the Enforcer correctly this week saves two days of integration work in Week 8\. Build the violation log schema now with this in mind: every violation record written this week must be ingestible by Week 8's alert pipeline without modification.

# **Phase 0 — Domain Reconnaissance** 

Before writing implementation code, you must develop and document a working mental model of the domain. Your DOMAIN\_NOTES.md is graded as a primary deliverable and forms part of the Thursday submission.

## **Core Concepts to Master**

* **Data Contracts:** A formal specification of what a dataset promises to provide. Three dimensions: structural (column names, types, nullability), statistical (value ranges, distribution shapes, cardinality), temporal (freshness SLA, update frequency). The Bitol Open Data Contract Standard is the emerging industry specification — read it: bitol-io/open-data-contract-standard on GitHub.

* **Schema Evolution Taxonomy:** Not all schema changes are equally dangerous. Study the Confluent Schema Registry backward/forward/full compatibility model — it is the clearest taxonomy available and you will implement a subset of it in Phase 3\.

* **dbt Test Architecture:** dbt's schema tests (not\_null, unique, accepted\_values, relationships) are the most widely-deployed contract enforcement in practice. Understand how a contract clause maps to a dbt test. Your ContractGenerator must output dbt-compatible schema.yml as one of its formats.

* **AI-Specific Contract Extensions:** Standard data contracts cover tabular data. AI systems add new requirements: embedding drift detection, prompt input validation, structured output enforcement. These are gaps in existing tooling that you fill this week.

* **Statistical vs. Structural Violations:** A column renamed from confidence to confidence\_score is a structural violation — easy to detect. A column whose mean shifts from 0.87 to 51.3 because someone changed the scale from 0.0–1.0 to 0–100 is a statistical violation — this is the class of failure that causes production incidents.

## **DOMAIN\_NOTES.md Deliverable** 

Your domain notes must answer all five questions with evidence, not assertions. Each answer should include a concrete example from your own Weeks 1–5 systems.

1. What is the difference between a backward-compatible and a breaking schema change? Give three examples of each, drawn from your own week 1–5 output schemas defined above.

2. The Week 3 Document Refinery's confidence field is float 0.0–1.0. An update changes it to integer 0–100. Trace the failure this causes in the Week 4 Cartographer. Write the data contract clause that would catch this change before it propagates, in Bitol YAML format.

3. The Cartographer (Week 4\) produced a lineage graph. Explain, step by step, how the Data Contract Enforcer uses that graph to produce a blame chain when a contract violation is detected. Include the specific graph traversal logic.

4. Write a data contract for the LangSmith trace\_record schema defined above. Include at least one structural clause, one statistical clause, and one AI-specific clause. Show it in Bitol-compatible YAML.

5. What is the most common failure mode of contract enforcement systems in production? Why do contracts get stale? How does your architecture prevent this?

# 

# **System Architecture**

| COMPONENT | ROLE | KEY INPUT | KEY OUTPUT | USES FROM |
| :---- | :---- | :---- | :---- | :---- |
| **ContractGenerator** | Auto-generates baseline contracts from your existing system outputs | JSONL outputs from Weeks 1–5 \+ Week 4 lineage graph | Contract YAML files (Bitol) \+ dbt schema.yml | Week 4 lineage (required) |
| **ValidationRunner** | Executes all contract checks on a dataset snapshot | Dataset snapshot \+ contract YAML | Structured validation report (PASS/FAIL/WARN/ERROR per clause) | ContractGenerator output |
| **ViolationAttributor** | Traces violations back to the upstream commit that caused them | Validation failures \+ Week 4 lineage graph \+ git log | Blame chain: {file, author, commit, timestamp, confidence} | Week 4 lineage (required) |
| **SchemaEvolutionAnalyzer** | Classifies schema changes and generates migration impact reports | Schema snapshots over time | Compatibility verdict \+ migration impact report \+ rollback plan | ValidationRunner snapshots |
| **AI Contract Extensions** | Applies contracts to AI-specific data patterns — embeddings, LLM I/O, trace schema | LangSmith trace JSONL, embedding vectors, Week 2 verdict records | Embedding drift score \+ output schema violation rate \+ trace contract report | All prior components |
| **ReportGenerator** | Auto-generates the Enforcer Report from live validation data | violation\_log/ \+ validation\_reports/ \+ ai\_metrics.json | enforcer\_report/report\_data.json \+ report\_{date}.pdf | All prior components |

# 

# **Phase 1 — ContractGenerator**

The ContractGenerator reads from your outputs/ directories and the Week 4 lineage graph and produces contract YAML files. The goal is a contract that is immediately useful — one that a teammate can read and understand without asking you to explain it.

## **Repository Layout (required)**

Your submission must follow this directory structure exactly. The evaluation scripts will look for files at these paths.

your-week7-repo/  
├── contracts/  
│   ├── generator.py           \# ContractGenerator entry point  
│   ├── runner.py              \# ValidationRunner entry point  
│   ├── attributor.py          \# ViolationAttributor entry point  
│   ├── schema\_analyzer.py     \# SchemaEvolutionAnalyzer entry point  
│   └── ai\_extensions.py       \# AI Contract Extensions entry point  
│   └── report\_generator.py    \# EnforcerReport entry point  
├── generated\_contracts/       \# OUTPUT: auto-generated YAML contract files  
│   ├── week1\_intent\_records.yaml  
│   ├── week3\_extractions.yaml  
│   ├── week4\_lineage.yaml  
│   ├── week5\_events.yaml  
│   └── langsmith\_traces.yaml  
├── validation\_reports/        \# OUTPUT: structured validation report JSON  
├── violation\_log/             \# OUTPUT: violation records JSONL  
├── schema\_snapshots/          \# OUTPUT: timestamped schema snapshots  
├── enforcer\_report/           \# OUTPUT: stakeholder PDF \+ data  
├── outputs/                   \# INPUT: symlink or copy of your weeks 1–5 outputs  
│   ├── week1/intent\_records.jsonl  
│   ├── week2/verdicts.jsonl  
│   ├── week3/extractions.jsonl  
│   ├── week4/lineage\_snapshots.jsonl  
│   ├── week5/events.jsonl  
│   └── traces/runs.jsonl      \# from LangSmith export  
└── DOMAIN\_NOTES.md

## **Contract Generation Pipeline**

* **Step 1 — Structural profiling.** Run ydata-profiling (pip install ydata-profiling) on each JSONL file after loading into a Pandas DataFrame. For each column: name, dtype, null fraction, cardinality estimate, five sample distinct values, and for string columns the dominant character pattern.

* **Step 2 — Statistical profiling.** For numeric columns: min, max, mean, p25, p50, p75, p95, p99, stddev. For the confidence column specifically: assert 0.0 \<= min and max \<= 1.0 and flag any distribution with mean \> 0.99 (almost certainly clamped) or mean \< 0.01 (almost certainly broken).

* **Step 3 — Lineage context injection.** Open the latest snapshot from outputs/week4/lineage\_snapshots.jsonl. For each contract column, query the lineage graph to find which downstream nodes consume the table containing that column. Store as downstream\_consumers\[\] in the contract. This enables blast-radius computation in Phase 2\.

* **Step 4 — LLM annotation.** For any column whose business meaning is ambiguous from name and sample values alone, invoke Claude with the column name, table name, five sample values, and adjacent column names. Ask for: (a) a plain-English description, (b) a business rule as a validation expression, (c) any cross-column relationship. Append to the contract as llm\_annotations.

* **Step 5 — dbt output.** For every contract YAML generated, produce a parallel dbt schema.yml with equivalent test definitions: not\_null for required fields, accepted\_values for enum fields, relationships for foreign keys. Place in generated\_contracts/{name}\_dbt.yml.

## **Bitol Contract YAML — Concrete Example**

The following shows the contract that ContractGenerator must produce for the Week 3 extraction\_record. Every field is present and every clause is machine-checkable.

\# generated\_contracts/week3\_extractions.yaml  
kind: DataContract  
apiVersion: v3.0.0  
id: week3-document-refinery-extractions  
info:  
  title: Week 3 Document Refinery — Extraction Records  
  version: 1.0.0  
  owner: week3-team  
  description: \>  
    One record per processed document. Each record contains all facts  
    extracted from the source document and the entities referenced.  
servers:  
  local:  
    type: local  
    path: outputs/week3/extractions.jsonl  
    format: jsonl  
terms:  
  usage: Internal inter-system data contract. Do not publish.  
  limitations: confidence must remain in 0.0–1.0 float range.  
schema:  
  doc\_id:  
    type: string  
    format: uuid  
    required: true  
    unique: true  
    description: Primary key. UUIDv4. Stable across re-extractions of the same source.  
  source\_hash:  
    type: string  
    pattern: "^\[a-f0-9\]{64}$"  \# SHA-256  
    required: true  
    description: SHA-256 of the source file. Changes iff the source content changes.  
  extracted\_facts:  
    type: array  
    items:  
      confidence:  
        type: number  
        minimum: 0.0  
        maximum: 1.0        \# BREAKING CHANGE if changed to 0–100  
        required: true  
      fact\_id:  
        type: string  
        format: uuid  
        unique: true  
  extraction\_model:  
    type: string  
    required: true  
    description: Model identifier. Must match pattern claude-\* or gpt-\*.  
    pattern: "^(claude|gpt)-"  
quality:  
  type: SodaChecks  
  specification:  
    checks for extractions:  
      \- missing\_count(doc\_id) \= 0  
      \- duplicate\_count(doc\_id) \= 0  
      \- min(confidence\_mean) \>= 0.0  
      \- max(confidence\_mean) \<= 1.0  
      \- row\_count \>= 1  
lineage:  
  upstream: \[\]  
  downstream:  
    \- id: week4-cartographer  
      description: Cartographer ingests doc\_id and extracted\_facts as node metadata  
      fields\_consumed: \[doc\_id, extracted\_facts, extraction\_model\]  
      breaking\_if\_changed: \[extracted\_facts.confidence, doc\_id\]

## **Contract Quality Floor**

A generated contract that requires more than 10 minutes of manual review to be trustworthy is not useful. Run the ContractGenerator on at least two of your own system outputs (Week 3 and Week 5 are required minimums) and measure the fraction of generated clauses that are correct without manual editing. Target: \> 70%. Document the fraction and any failure patterns in your DOMAIN\_NOTES.md.

# **Phase 2 \- ValidationRunner & ViolationAttributor**

## **2A — ValidationRunner**

The ValidationRunner executes every clause in a contract file against a data snapshot and produces a structured report. Run it as:

python contracts/runner.py \\  
  \--contract generated\_contracts/week3\_extractions.yaml \\  
  \--data outputs/week3/extractions.jsonl \\  
  \--output validation\_reports/week3\_$(date \+%Y%m%d\_%H%M).json

The output JSON must follow this schema exactly — evaluation scripts will parse it:

{  
  "report\_id":        "uuid-v4",  
  "contract\_id":      "week3-document-refinery-extractions",  
  "snapshot\_id":      "sha256\_of\_input\_jsonl",  
  "run\_timestamp":    "ISO 8601",  
  "total\_checks":     14,  
  "passed":           12,  
  "failed":            1,  
  "warned":            1,  
  "errored":           0,  
  "results": \[  
    {  
      "check\_id":      "week3.extracted\_facts.confidence.range",  
      "column\_name":   "extracted\_facts\[\*\].confidence",  
      "check\_type":    "range",  
      "status":        "FAIL",  
      "actual\_value":  "max=51.3, mean=43.2",  
      "expected":      "max\<=1.0, min\>=0.0",  
      "severity":      "CRITICAL",  
      "records\_failing": 847,  
      "sample\_failing": \["fact\_id\_1", "fact\_id\_2"\],  
      "message":       "confidence is in 0–100 range, not 0.0–1.0. Breaking change detected."  
    }  
  \]  
}

Severity levels: CRITICAL (structural or type violation), HIGH (statistical drift \> 3 stddev), MEDIUM (statistical drift 2–3 stddev), LOW (informational), WARNING (near-threshold).

Partial failure rule: if a check cannot execute because the column does not exist, return status \= "ERROR" with a diagnostic message and continue to the next check. Never crash. Always produce a complete report.

## **2B — ViolationAttributor**

When a validation result contains status \= "FAIL", the ViolationAttributor traces the failure to its origin.

**Step 1 — Lineage traversal.** Load the Week 4 lineage graph. Starting from the failing schema element, find the upstream node that produces it. Use breadth-first traversal, stopping at the first external boundary or file-system root.

**Step 2 — Git blame integration.** For each upstream file identified, run:

git log \--follow \--since="14 days ago" \--format='%H|%an|%ae|%ai|%s' \-- {file\_path}  
\# Then for targeted line-level blame:  
git blame \-L {line\_start},{line\_end} \--porcelain {file\_path}

**Step 3 — Blame chain output.** Write to violation\_log/violations.jsonl:

{  
  "violation\_id":    "uuid-v4",  
  "check\_id":        "week3.extracted\_facts.confidence.range",  
  "detected\_at":     "ISO 8601",  
  "blame\_chain": \[  
    {  
      "rank":             1,  
      "file\_path":        "src/week3/extractor.py",  
      "commit\_hash":      "abc123def456...",  
      "author":           "jane.doe@example.com",  
      "commit\_timestamp": "2025-01-14T09:00:00Z",  
      "commit\_message":   "feat: change confidence to percentage scale",  
      "confidence\_score": 0.94  
    }  
  \],  
  "blast\_radius": {  
    "affected\_nodes":    \["file::src/week4/cartographer.py"\],  
    "affected\_pipelines":\["week4-lineage-generation"\],  
    "estimated\_records": 847  
  }  
}

Confidence score formula: base \= 1.0 − (days\_since\_commit × 0.1). Reduce by 0.2 for each lineage hop between the blamed file and the failing column. Never return fewer than 1 candidate or more than 5\.

## **The Statistical Drift Rule (Silent Corruption Detection)**

The most dangerous violations are the ones that pass structural checks. Implement this rule in the ValidationRunner: for every numeric column that has an established baseline mean and stddev (from the first validation run on that contract), emit a WARNING if the current mean deviates by more than 2 stddev and a FAIL if it deviates by more than 3 stddev. Store baselines in schema\_snapshots/baselines.json. This catches the confidence 0.0–1.0 → 0–100 change even if the type check passes.

# 

# **Phase 3 — SchemaEvolutionAnalyzer**

Schema evolution is inevitable. The SchemaEvolutionAnalyzer does not prevent it — it makes it safe by classifying every detected change and generating the impact report that downstream consumers need to adapt.

## **Schema Snapshot Discipline**

On every ContractGenerator run, write a timestamped snapshot of the inferred schema to schema\_snapshots/{contract\_id}/{timestamp}.yaml. The SchemaEvolutionAnalyzer diffs consecutive snapshots to detect changes. Without this, you can detect that a change happened but not when — which makes the blame chain unreliable.

python contracts/schema\_analyzer.py \\  
  \--contract-id week3-document-refinery-extractions \\  
  \--since "7 days ago" \\  
  \--output validation\_reports/schema\_evolution\_week3.json

## **Change Classification Taxonomy**

| CHANGE TYPE | EXAMPLE | BACKWARD COMPATIBLE? | REQUIRED ACTION |
| :---- | :---- | :---- | :---- |
| Add nullable column | ADD COLUMN notes TEXT NULL | Yes | None. Downstream consumers can ignore the new column. |
| Add non-nullable column | ADD COLUMN required\_field TEXT NOT NULL | No | Coordinate with all producers. Provide a default or migration script. Block deployment until all producers updated. |
| Rename column | confidence → confidence\_score | No | Deprecation period with alias column. Notify all downstream consumers via blast radius report. Minimum 1 sprint before removal. |
| Change type (widening) | INT → BIGINT or float32 → float64 | Usually yes | Validate no precision loss on existing data. Re-run statistical checks to confirm distribution unchanged. |
| Change type (narrowing) | float 0.0–1.0 → int 0–100 | No — data loss risk | CRITICAL. Requires explicit migration plan with rollback. Blast radius report mandatory. Statistical baseline must be re-established after migration. |
| Remove column | DROP COLUMN old\_field | No | Deprecation period mandatory (minimum 2 sprints). Blast radius report required. Each affected consumer must acknowledge removal in writing (JIRA ticket or PR comment). |
| Change enum values | Add "EXTERNAL" to node type enum | Usually yes (additive) | Additive: notify all consumers. Removal of existing value: treat as breaking change. |

## **Migration Impact Report Format**

When a breaking change is detected, auto-generate migration\_impact\_{contract\_id}\_{timestamp}.json containing: the exact diff (human-readable), compatibility verdict, full blast radius from the lineage graph, per-consumer failure mode analysis, an ordered migration checklist, and a rollback plan. This is the document you hand to the team lead.

# **Phase 4 — AI Contract Extensions & The Enforcer Report**

## **4A — AI-Specific Contract Clauses**

Standard data contracts cover tabular data. The following three extensions cover AI system requirements that no existing framework provides out of the box.

### **Extension 1: Embedding Drift Detection**

Applies to: the extracted\_facts\[\*\].text column in Week 3 outputs (text fed to embeddings before being stored or searched).

Implementation: On baseline run, embed a random sample of 200 text values using text-embedding-3-small. Store the centroid vector in schema\_snapshots/embedding\_baselines.npz. On each subsequent run, embed a fresh sample of 200 values and compute cosine distance from the stored centroid.

\# contracts/ai\_extensions.py  
def check\_embedding\_drift(column\_values, baseline\_path, threshold=0.15):  
    current \= embed\_sample(column\_values, n=200)  
    current\_centroid \= np.mean(current, axis=0)  
    baseline\_centroid \= np.load(baseline\_path)\['centroid'\]  
    drift \= 1 \- cosine\_similarity(\[current\_centroid\], \[baseline\_centroid\])\[0\]\[0\]  
    return {  
        'drift\_score': round(drift, 4),  
        'status': 'FAIL' if drift \> threshold else 'PASS',  
        'threshold': threshold  
    }

### **Extension 2: Prompt Input Schema Validation**

Applies to: any structured data interpolated into a prompt template. For Week 3, this is the document metadata object passed into the extraction prompt.

Define the expected prompt input as a JSON Schema. Validate every record before it enters the prompt. Quarantine non-conforming records to outputs/quarantine/{timestamp}.jsonl — do not drop them silently.

\# generated\_contracts/prompt\_inputs/week3\_extraction\_prompt\_input.json  
{  
  "$schema": "http://json-schema.org/draft-07/schema\#",  
  "type": "object",  
  "required": \["doc\_id", "source\_path", "content\_preview"\],  
  "properties": {  
    "doc\_id":          { "type": "string", "format": "uuid" },  
    "source\_path":     { "type": "string", "minLength": 1 },  
    "content\_preview": { "type": "string", "maxLength": 8000 }  
  },  
  "additionalProperties": false  
}

### **Extension 3: Structured LLM Output Enforcement**

Applies to: Week 2 verdict records (structured LLM output) and any system where an LLM returns JSON.

Define the expected output schema. Validate every LLM response against it. Track the output\_schema\_violation\_rate metric per prompt version — a rising rate signals prompt degradation or model behaviour change. Write violations to violation\_log/ as type \= "llm\_output\_schema".

\# Track this metric in validation\_reports/ai\_metrics.json  
{  
  "run\_date": "2025-01-15",  
  "prompt\_hash": "abc123...",  
  "total\_outputs": 847,  
  "schema\_violations": 12,  
  "violation\_rate": 0.0142,  
  "trend": "stable",  // 'rising' triggers WARN, 'stable'/'falling' are OK  
  "baseline\_violation\_rate": 0.0089

## **4B — The Enforcer Report**

The Enforcer Report is the document you leave behind. It must be auto-generated from your live data and be readable by someone who has never heard of a data contract.

Required sections in enforcer\_report/report\_{date}.pdf:

1. **Data Health Score:** A single 0–100 score for the monitored data system, with a one-sentence narrative. Formula: (checks\_passed / total\_checks) × 100, adjusted down by 20 points for each CRITICAL violation.

2. **Violations this week:** Count by severity. Plain-language description of the three most significant violations. Each description must name the failing system, the failing field, and the impact on downstream consumers.

3. **Schema changes detected:** A plain-language summary of every schema change observed in the past 7 days, with its compatibility verdict and what action is required of the downstream team.

4. **AI system risk assessment:** Based on the AI Contract Extensions. Are the AI systems currently consuming reliable data? Is embedding drift within acceptable bounds? Is the LLM output schema violation rate stable?

5. **Recommended actions:** Three prioritised actions for the data engineering team, ordered by risk reduction value. Each action must be specific: not "fix the schema" but "update src/week3/extractor.py to output confidence as float 0.0–1.0 per contract week3-document-refinery-extractions clause extracted\_facts.confidence.range".

**Report Generation Script**

The Enforcer Report must be produced programmatically by contracts/report\_generator.py. Run it after all validation, attribution, and AI extension steps are complete.

# 

# **Interim Submission  (due Thursday 03:00 UTC)**

***GitHub link \+ public Google Drive link to PDF report required. Submissions without both links are not evaluated.***

## **What Must Be In Your GitHub Repository by Thursday 03:00 UTC**

* **DOMAIN\_NOTES.md:** All five Phase 0 questions answered with evidence and concrete examples from your own systems. Minimum 800 words.

* **generated\_contracts/:** At minimum, contracts for Week 3 extractions and Week 5 events, in Bitol-compatible YAML. Each contract must have at least 8 clauses. dbt schema.yml counterparts present.

* **contracts/generator.py:** Runnable ContractGenerator. Evaluators will run: python contracts/generator.py \--source outputs/week3/extractions.jsonl \--output generated\_contracts/. It must complete without errors and produce valid YAML.

* **contracts/runner.py:** Runnable ValidationRunner. Evaluators will run: python contracts/runner.py \--contract generated\_contracts/week3\_extractions.yaml \--data outputs/week3/extractions.jsonl. Must produce a validation report JSON matching the schema in Phase 2\.

* **outputs/ directory:** At least 50 records in each of outputs/week3/extractions.jsonl and outputs/week5/events.jsonl. If your previous systems did not produce this format, include a migration script and the migrated output.

* **validation\_reports/:** At least one real validation report from running the ValidationRunner on your own data. Not a fabricated example.

## **Thursday PDF Report — Required Sections**

The PDF must be linked from your Google Drive as a public shareable link. It must contain:

1. Data Flow Diagram: your five systems with arrows annotated with schema names. Can be a hand-drawn photo, a Miro board screenshot, or a generated diagram — the content matters, not the tool.

2. Contract Coverage Table: a table listing every inter-system interface, whether a contract has been written for it (Yes/Partial/No), and if No, why not.

3. First Validation Run Results: a summary of the ValidationRunner results on your own data. How many checks passed? Were any violations found? If violations were found — real or injected — describe them.

4. Reflection (max 400 words): What did you discover about your own systems that you did not know before writing the contracts? What assumptions turned out to be wrong?

# 

# **Final Submission  (due Sunday 03:00 UTC)**

***GitHub link \+ public Google Drive link to PDF report  \+  public Google Drive Link for Demo Video required. All must be accessible without a login.***

## **What Must Be In Your GitHub Repository by Sunday 03:00 UTC**

Everything from the Thursday submission, plus:

* **contracts/attributor.py:** Runnable ViolationAttributor. When run against a violation log entry, it must produce a blame chain JSON with at least one ranked candidate, a commit hash, and a blast radius.

* **contracts/schema\_analyzer.py:** Runnable SchemaEvolutionAnalyzer. Must produce a schema diff and compatibility verdict when run on two snapshots. Must classify at least one change as breaking.

* **contracts/ai\_extensions.py:** All three AI extensions implemented. The embedding drift check must run on real extracted\_facts text values. The LLM output schema check must run on real Week 2 verdict records.

* **contracts/report\_generator.py:** Runnable ReportGenerator. Must produce enforcer\_report/report\_data.json with a data\_health\_score between 0 and 100\.

* **violation\_log/violations.jsonl:** At least 3 violation records — at least 1 must be a real violation found in your own data, and at least 1 must be an intentionally injected violation with the injection documented in a comment at the top of the file.

* **schema\_snapshots/:** At least 2 timestamped snapshots per contract demonstrating the evolution tracking. If you made no schema changes, inject one: change a field type in your test data and run the generator again.

* **enforcer\_report/:** A generated Enforcer Report covering the full submission period. Must be machine-generated from your violation\_log and validation\_reports — not hand-written.

* **README.md:** One-page guide explaining how to run each of the five entry-point scripts end-to-end on a fresh clone of the repo. Include the expected output for each. Evaluators will follow this guide.

## 

## **Sunday PDF Report — Required Sections**

1. Enforcer Report (auto-generated): embed or link the machine-generated Enforcer Report. If embedded, it must be clearly labelled as auto-generated.

2. Violation Deep-Dive: for the most significant violation found (real or injected), walk through the full blame chain. Show the failing check, the lineage traversal, the git commit identified, and the blast radius.

3. AI Contract Extension Results: show the embedding drift score, the LLM output schema violation rate, and whether either metric triggered a WARN or FAIL. Include the raw numbers.

4. Schema Evolution Case Study: describe one schema change you detected (real or injected). Show the diff, the compatibility verdict from the taxonomy, and the migration impact report.

5. What Would Break Next: given what you now know about your data contracts, name the single highest-risk inter-system interface in your platform — the one most likely to fail silently in production — and explain why.

   

## **Video Demo (max 6 min):**

**Minutes 1–3:**

* Step 1: Contract Generation: Run contracts/generator.py on outputs/week3/extractions.jsonl live. Show the generated YAML file with at least 8 clauses including the extracted\_facts.confidence range clause.  
* Step 2: Violation Detection: Run contracts/runner.py against the violated dataset. Show the FAIL result for the confidence range check, the severity level, and the count of failing records in the structured JSON report.  
* Step 3: Blame Chain: Run contracts/attributor.py against the violation. Show the lineage traversal, the identified commit, the author, and the blast radius of affected downstream nodes.

**Minutes 4–6:**

* Step 4: Schema Evolution: Run contracts/schema\_analyzer.py diffing two snapshots. Show the breaking change classification and the generated migration impact report.  
* Step 5: AI Extensions: Run contracts/ai\_extensions.py on real Week 3 extraction text. Show the embedding drift score, the prompt input validation result, and the LLM output schema violation rate.  
* Step 6: Enforcer Report: Run contracts/report\_generator.py end-to-end. Show the auto-generated report\_data.json with the data health score and the top three violations in plain language.

# 

# 

# 

# 

# **Assessment Rubric**

Each criterion is scored 1–5. Score 3 \= functional. Score 5 \= production-ready and field-deployable. Evaluators run your scripts; they do not take your word for it.

| CRITERION | SCORE 1 | SCORE 2 | SCORE 3 | SCORE 4 | SCORE 5 |
| :---- | :---- | :---- | :---- | :---- | :---- |
| **ContractGenerator** | Manual contract only; no generation code | Structural profiling only; no statistics | Structural \+ statistical; generates valid YAML for Week 3 and Week 5 | LLM annotation; dbt YAML output; lineage context injected | All above \+ \>70% of clauses survive review; runs on evaluator machine without errors |
| **ValidationRunner** | Crashes on bad input | Runs; output is unstructured text | Structured JSON report; PASS/FAIL/WARN/ERROR per clause; correct schema | Partial failure handling; statistical drift detection; ERROR status on missing columns | All above \+ detects injected violation; statistical drift catches 0.0-1.0 → 0-100 change |
| **ViolationAttributor** | No attribution | Points to upstream file only | Git blame integrated; commit identified; violation log written | Ranked blame chain with confidence scores; blast radius report | All above \+ evaluator can trace a real violation from failing check to specific commit in your git history |
| **SchemaEvolutionAnalyzer** | No change detection | Detects changes; no classification | Taxonomy applied; compatibility verdict produced | Migration impact report generated; temporal snapshots stored and diffable | All above \+ rollback plan; evaluator can diff two snapshots using your CLI and get a migration checklist |
| **AI Contract Extensions** | No AI extensions | Prompt input schema validation only | Embedding drift check \+ prompt schema; both run on real data | All 3 extensions; output schema violation rate tracked as metric | All above \+ rising violation rate triggers WARN in violation log; demo shows detection of a real drift |
| **Enforcer Report** | No report or raw data dump | Report exists; technical jargon throughout | Plain language; all 5 sections present; Data Health Score present | Auto-generated from live validation data; not hand-written | A non-engineer reads the report and identifies the correct action without any explanation from you. Test this. |
| **DOMAIN\_NOTES.md** | Surface definitions only | Concepts described; no examples from own systems | All 5 questions answered with examples from your own Weeks 1–5 schemas | Answers reference specific tool internals; Bitol YAML example is syntactically valid | Answers demonstrate ability to predict failure modes before they occur. The confidence scale change example is worked through end-to-end. |

