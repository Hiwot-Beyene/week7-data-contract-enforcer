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

# **How This Maps to the Real World**

Before implementing anything, you need an honest mental model of where this project sits relative to production data contract systems. The project simplifies several things deliberately — the simplifications are pedagogically sound, but an FDE who does not understand them will architect incorrectly in the field.

## **The Three Trust Boundary Tiers**

Every architectural decision in data contract enforcement changes depending on which tier you are operating in. This project operates in Tier 1\. You must understand all three.

| TIER | BOUNDARY | WHAT YOU CAN SEE | BLAST RADIUS METHOD | REAL EXAMPLE |
| :---- | :---- | :---- | :---- | :---- |
| **1 — Same team** | Single repo or monorepo | Full lineage graph. Git history of all producers. All schemas. | Traverse lineage graph downstream from producer node. Full transitive depth. | This project. All five systems in one org. |
| **2 — Same company** | Multiple teams, shared data platform | Your own systems \+ published contracts from other teams. Not their internal graphs. | Registry subscription query: who subscribed to this contract? Then each team runs their own internal blast radius. | Netflix inter-team data mesh. Airbnb's Minerva metric platform. |
| **3 — Different companies** | API / data partnership boundary | Your own systems only. The partner is a black box. | Subscriber count from registry \+ version compatibility matrix. No graph traversal. | Stripe publishing API schema changes. AWS S3 event format changes affecting third-party consumers. |

**In the real world:**  *Most data contract failures happen at Tier 2 boundaries — between teams inside the same company who assumed informal coordination would suffice. The tools exist (DataHub, OpenMetadata, dbt Mesh) but adoption lags because writing contracts feels like overhead until the first production incident caused by a silent schema change. After that incident, everyone wants contracts retroactively.*

## **The Core Architectural Principle: Enforcement Is Always at the Consumer**

Enforcement runs at the consumer's ingestion boundary — not at the producer, and not in transit. This is the most important principle in the project and the one most frequently misunderstood.

The producer publishes the contract: a formal declaration of what it promises to deliver. The consumer runs the ValidationRunner against incoming data before any business logic processes it. If the check fails, the pipeline stops. The producer never knew the consumer was checking.

This mirrors how APIs work in practice. Stripe publishes their API schema. You validate responses on your side before processing them. You do not trust that the data is valid simply because it arrived.

**⚠  The SchemaEvolutionAnalyzer is the exception — it runs primarily on the producer side as a pre-emptive layer. It catches breaking changes before they ship to consumers. The ValidationRunner is the reactive layer that catches what the SchemaEvolutionAnalyzer missed or what changed without going through the schema evolution process.**

## **Blast Radius: Lineage Graph vs Registry Subscription Model**

The project uses the Week 4 lineage graph to compute blast radius. This works because you own all five systems and can traverse the graph. In production beyond Tier 1, this approach fails for three reasons:

* **You cannot see inside external systems.** A downstream partner's internal architecture is opaque. You cannot traverse their lineage graph.

* **The lineage graph is commercially sensitive.** It reveals architecture, data models, vendor dependencies. Companies do not publish it externally.

* **The graph goes stale.** Lineage graphs must be continuously maintained. In practice they are generated periodically and are often weeks out of date.

The production solution inverts the knowledge model. Instead of the producer discovering its consumers by traversing a graph, consumers register their dependency on a contract. The registry becomes the blast radius computation mechanism. When a producer publishes a breaking change, the registry answers: 'which subscribers are affected?' Each subscriber then independently computes their own internal blast radius.

\# contract\_registry/subscriptions.yaml — the registry model  
subscriptions:  
  \- contract\_id: week3-document-refinery-extractions  
    subscriber\_id: week4-cartographer  
    fields\_consumed: \[doc\_id, extracted\_facts, extraction\_model\]  
    breaking\_fields: \[extracted\_facts.confidence\]  
    registered\_at: '2025-01-10T09:00:00Z'  
    contact: week4-team@org.com

  \- contract\_id: week3-document-refinery-extractions  
    subscriber\_id: week6-enforcer  
    fields\_consumed: \[extracted\_facts.confidence, doc\_id\]  
    breaking\_fields: \[extracted\_facts.confidence\]  
    registered\_at: '2025-01-10T09:00:00Z'  
    contact: week6-team@org.com

In this project the registry is a YAML file you maintain. In Tier 2–3 production systems it is a service — DataHub, OpenMetadata, or a purpose-built internal tool. The contract YAML and the ValidationRunner remain identical across all tiers. Only the blast radius computation mechanism changes.

**In the real world:**  *Confluent Schema Registry is the most widely deployed contract enforcement system. It handles Tier 2–3 by enforcing compatibility rules at write time — it refuses to register a breaking schema change unless the compatibility mode explicitly permits it. Blast radius is never computed because breaking changes are blocked before they ship. This is stricter but less flexible than the ValidationRunner approach.*

## **Real Tooling Comparison**

This table positions the Data Contract Enforcer's components against the real tools that serve similar roles in production. Understanding the comparison is what allows you to have an intelligent conversation with a client who already has tooling.

| FACET | THIS PROJECT | CONFLUENT SCHEMA REGISTRY | dbt TESTS / dbt MESH | GREAT EXPECTATIONS / SODA | PACT (API CONTRACTS) |
| :---- | :---- | :---- | :---- | :---- | :---- |
| **Contract format** | Bitol YAML \+ JSON Schema | Avro / Protobuf / JSON Schema in registry | schema.yml \+ singular tests | Expectation suites / Soda checks YAML | Consumer-driven pact files (JSON) |
| **Enforcement location** | Consumer ingestion boundary (ValidationRunner) | Producer publish-time (blocks bad schema at source) | Consumer-side, before dbt run | Consumer-side, on data snapshot | Producer CI gate (pact tests run against provider) |
| **Blast radius** | Registry subscriptions \+ lineage graph (Tier 1\) | Implicit: blocked change cannot affect anyone | dbt DAG — explicit via ref() cross-project | No native blast radius — validation only | Consumer-driven: each consumer defines what it needs |
| **Schema evolution** | SchemaEvolutionAnalyzer snapshots \+ diff \+ taxonomy | Compatibility modes: BACKWARD / FORWARD / FULL — enforced at registration | Version field in schema.yml; manual migration | Checkpoint diffing across runs | Consumer pact is the version — provider must pass all registered pacts |
| **AI-specific contracts** | Embedding drift, prompt schema, output schema violation rate | None — schema registry is structure only | None natively — custom macros possible | Partial: column-level distribution checks, no embedding awareness | None — API contract only, no AI semantics |
| **Trust boundary** | Tier 1 (same repo). Registry design supports Tier 2\. | Tier 2–3. Used across teams and companies (Kafka ecosystem). | Tier 2 within dbt Mesh. Tier 1 without Mesh. | Tier 1–2. Usually within a single data platform. | Tier 3\. Designed for inter-company API contracts. |
| **Key strength** | AI-specific extensions. Blame chain. Human-readable report. | Prevents breaking changes before they ship. Industry standard for Kafka. | Already in client stack if they use dbt. Zero additional infrastructure. | Statistical profiling is the deepest of any tool. Handles data quality beyond schema. | Puts consumer in control. Forces explicit compatibility negotiation. |
| **Key weakness** | Lineage graph must be maintained. Blast radius limited to Tier 1 without registry. | Schema-only — no statistical or AI-specific checks. Requires Kafka/Confluent ecosystem. | Only checks what dbt can see. Cannot enforce AI-specific contracts. | No blame chain. No cross-system lineage. Verbose configuration. | Requires all consumers to maintain pact files. High coordination overhead. |

**In the real world:**  *Most mature data platforms use multiple tools: Confluent Schema Registry for streaming schemas, dbt tests for warehouse data quality, Great Expectations for statistical profiling, and a custom or open-source data catalog (DataHub, OpenMetadata) for lineage and discovery. The Data Contract Enforcer's value proposition is unifying these concerns for AI systems specifically — which none of the existing tools do well.*

# **Practical Tradeoffs Companies Actually Make**

Every design decision in this project reflects a real tradeoff that engineering teams debate. Understanding the tradeoffs — not just the implementations — is what makes you credible in a client conversation.

## **Tradeoff 1: Enforcement Strictness vs Pipeline Availability**

The ValidationRunner in this project blocks the pipeline on CRITICAL violations. In production, strict blocking is sometimes the wrong choice — especially during initial deployment when contracts are new and may have false positives.

| MODE | BEHAVIOUR | WHEN TO USE |
| :---- | :---- | :---- |
| AUDIT (default, week 1\) | Run checks, log results, never block. Write all violations to violation\_log/. | First 2 weeks of contract deployment on a client system. Contracts may have false positives. You need real production data to calibrate thresholds before you can justify blocking. |
| WARN | Block on CRITICAL only. Warn on HIGH/MEDIUM. Pass data through with violations annotated. | After calibration period. Most useful when downstream systems can handle annotated data — they filter out low-confidence records themselves. |
| ENFORCE (strict) | Block pipeline on any CRITICAL or HIGH violation. Quarantine data. Alert. | Mature contracts with low false positive rate. Mission-critical pipelines (financial data, healthcare). SLA requirements that make silent corruption unacceptable. |

Implement all three modes in your ValidationRunner with a \--mode flag. Default to AUDIT for the first run on any new dataset. Trainees who start in ENFORCE mode on a client system and create a false positive that blocks production will have a very difficult conversation with that client.

**In the real world:**  *Airbnb's Minerva platform and Spotify's Backstage both deployed data contract enforcement in AUDIT mode first, spending 4–8 weeks observing violations without blocking. This calibration period revealed which contracts were wrong before any downstream impact. Skipping the audit period is the most common deployment mistake.*

## **Tradeoff 2: Contract Completeness vs Contract Drift**

A complete contract — one that captures every structural, statistical, and semantic property of the data — is also a contract that breaks whenever the data legitimately evolves. Strict contracts become obstacles to intentional change. Loose contracts miss the violations they were meant to catch.

* **The over-specified contract trap:** A contract that specifies exact row counts, exact cardinality, and tight statistical ranges will generate constant false positives as the upstream system grows. The enforcement team starts ignoring alerts. The system loses value.

* **The under-specified contract trap:** A contract that only checks column existence and type passes the confidence 0.0–1.0 → 0–100 change because the type is still float. Statistical checks are what catch silent corruption — but they must be calibrated.

* **The practical balance:** Structural checks should be strict (exact types, required fields, enum values). Statistical checks should have configurable thresholds and a baseline refresh cadence — typically monthly or after any intentional schema change.

**In the real world:**  *dbt Labs recommends starting with only not\_null and unique tests on primary keys. Graduate to accepted\_values and relationships after one quarter. Graduate to statistical checks only after a full data cycle has been observed. This staged approach prevents the 'contract fatigue' pattern where teams disable checks because they fire too often.*

## **Tradeoff 3: Who Owns the Contract**

Three ownership models exist in production. Each has consequences for who does the work when a schema changes.

| MODEL | HOW IT WORKS | ADVANTAGE | DISADVANTAGE |
| :---- | :---- | :---- | :---- |
| Producer-owned | Producer writes the contract and publishes it. Consumers subscribe and adapt to changes. | Producer understands the data best. One authoritative source. | Producer may not know what consumers need. Can change contract without consumer input. |
| Consumer-owned (Pact model) | Each consumer publishes the subset of the contract they depend on. Producer must pass all consumer pacts to deploy. | Consumers are protected. Breaking changes are caught at producer CI gate. | High coordination overhead. N consumers × M fields \= large pact matrix. Slows producer deployment. |
| Jointly negotiated | Producer proposes. Consumers review. Contract is merged via PR with approval from both sides. | Both parties understand and agree. Fewer surprises. | Process overhead. Requires clear ownership of the contract repository. Fails without governance discipline. |

This project uses producer-owned contracts because the ContractGenerator runs on the producer's output. In a real multi-team engagement, push the client toward jointly negotiated contracts for any interface between two different teams. Producer-owned contracts without consumer review accumulate silent assumptions.

**In the real world:**  *Pact (pact.io) is the most rigorous implementation of consumer-driven contracts and is widely used for microservice APIs. For data pipelines, DataHub's data contracts feature (released 2023\) implements a jointly negotiated model with workflow-based approval. Neither tool handles AI-specific contracts — that gap is where the Enforcer adds unique value.*

## **Tradeoff 4: Real-Time Enforcement vs Batch Enforcement**

The ValidationRunner in this project runs on a snapshot — a complete file passed at validation time. Real production systems have data arriving continuously.

* **Batch enforcement (this project):** Run ValidationRunner on a complete snapshot before the pipeline consumes it. Simple. Works for any pipeline that processes files or daily partitions. Latency of violation detection: up to one batch cycle (hours or days).

* **Stream enforcement:** Attach validation to the stream processor (Kafka Streams, Flink, Spark Structured Streaming). Validates every message as it arrives. Latency of violation detection: seconds. Implementation complexity: high. Requires embedding the contract checks into the stream processing job.

* **Sampling enforcement:** Run structural checks on every record; run statistical checks on a rolling sample (e.g., 5% of records in a 1-hour window). Used when validation cost is non-trivial relative to data volume. Most practical for large-scale production.

For this project: batch enforcement is correct. For a client with a streaming pipeline, frame it as: 'We will deploy batch enforcement first to calibrate thresholds with zero production risk, then migrate the critical checks to stream enforcement after 30 days.'

**In the real world:**  *Great Expectations' streaming support (GX Cloud) and Soda's Soda Scan are the two most mature batch-to-stream enforcement systems. Both use the sampling model for statistical checks at scale — validating every record for structural checks and sampling for distribution checks. Full record-by-record statistical validation is rarely cost-effective above 10 million records per day.*

## **Tradeoff 5: Schema Registry Overhead vs Governance Visibility**

Maintaining a contract registry — even a YAML file — has a cost. That cost is justified in the right contexts and wasteful in others.

* **When the registry overhead is worth it:** More than two teams share a data interface. Any cross-company data dependency. Regulatory or compliance requirements that demand auditability of schema changes. AI systems where silent data corruption has direct product consequences.

* **When it is probably not worth it:** A single team owns both producer and consumer. The interface is changed by one person who also maintains the downstream code. The data is exploratory / experimental and schemas change daily.

* **The minimum viable registry:** A YAML file in a shared repo that lists every inter-system dependency with the consuming fields and a contact email. This costs one hour to set up and saves days when the first schema change happens. Start here.

**In the real world:**  *The most common data contract failure mode is not missing tooling — it is missing process. Teams have DataHub or OpenMetadata deployed, but nobody requires engineers to update the catalog when schemas change. The registry only works if schema changes require a registry update as part of the deployment process. The enforcement step that makes this real: the producer's CI pipeline must run the SchemaEvolutionAnalyzer before any deploy. If a breaking change is detected without a matching registry notification, the deploy fails.*

# 

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

Each arrow below represents a contract. The subscribing systems register in the contract registry. The ValidationRunner enforces the contract at the consumer ingestion boundary.

Week 1 intent\_record.code\_refs\[\]    ──►  Week 2 verdict: target\_ref is a code\_refs.file  
Week 3 extraction\_record            ──►  Week 4 lineage: doc\_id becomes a node, facts become metadata  
Week 4 lineage\_snapshot             ──►  Week 7 ViolationAttributor (REQUIRED DEPENDENCY)  
Week 5 event\_record                 ──►  Week 7 schema contract: payload validated against event schema  
LangSmith trace\_record              ──►  Week 7 AI Contract Extension: trace schema enforced  
Week 2 verdict\_record               ──►  Week 7 AI Contract Extension: LLM output schema validation

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

This system architecture is designed for Tier 1 (runs in your repo against your data). You could extend it with additional contract registery

| COMPONENT | ROLE | KEY INPUT | KEY OUTPUT | USES FROM |
| :---- | :---- | :---- | :---- | :---- |
| **ContractGenerator** | Auto-generates baseline contracts from your existing system outputs | JSONL outputs from Weeks 1–5 \+ Week 4 lineage graph | Contract YAML files (Bitol) \+ dbt schema.yml | Week 4 lineage (required) |
| **ValidationRunner** | Executes all contract checks on a dataset snapshot | Dataset snapshot \+ contract YAML | Structured validation report (PASS/FAIL/WARN/ERROR per clause) | ContractGenerator output |
| **ViolationAttributor** | Traces violations back to the upstream commit that caused them | Validation failures \+ Week 4 lineage graph \+ git log | Blame chain: {file, author, commit, timestamp, confidence} | Week 4 lineage (required) |
| **SchemaEvolutionAnalyzer** | Classifies schema changes and generates migration impact reports | Schema snapshots over time | Compatibility verdict \+ migration impact report \+ rollback plan | ValidationRunner snapshots |
| **AI Contract Extensions** | Applies contracts to AI-specific data patterns — embeddings, LLM I/O, trace schema | LangSmith trace JSONL, embedding vectors, Week 2 verdict records | Embedding drift score \+ output schema violation rate \+ trace contract report | All prior components |
| **ReportGenerator** | Auto-generates the Enforcer Report from live validation data | violation\_log/ \+ validation\_reports/ \+ ai\_metrics.json | enforcer\_report/report\_data.json \+ report\_{date}.pdf | All prior components |

Update your architecture to implement contract registery

| ContractRegistry | Records who subscribes to which contract and which fields they consume | Manual subscriptions.yaml entries | Blast radius list for any contract violation notification | Tier 1–2. YAML for Tier 1; service (DataHub/OpenMetadata) for Tier 2 |
| :---- | :---- | :---- | :---- | :---- |
| **ViolationAttributor** | Traces violations to upstream commit; queries registry for blast radius | Validation failures \+ lineage graph \+ git log \+ registry | Blame chain \+ subscriber blast radius (not lineage-only) | Tier 1: lineage graph. Tier 2: registry query replaces graph traversal. |

# 

# **Phase 1 — ContractGenerator**

The ContractGenerator reads from your outputs/ directories and the Week 4 lineage graph and produces contract YAML files. The goal is a contract that is immediately useful — one that a teammate can read and understand without asking you to explain it.

## **1A – Repository Layout (required)**

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

## **1B — ContractRegistry  (optional, required component)**

The ContractRegistry records who depends on which contract. This is the component that makes blast radius computation correct. Without it, the ViolationAttributor can only traverse the lineage graph — which only works in Tier 1\.

In this project the registry is a YAML file. The structure must follow this schema exactly:

\# contract\_registry/subscriptions.yaml  
\# This file is manually maintained.  
\# Every inter-system data dependency must be listed here.  
\# A subscription is a consumer's formal declaration of dependency.

subscriptions:  
  \- contract\_id: week3-document-refinery-extractions  
    subscriber\_id: week4-cartographer  
    subscriber\_team: week4  
    fields\_consumed: \[doc\_id, extracted\_facts, extraction\_model\]  
    breaking\_fields:  
      \- field: extracted\_facts.confidence  
        reason: used for node ranking; scale change breaks ranking logic  
      \- field: doc\_id  
        reason: primary key for node identity in lineage graph  
    validation\_mode: ENFORCE  
    registered\_at: '2025-01-10T09:00:00Z'  
    contact: week4-team@org.com

  \- contract\_id: week3-document-refinery-extractions  
    subscriber\_id: week6-enforcer  
    subscriber\_team: week6  
    fields\_consumed: \[extracted\_facts.confidence, doc\_id\]  
    breaking\_fields:  
      \- field: extracted\_facts.confidence  
        reason: AI extension embedding drift check baseline  
    validation\_mode: AUDIT  
    registered\_at: '2025-01-10T09:00:00Z'  
    contact: week6-team@org.com

Write one subscription entry for every arrow in your data-flow diagram. You must have at minimum four subscriptions covering the Week 3 → Week 4, Week 4 → Week 7, Week 5 → Week 7, and LangSmith → Week 7 dependencies.

**In the real world:**  *DataHub (datahubproject.io) and OpenMetadata (open-metadata.org) are the two dominant open-source data catalogs that implement registry functionality at Tier 2\. Both support subscribing to datasets and receiving notifications on schema change. Both require significant infrastructure to deploy. For a client engagement, the subscriptions.yaml pattern is the minimum viable registry — it can be migrated to a full catalog later without changing the ValidationRunner.*

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

If you have implemented the contract registry, then use the ContractRegistry as the primary blast radius source and the lineage graph as an enrichment source. This is the correct Tier 1 model — and the one that degrades gracefully to Tier 2 by replacing the lineage traversal with a registry API call.

The attribution pipeline runs in four steps:

6. **Registry blast radius query:** Load contract\_registry/subscriptions.yaml. Find all subscriptions where contract\_id matches the failing contract and breaking\_fields contains the failing field. This is the definitive subscriber list. Do not compute this from the lineage graph.

7. **Lineage traversal for enrichment:** Use the Week 4 lineage graph to compute transitive contamination — subscribers who received data from a directly affected subscriber. Annotate the blast radius with contamination\_depth. This is additive to the registry result, not a replacement for it.

8. **Git blame for cause attribution:** For each upstream file identified via lineage traversal, run git log \--follow on the file to find recent changes. Rank by temporal proximity. Produce a blame chain with confidence scores.

9. **Write violation log:** Write to violation\_log/violations.jsonl with the full blame chain, registry-sourced blast radius, and transitive depth from lineage enrichment.

The blame chain confidence score formula: base \= 1.0 − (days\_since\_commit × 0.1). Reduce by 0.2 for each lineage hop between the blamed file and the failing column. Never return more than five candidates.

**In the real world:**  *At Tier 2 and above, Step 1 is replaced by a registry API call: GET /api/subscriptions?contract\_id={id}\&breaking\_field={field}. Steps 2–4 remain identical. This is exactly how DataHub's impact analysis feature works — it queries the lineage graph for internal enrichment but uses the catalog registry as the authoritative subscriber list. The architectural pattern is: registry for 'who is affected', lineage for 'how deeply'.*

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

## 

| CHANGE TYPE | EXAMPLE IN YOUR SCHEMAS | COMPATIBLE? | REQUIRED ACTION | REAL TOOL HANDLING |
| :---- | :---- | :---- | :---- | :---- |
| Add nullable field | Add notes: string | null to extraction\_record | Yes | None. Consumers can ignore new fields. | Confluent BACKWARD mode: allows |
| Add required field | Add required\_classifier to verdict\_record | No | Coordinate with all producers. Provide default or migration script. Block deploy until all producers updated. | Confluent BACKWARD mode: blocks |
| Rename field | confidence → confidence\_score in any schema | No | Deprecation period with alias. Notify all registry subscribers. One sprint minimum before alias removal. | Confluent blocks. dbt: manual. Pact: consumer pact fails immediately. |
| Widen type | INT → BIGINT in sequence\_number | Usually yes | Validate no precision loss. Re-run statistical checks to confirm distribution unchanged. | Confluent FULL mode: allows. Most tools: pass silently. |
| Narrow type | float 0.0–1.0 → int 0–100 for confidence | No — data loss | CRITICAL. Requires migration plan with rollback. Registry blast radius report mandatory. Statistical baseline must be re-established after migration. | Confluent FORWARD mode: blocks. Great Expectations: catches via distribution check. |
| Remove field | Drop source\_excerpt from extraction\_record | No | Two-sprint deprecation minimum. Each registry subscriber must acknowledge removal. No silent drops. | Confluent blocks. Pact: consumer pact fails if field was declared. |
| Change enum values | Add EXTERNAL to Week 4 node.type enum | Additive: yes. Removal: no. | Additive additions: notify subscribers. Removal of existing value: treat as breaking — blast radius required. | Confluent BACKWARD: allows additions, blocks removals. |

**In the real world:**  *Confluent Schema Registry's compatibility enforcement is the most battle-tested breaking change prevention system available. It operates at write time (the schema cannot be registered if it breaks compatibility), which is fundamentally more reliable than detect-and-alert. If a client already uses Confluent for Kafka, recommend they extend compatibility enforcement to their non-Kafka schemas using the REST API — it supports arbitrary JSON Schema registration, not just Kafka-specific schemas.*

## **Migration Impact Report Format**

When a breaking change is detected, auto-generate migration\_impact\_{contract\_id}\_{timestamp}.json containing: the exact diff (human-readable), compatibility verdict, full blast radius from the lineage graph, per-consumer failure mode analysis, an ordered migration checklist, and a rollback plan. This is the document you hand to the team lead.

# **Phase 4 — AI Contract Extensions & The Enforcer Report**

## **4A — AI-Specific Contract Clauses**

Standard data contracts cover tabular data. The following three extensions cover AI system requirements that no existing framework provides out of the box.

### **Extension 1: Embedding Drift Detection**

Applies to: the extracted\_facts\[\*\].text column in Week 3 outputs. Any column whose text values are embedded before being stored or searched.

Implementation: on baseline run, embed a random sample of 200 text values using text-embedding-3-small. Store the centroid vector. On each subsequent run, embed a fresh sample and compute cosine distance from the stored centroid. Alert if distance exceeds threshold (default 0.15).

def check\_embedding\_drift(texts, baseline\_path, threshold=0.15):  
    current \= embed\_sample(texts, n=200)  
    current\_centroid \= np.mean(current, axis=0)  
    if not Path(baseline\_path).exists():  
        np.savez(baseline\_path, centroid=current\_centroid)  
        return {'status': 'BASELINE\_SET', 'drift\_score': 0.0}  
    baseline\_centroid \= np.load(baseline\_path)\['centroid'\]  
    cosine\_sim \= np.dot(current\_centroid, baseline\_centroid) / (  
        np.linalg.norm(current\_centroid) \* np.linalg.norm(baseline\_centroid) \+ 1e-9)  
    drift \= 1 \- cosine\_sim  
    return {'status': 'FAIL' if drift \> threshold else 'PASS',  
            'drift\_score': round(float(drift), 4), 'threshold': threshold}

**In the real world:**  *Arize AI and WhyLabs both offer commercial embedding drift monitoring. The cosine distance approach in this project matches their core algorithm. The practical challenge is establishing a meaningful baseline — a baseline from 200 records is statistically weak. Production systems use 10,000+ samples and track drift as a rolling percentile rather than a point-in-time distance. The threshold of 0.15 is a starting point; calibrate it against your actual data after observing two or three legitimate non-drifting runs.*

### **Extension 2: Prompt Input Schema Validation**

Applies to: any structured data interpolated into a prompt template. For Week 3, this is the document metadata object passed into the extraction prompt. Non-conforming records go to outputs/quarantine/ — they are never silently dropped or silently passed through.

PROMPT\_INPUT\_SCHEMA \= {  
  '$schema': 'http://json-schema.org/draft-07/schema\#',  
  'type': 'object',  
  'required': \['doc\_id', 'source\_path', 'content\_preview'\],  
  'properties': {  
    'doc\_id':          {'type': 'string', 'minLength': 36, 'maxLength': 36},  
    'source\_path':     {'type': 'string', 'minLength': 1},  
    'content\_preview': {'type': 'string', 'maxLength': 8000}  
  },  
  'additionalProperties': False  
}

**In the real world:**  *Prompt input validation is almost universally absent in production AI systems. Most teams hardcode prompt templates with f-strings and discover missing fields via LLM hallucination ('I could not find the document ID you mentioned') rather than validation errors. The gap between what a prompt template expects and what the data actually provides is the single most common source of structured output failures. Implementing this check is a fast win on any client engagement.*

### **Extension 3: LLM Output Schema Violation Rate**

Applies to: Week 2 verdict records and any system where an LLM returns structured JSON. Track the output\_schema\_violation\_rate metric per prompt version. A rising rate signals prompt degradation or model behaviour change.

def check\_output\_schema\_violation\_rate(verdict\_records, baseline\_rate=None, warn\_threshold=0.02):  
    total \= len(verdict\_records)  
    violations \= sum(1 for v in verdict\_records  
                     if v.get('overall\_verdict') not in ('PASS', 'FAIL', 'WARN'))  
    rate \= violations / max(total, 1\)  
    trend \= 'unknown'  
    if baseline\_rate is not None:  
        trend \= 'rising' if rate \> baseline\_rate \* 1.5 else 'stable'  
    return {'total\_outputs': total, 'schema\_violations': violations,  
            'violation\_rate': round(rate, 4), 'trend': trend,  
            'status': 'WARN' if rate \> warn\_threshold else 'PASS'}

**In the real world:**  *OpenAI's structured output mode (JSON mode with schema enforcement) reduces but does not eliminate output schema violations — models still occasionally produce syntactically valid JSON that does not conform to the semantic schema. Anthropic's tool use with input\_schema serves the same purpose. Tracking the violation rate over time is more useful than measuring it at a single point — a stable 1.5% violation rate is acceptable; a rate that rises from 0.5% to 3.0% over two weeks signals a prompt or model issue.*

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

