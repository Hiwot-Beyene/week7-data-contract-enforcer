Data Contract Enforcer

Practitioner Manual

*A step-by-step field guide from first commit to client-ready report.*

*Use this manual alongside the project document. The project document defines **what** to build. This manual tells you **how** to build it, in what order, with what commands, and how to know when each step is done.*

# **Mntal Model — Read This Before Writing Any Code**

Three things must be true in your head before you open a code editor. If any of these are unclear, reread the project document before continuing.

## **1\. Enforcement Always Runs at the Consumer**

The ValidationRunner runs at the boundary where a consumer ingests data from a producer. It never runs inside the producer. The producer publishes the contract — a promise about what it will deliver. The consumer enforces that promise on the data it receives, before any business logic processes it.

You own both sides in this project (all five systems are yours). In a real client engagement you will often only own the consumer side. The enforcement model is identical in both cases — you run the ValidationRunner on the data you receive, regardless of whether you own the system that produced it.

**⚠  The SchemaEvolutionAnalyzer is the only exception. It runs on the producer side as a pre-deploy gate — it catches breaking changes before they ship. Everything else runs at the consumer.**

## **2\. Blast Radius Comes from the Registry**

This is the most important architectural correction from the original project design. The lineage graph tells you how data flows inside systems you built. It cannot tell you who depends on your data outside your systems.

The ContractRegistry (subscriptions.yaml) is the correct source for blast radius. When a violation is detected, the ViolationAttributor queries the registry: 'which subscribers listed this field as breaking?' That list is the blast radius. The lineage graph then enriches it by computing transitive depth — how many hops downstream the contamination travels within systems you can see.

This distinction matters in the field. At a client with 50 teams, you will not have a lineage graph that spans all teams. You will have a registry where teams declared their dependencies. The blast radius query is the same; only the underlying mechanism changes.

**The Tier model:**  *Tier 1 (this project): you own everything, lineage graph \+ registry both available. Tier 2 (multi-team company): registry is the primary source, lineage enriches within your systems only. Tier 3 (cross-company): registry subscriber count is all you get. The ValidationRunner and contract YAML are identical across all three tiers.*

## **3\. The Registry Is the Process, Not Just the Data**

A subscriptions.yaml file sitting in a repo does nothing unless the process around it is enforced. The registry only works if schema changes require a registry update as part of deployment. In practice this means: the SchemaEvolutionAnalyzer runs in the producer's CI pipeline. If it detects a breaking change and no migration plan exists in the registry, the deploy fails.

This week you will build the technical components. In a real engagement the harder work is the process conversation — convincing the producer team that a deploy gate is worth the overhead. The correct argument: 'The last three production incidents in this system were caused by undocumented schema changes. The gate takes 30 seconds. The incidents took days to diagnose.' If you can show them their own incident history, this is not a hard sell.

# **Prerequisites — Before Hour 0**

## **Environment**

* **Python 3.11+:** Run python \--version. If below 3.11, install via pyenv.

* **Required packages:** pip install ydata-profiling pandas numpy scikit-learn jsonschema pyyaml openai anthropic langsmith gitpython soda-core

* **LangSmith export:** You must have LangSmith integrated in your Week 3–5 agents. Export at least 50 traces: langsmith export \--project your\_project \--format jsonl \> outputs/traces/runs.jsonl

* **Git:** All five prior-week repositories must be cloned locally. The ViolationAttributor runs git commands against them.

## **Data Readiness Check**

\# Verify each output file exists and has content  
wc \-l outputs/week3/extractions.jsonl    \# expect \>= 50  
wc \-l outputs/week4/lineage\_snapshots.jsonl  \# expect \>= 1  
wc \-l outputs/week5/events.jsonl         \# expect \>= 50  
wc \-l outputs/traces/runs.jsonl          \# expect \>= 50

\# Schema sanity check — run this before starting  
import json  
for path in \['outputs/week3/extractions.jsonl', 'outputs/week5/events.jsonl'\]:  
    with open(path) as f:  
        first \= json.loads(f.readline())  
    print(f'{path}: {list(first.keys())}')

**⚠  If your keys differ from the canonical schemas in the project document, write a migration script in outputs/migrate/ before proceeding. Do not silently redefine the contract to match broken data — document the deviation in DOMAIN\_NOTES.md.**

# **Day 1 — Hours 0–24**

Target: DOMAIN\_NOTES.md complete, ContractRegistry bootstrapped, ContractGenerator running, first contracts generated, first ValidationRunner report produced.

**Hours 0–2: Draw the Data Flow Diagram**

Open a blank document (Miro, Excalidraw, or paper). Draw one box per system (Weeks 1–5 \+ LangSmith). Draw an arrow for every data dependency. For each arrow write three things:

1. The file path of the data being transferred

2. The top-level keys of the schema (e.g. {doc\_id, extracted\_facts\[\], entities\[\]})

3. The name of the consuming field that would break if the producer changed its schema

Then ask: if this arrow broke silently, how long before someone noticed? This question drives your contract priority order — start with the arrows where the answer is 'days' or 'never'.

**⚠  Do not skip this step to start coding. The diagram is the engineering work. The code is the implementation of decisions already made. The diagram is also a required artefact in your Wednesday report.**

**Hours 2–3: Bootstrap the ContractRegistry**

Create contract\_registry/subscriptions.yaml before any other code. The registry is the first deliverable because it forces you to think about consumers before you think about contracts. Producers who write contracts without knowing their consumers produce contracts optimised for the producer's convenience, not the consumer's safety.

\# contract\_registry/subscriptions.yaml  
\# Write one entry per arrow in your data flow diagram.  
\# breaking\_fields is the most important field — what would actually break  
\# if the producer changed their schema?

subscriptions:  
  \- contract\_id: week3-document-refinery-extractions  
    subscriber\_id: week4-cartographer  
    fields\_consumed: \[doc\_id, extracted\_facts, extraction\_model\]  
    breaking\_fields:  
      \- field: extracted\_facts.confidence  
        reason: used for node ranking; scale change (0.0-1.0 vs 0-100) breaks ordering  
      \- field: doc\_id  
        reason: primary key used as node identity in lineage graph  
    validation\_mode: AUDIT  
    registered\_at: '2025-01-13T09:00:00Z'  
    contact: your-email@org.com

Write one subscription for each of these four minimum interfaces: Week 3 → Week 4, Week 4 → Week 6, Week 5 → Week 6, LangSmith → Week 6\. Include breaking\_fields for each. If you are not sure which fields would break the consumer, that uncertainty is itself the finding — document it.

**In the real world:**  *DataHub's 'Data Products' feature and OpenMetadata's 'Data Contracts' feature both implement this subscription model at scale. The difference from a YAML file: teams subscribe through a UI, notifications are automatic, and the registry is queryable via API. For a client with fewer than 10 teams, the YAML file is the right starting point — it has zero infrastructure cost and migrates to a full catalog later by importing the YAML.*

**Hours 3–5: Write DOMAIN\_NOTES.md**

Open DOMAIN\_NOTES.md. Answer all five Phase 0 questions. The most important question is Question 3 — the trust boundary question. This is the one that separates an FDE who understands the architecture from one who just implemented it.

For Question 3, draw a sequence diagram with these participants: Producer, ProducerCI, ContractRegistry, SubscriberA-internal, SubscriberB-external. Show the flow when the Producer detects a breaking change via SchemaEvolutionAnalyzer and publishes a notification. The key insight: SubscriberB-external only receives a notification — they compute their own internal blast radius independently. You never traverse their systems.

\# Sequence for Question 3 — draw this in your DOMAIN\_NOTES.md  
Producer          ProducerCI         Registry           SubA(internal)  SubB(external)  
   |                  |                  |                   |               |  
   |--schema change--\>|                  |                   |               |  
   |                  |--analyzer runs--\>|                   |               |  
   |                  |\<--BREAKING found-|                   |               |  
   |                  |--notify subs----\>|                   |               |  
   |                  |                  |--notify----------\>|               |  
   |                  |                  |--notify------------------------\>  |  
   |                  |                  |                   |               |  
   |                  |\<--deploy blocked-|       SubA runs own blast radius  |  
   |                  |   until migration|       on their internal systems   |  
   |                  |   plan filed     |                   SubB same      |

For Question 5 (the process failure question): the answer is not a technical failure. The answer is that contracts get stale because no process requires updating them when schemas change. Engineers update the code, update the tests, and forget to update the contract. The fix is the CI gate — the SchemaEvolutionAnalyzer as a required CI step that fails the build if a breaking change is detected without a corresponding registry entry.

**Hours 5–12: Build ContractGenerator — Four Stages**

Create contracts/generator.py. Build it in four stages. Do not skip stages — each one builds on the previous.

### **Stage 1 (90 min): Load, flatten, and profile**

\# contracts/generator.py  
import json, pandas as pd, yaml, uuid, hashlib, argparse  
from pathlib import Path  
from datetime import datetime

def load\_jsonl(path):  
    with open(path) as f:  
        return \[json.loads(l) for l in f if l.strip()\]

def flatten\_records(records, array\_key=None):  
    """Flatten nested JSONL. For week3, array\_key='extracted\_facts'"""  
    rows \= \[\]  
    for r in records:  
        base \= {k: v for k, v in r.items() if not isinstance(v, (list, dict))}  
        if array\_key and array\_key in r:  
            for item in r\[array\_key\]:  
                rows.append({\*\*base, \*\*{f'{array\_key}\_{k}': v for k, v in item.items()}})  
        else:  
            rows.append(base)  
    return pd.DataFrame(rows)

def profile\_column(series, col\_name):  
    result \= {  
        'name': col\_name,  
        'dtype': str(series.dtype),  
        'null\_fraction': float(series.isna().mean()),  
        'cardinality\_estimate': int(series.nunique()),  
        'sample\_values': \[str(v) for v in series.dropna().unique()\[:5\]\]  
    }  
    if pd.api.types.is\_numeric\_dtype(series):  
        result\['stats'\] \= {k: float(getattr(series, k)()) if hasattr(series, k)  
                           else float(series.quantile(float(k\[1:\])/100))  
                           for k in \['min','max','mean','std'\]}  
        result\['stats'\]\['p95'\] \= float(series.quantile(0.95))  
        result\['stats'\]\['p99'\] \= float(series.quantile(0.99))  
    return result

### **Stage 2 (90 min): Translate profiles to Bitol clauses**

def infer\_type(dtype\_str):  
    return {'float64':'number','float32':'number','int64':'integer',  
            'int32':'integer','bool':'boolean','object':'string'}.get(dtype\_str,'string')

def column\_to\_clause(profile):  
    clause \= {  
        'type': infer\_type(profile\['dtype'\]),  
        'required': profile\['null\_fraction'\] \== 0.0,  
        'description': f'Auto-generated. Null fraction: {profile\["null\_fraction"\]:.3f}.'  
    }  
    \# Confidence field rule: always enforce 0.0–1.0 range  
    if 'confidence' in profile\['name'\] and clause\['type'\] \== 'number':  
        clause\['minimum'\] \= 0.0  
        clause\['maximum'\] \= 1.0  
        clause\['description'\] \= ('Confidence score. MUST be float 0.0-1.0. '  
                                 'BREAKING CHANGE if changed to 0-100 scale. '  
                                 'Statistical drift check will catch scale change even if type check passes.')  
    if profile\['name'\].endswith('\_id'): clause\['format'\] \= 'uuid'  
    if profile\['name'\].endswith('\_at'): clause\['format'\] \= 'date-time'  
    \# Enum detection: if cardinality \<= 8 and type is string  
    if profile\['cardinality\_estimate'\] \<= 8 and clause\['type'\] \== 'string':  
        clause\['enum'\] \= profile\['sample\_values'\]  
    return clause

**⚠  After Stage 2, open one generated contract YAML and read it. If any clause is confusing without the code that generated it, rewrite the description field in plain English. A contract that only makes sense to its author is not a contract — it is a private note.**

### **Stage 3 (45 min): Inject lineage context from Week 4**

def inject\_lineage\_context(contract, lineage\_path, registry\_path):  
    \# Load the latest lineage snapshot  
    with open(lineage\_path) as f:  
        lines \= \[l for l in f if l.strip()\]  
    snapshot \= json.loads(lines\[-1\])  
    contract\_id \= contract\['id'\]  
    \# Find downstream nodes by traversing PRODUCES/WRITES edges  
    producer\_nodes \= \[n\['node\_id'\] for n in snapshot\['nodes'\]  
                      if contract\_id.split('-')\[0\] in n\['node\_id'\]\]  
    downstream \= \[\]  
    for edge in snapshot\['edges'\]:  
        if edge\['source'\] in producer\_nodes and edge\['relationship'\] in ('PRODUCES','WRITES'):  
            downstream.append({'node\_id': edge\['target'\], 'relationship': edge\['relationship'\]})  
    \# Load registry subscriptions for this contract  
    with open(registry\_path) as f:  
        registry \= yaml.safe\_load(f)  
    subscribers \= \[s for s in registry.get('subscriptions',\[\])  
                   if s\['contract\_id'\] \== contract\_id\]  
    contract\['lineage'\] \= {  
        'downstream\_nodes\_from\_lineage': downstream,  
        'registry\_subscribers': \[s\['subscriber\_id'\] for s in subscribers\],  
        'note': 'Blast radius uses registry\_subscribers as primary source. downstream\_nodes is enrichment only.'  
    }  
    return contract

### **Stage 4 (45 min): Write YAML and snapshot**

def write\_contract(contract, contract\_id, output\_dir):  
    output\_dir \= Path(output\_dir)  
    output\_dir.mkdir(parents=True, exist\_ok=True)  
    \# Write primary contract  
    path \= output\_dir / f'{contract\_id}.yaml'  
    with open(path, 'w') as f:  
        yaml.dump(contract, f, default\_flow\_style=False, sort\_keys=False, allow\_unicode=True)  
    \# Write timestamped snapshot for SchemaEvolutionAnalyzer  
    snap\_dir \= Path('schema\_snapshots') / contract\_id  
    snap\_dir.mkdir(parents=True, exist\_ok=True)  
    ts \= datetime.utcnow().strftime('%Y%m%d\_%H%M%S')  
    import shutil  
    shutil.copy(path, snap\_dir / f'{ts}.yaml')  
    \# Write dbt schema.yml  
    dbt \= build\_dbt\_schema(contract, contract\_id)  
    with open(output\_dir / f'{contract\_id}\_dbt.yml', 'w') as f:  
        yaml.dump(dbt, f, default\_flow\_style=False)

✓  After Stage 4: run the generator on outputs/week3/extractions.jsonl and outputs/week5/events.jsonl. Commit both generated YAMLs and the snapshot directories. This is the minimum Wednesday threshold for Phase 1\.

**Hours 12–20: Build ValidationRunner**

Create contracts/runner.py. Structural checks first, statistical checks second, never crash.

### **Structural checks — implement in this order**

* **Required field present:** For every field with required: true, check null\_fraction \== 0.0. Emit CRITICAL if any nulls.

* **Type conformance:** For every field with type: number, verify pandas dtype is float64 or int64. CRITICAL if mismatch.

* **Enum conformance:** For every field with enum: \[...\], verify all non-null values are in the list. Report count and sample of violators.

* **UUID pattern:** For fields with format: uuid, check regex ^\[0-9a-f-\]{36}$. Sample 100 records if \> 10,000 total.

* **Date-time format:** For fields with format: date-time, attempt datetime.fromisoformat(). Count failures.

* **Range check:** For fields with minimum/maximum, verify data min \>= contract minimum AND data max \<= contract maximum. This is the check that catches the 0.0–1.0 → 0–100 scale change.

### **Statistical drift check — implement after structural**

def check\_statistical\_drift(col\_name, current\_mean, current\_std, baselines\_path):  
    import json  
    if not Path(baselines\_path).exists():  
        return None  \# no baseline yet  
    with open(baselines\_path) as f:  
        baselines \= json.load(f).get('columns', {})  
    if col\_name not in baselines:  
        return None  
    b \= baselines\[col\_name\]  
    z \= abs(current\_mean \- b\['mean'\]) / max(b\['stddev'\], 1e-9)  
    if z \> 3:  
        return {'status': 'FAIL', 'z\_score': round(z, 2),  
                'severity': 'HIGH',  
                'message': f'{col\_name} mean drifted {z:.1f} stddev from baseline. Possible scale change.'}  
    elif z \> 2:  
        return {'status': 'WARN', 'z\_score': round(z, 2), 'severity': 'MEDIUM',  
                'message': f'{col\_name} approaching drift threshold ({z:.1f} stddev).'}  
    return {'status': 'PASS', 'z\_score': round(z, 2), 'severity': None}

**In the real world:**  *Great Expectations calls the statistical drift check a 'distribution check' and Great Expectations 1.x implements it via expect\_column\_mean\_to\_be\_between and expect\_column\_quantile\_values\_to\_be\_between. The z-score approach in this project is more sensitive to small sample sizes — calibrate the threshold (default 2.0 stddev) after observing your actual data variance over multiple runs.*

### **Write baseline after first run**

\# Add to end of runner.py after first successful validation  
def write\_baseline(df, output\_path='schema\_snapshots/baselines.json'):  
    from datetime import datetime  
    baselines \= {'written\_at': datetime.utcnow().isoformat(), 'columns': {}}  
    for col in df.select\_dtypes(include='number').columns:  
        baselines\['columns'\]\[col\] \= {  
            'mean': float(df\[col\].mean()), 'stddev': float(df\[col\].std()),  
            'min': float(df\[col\].min()), 'max': float(df\[col\].max())  
        }  
    Path(output\_path).parent.mkdir(exist\_ok=True)  
    with open(output\_path, 'w') as f:  
        json.dump(baselines, f, indent=2)

✓  After first run: commit validation\_reports/clean\_run.json and schema\_snapshots/baselines.json. These are proof-of-work for Wednesday.

**Hours 20–24: Wednesday prep**

Run the full generator \+ runner pipeline on clean data one final time. Capture output. Prepare the Wednesday PDF.

python contracts/generator.py \\  
  \--source outputs/week3/extractions.jsonl \\  
  \--contract-id week3-document-refinery-extractions \\  
  \--lineage outputs/week4/lineage\_snapshots.jsonl \\  
  \--registry contract\_registry/subscriptions.yaml \\  
  \--output generated\_contracts/

python contracts/runner.py \\  
  \--contract generated\_contracts/week3\_extractions.yaml \\  
  \--data outputs/week3/extractions.jsonl \\  
  \--mode AUDIT \\  
  \--output validation\_reports/wednesday\_baseline.json

The validation report JSON is your primary proof of work for Wednesday. If all checks pass on clean data, that is the expected result. Include it in the PDF and label it 'baseline run — all checks pass.'

# **Day 2 — Hours 24–48**

Target: ViolationAttributor working end-to-end, SchemaEvolutionAnalyzer classifying a breaking change, AI Contract Extensions producing real numbers.

**Hours 24–30: Inject a Known Violation, Then Find It**

Before building the ViolationAttributor, you need a violation to attribute. The rubric requires at least one attributed violation. If your data is clean (no real violations in Day 1), inject one deliberately.

### **Injection: The Scale Change Violation**

\# create\_violation.py — run once to inject the canonical violation  
import json  
records \= \[\]  
with open('outputs/week3/extractions.jsonl') as f:  
    for line in f:  
        r \= json.loads(line)  
        for fact in r.get('extracted\_facts', \[\]):  
            fact\['confidence'\] \= round(fact\['confidence'\] \* 100, 1\)  \# 0.87 → 87.0  
        records.append(r)  
with open('outputs/week3/extractions\_violated.jsonl', 'w') as f:  
    for r in records: f.write(json.dumps(r) \+ '\\n')  
\# Document the injection at top of violation\_log/violations.jsonl  
\# injection\_note: true, injected\_at: \<timestamp\>, type: scale\_change

Run the ValidationRunner against the violated file. It must produce FAIL for the confidence range check AND for the statistical drift check (mean shifted from \~0.87 to \~87.0 — well over 3 stddev). If only the range check fires, your statistical drift check is not working. Fix it before continuing.

python contracts/runner.py \\  
  \--contract generated\_contracts/week3\_extractions.yaml \\  
  \--data outputs/week3/extractions\_violated.jsonl \\  
  \--mode ENFORCE \\  
  \--output validation\_reports/violated\_run.json

**In the real world:**  *Intentional violation injection is standard practice in data quality testing. The Soda Core framework calls it 'failed check simulation' and recommends it as part of contract deployment validation. The logic: if your enforcement system cannot catch a known violation, it will not catch unknown ones. Inject before you trust.*

**Hours 30–38: Build ViolationAttributor — Registry-First**

Create contracts/attributor.py. The four-step pipeline:

### **Step 1: Registry blast radius query (primary source)**

def registry\_blast\_radius(contract\_id, failing\_field, registry\_path):  
    with open(registry\_path) as f:  
        registry \= yaml.safe\_load(f)  
    affected \= \[\]  
    for sub in registry.get('subscriptions', \[\]):  
        if sub\['contract\_id'\] \!= contract\_id:  
            continue  
        for bf in sub.get('breaking\_fields', \[\]):  
            if bf\['field'\] \== failing\_field or failing\_field.startswith(bf\['field'\]):  
                affected.append({  
                    'subscriber\_id': sub\['subscriber\_id'\],  
                    'contact': sub.get('contact', 'unknown'),  
                    'validation\_mode': sub.get('validation\_mode', 'AUDIT'),  
                    'reason': bf\['reason'\]  
                })  
                break  
    return affected

**In the real world:**  *At Tier 2, this function becomes a REST call: GET /api/registry/subscriptions?contract\_id={id}\&breaking\_field={field}. DataHub exposes this via its GraphQL API as the 'downstream lineage' query filtered by schema field. The function signature and return format remain identical — only the data source changes. This is the correct abstraction boundary.*

### **Step 2: Lineage transitive depth (enrichment, not primary source)**

def compute\_transitive\_depth(producer\_node\_id, lineage\_path, max\_depth=2):  
    with open(lineage\_path) as f:  
        lines \= \[l for l in f if l.strip()\]  
    snapshot \= json.loads(lines\[-1\])  
    visited, frontier, depth\_map \= set(), {producer\_node\_id}, {}  
    for depth in range(1, max\_depth \+ 1):  
        next\_frontier \= set()  
        for node in frontier:  
            for edge in snapshot\['edges'\]:  
                if edge\['source'\] \== node and edge\['relationship'\] in ('PRODUCES','WRITES','CONSUMES'):  
                    if edge\['target'\] not in visited:  
                        depth\_map\[edge\['target'\]\] \= depth  
                        next\_frontier.add(edge\['target'\])  
                        visited.add(edge\['target'\])  
        frontier \= next\_frontier  
    return {  
        'direct': \[n for n, d in depth\_map.items() if d \== 1\],  
        'transitive': \[n for n, d in depth\_map.items() if d \> 1\],  
        'max\_depth': max(depth\_map.values()) if depth\_map else 0}

### **Step 3: Git blame for cause attribution**

import subprocess  
def get\_recent\_commits(file\_path, repo\_root, days=14):  
    cmd \= \['git', 'log', '--follow', f'--since={days} days ago',  
           '--format=%H|%ae|%ai|%s', '--', file\_path\]  
    result \= subprocess.run(cmd, capture\_output=True, text=True, cwd=repo\_root)  
    commits \= \[\]  
    for line in result.stdout.strip().split('\\n'):  
        if '|' not in line: continue  
        h, ae, ai, s \= line.split('|', 3\)  
        commits.append({'commit\_hash': h, 'author': ae,  
                        'commit\_timestamp': ai.strip(), 'commit\_message': s})  
    return commits

def score\_candidates(commits, violation\_ts, lineage\_distance):  
    from datetime import datetime  
    scored \= \[\]  
    vt \= datetime.fromisoformat(violation\_ts.replace('Z','+00:00'))  
    for rank, c in enumerate(commits\[:5\], 1):  
        ct \= datetime.fromisoformat(c\['commit\_timestamp'\].replace(' ','+',1) if ' \+' in c\['commit\_timestamp'\] or ' \-' in c\['commit\_timestamp'\] else c\['commit\_timestamp'\])  
        days \= abs((vt \- ct).days)  
        score \= max(0.0, round(1.0 \- (days \* 0.1) \- (lineage\_distance \* 0.2), 3))  
    scored.append({\*\*c, 'rank': rank, 'confidence\_score': score})

    return sorted(scored, key=lambda x: x\['confidence\_score'\], reverse=True)

### **Step 4: Write the violation log entry**

def write\_violation(check\_result, registry\_blast, lineage\_enrichment, blame\_chain, out\_path):  
    import uuid  
    entry \= {  
        'violation\_id': str(uuid.uuid4()),  
        'check\_id': check\_result\['check\_id'\],  
        'detected\_at': datetime.utcnow().isoformat(),  
        'blast\_radius': {  
            'source': 'registry',  \# always registry-first  
            'direct\_subscribers': registry\_blast,  
            'transitive\_nodes': lineage\_enrichment\['transitive'\],  
            'contamination\_depth': lineage\_enrichment\['max\_depth'\],  
            'note': 'direct\_subscribers from registry; transitive\_nodes from lineage graph enrichment'  
        },  
        'blame\_chain': blame\_chain,  
        'records\_failing': check\_result.get('records\_failing', 0\)  
    }  
    with open(out\_path, 'a') as f:  
        f.write(json.dumps(entry) \+ '\\n')

**Hours 38–44: Build SchemaEvolutionAnalyzer**

Create contracts/schema\_analyzer.py. The analyzer diffs two schema snapshots from schema\_snapshots/{contract\_id}/ and classifies each change.

First, confirm you have two snapshots to diff. If you only ran the generator once, run it again after the violation injection — the violated data produces a different statistical profile, creating a second snapshot.

python contracts/generator.py \\  
  \--source outputs/week3/extractions\_violated.jsonl \\  
  \--contract-id week3-document-refinery-extractions \\  
  \--lineage outputs/week4/lineage\_snapshots.jsonl \\  
  \--output generated\_contracts/

def classify\_change(field, old\_clause, new\_clause):  
    if old\_clause is None:  
        req \= new\_clause.get('required', False)  
        return ('BREAKING', f'New required field {field}') if req else ('COMPATIBLE', f'New optional field {field}')  
    if new\_clause is None:  
        return ('BREAKING', f'Field removed: {field}. Deprecation period mandatory.')  
    if old\_clause.get('type') \!= new\_clause.get('type'):  
        return ('BREAKING', f'Type changed {old\_clause\["type"\]} \-\> {new\_clause\["type"\]} for {field}')  
    if old\_clause.get('maximum') \!= new\_clause.get('maximum'):  
        return ('BREAKING', f'Range changed: maximum {old\_clause.get("maximum")} \-\> {new\_clause.get("maximum")} for {field}')  
    old\_enum \= set(old\_clause.get('enum', \[\]))  
    new\_enum \= set(new\_clause.get('enum', \[\]))  
    if old\_enum \- new\_enum:  
        return ('BREAKING', f'Enum values removed from {field}: {old\_enum \- new\_enum}')  
    if new\_enum \- old\_enum:  
        return ('COMPATIBLE', f'Enum values added to {field}: {new\_enum \- old\_enum}')  
    return ('COMPATIBLE', f'No material change to {field}')

**In the real world:**  *Confluent Schema Registry implements compatibility as a write-time gate — you cannot register a new schema version if it violates the configured compatibility mode. BACKWARD mode (the most common) allows adding optional fields and removing required fields but blocks the reverse. FULL mode (stricter) requires every change to be both backward and forward compatible. For this project, implement BACKWARD compatibility as the default mode in your classify\_change function.*

**Hours 44–48: Build AI Contract Extensions**

Create contracts/ai\_extensions.py. Three checks, each independently testable.

### **Extension 1: Embedding Drift**

import numpy as np  
from openai import OpenAI  
from pathlib import Path

def embed\_sample(texts, n=200, model='text-embedding-3-small'):  
    sample \= texts\[:n\]  
    client \= OpenAI()  
    resp \= client.embeddings.create(input=sample, model=model)  
    return np.array(\[e.embedding for e in resp.data\])

def check\_embedding\_drift(texts, baseline\_path='schema\_snapshots/embedding\_baselines.npz', threshold=0.15):  
    vecs \= embed\_sample(texts)  
    centroid \= vecs.mean(axis=0)  
    if not Path(baseline\_path).exists():  
        np.savez(baseline\_path, centroid=centroid)  
        return {'status': 'BASELINE\_SET', 'drift\_score': 0.0,  
                'message': 'Baseline established. Run again to detect drift.'}  
    baseline \= np.load(baseline\_path)\['centroid'\]  
    sim \= np.dot(centroid, baseline) / (np.linalg.norm(centroid) \* np.linalg.norm(baseline) \+ 1e-9)  
    drift \= float(1 \- sim)  
    return {'status': 'FAIL' if drift \> threshold else 'PASS',  
            'drift\_score': round(drift, 4), 'threshold': threshold,  
            'interpretation': 'semantic content shifted' if drift \> threshold else 'stable'}

**In the real world:**  *The embedding drift threshold of 0.15 cosine distance is a conservative starting point. Arize AI's production data shows that seasonal content drift (e.g., a news classifier in December vs July) typically produces drift scores of 0.08–0.12. Model-breaking drift (e.g., a domain shift from English to French content) produces scores above 0.25. Tune your threshold based on at least 10 baseline runs before enabling automated alerts.*

### **Extension 2: Prompt Input Schema Validation**

from jsonschema import validate, ValidationError

WEEK3\_PROMPT\_SCHEMA \= {  
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

def validate\_prompt\_inputs(records, schema, quarantine\_path='outputs/quarantine/'):  
    valid, quarantined \= \[\], \[\]  
    for r in records:  
        try:  
            validate(instance=r, schema=schema)  
            valid.append(r)  
        except ValidationError as e:  
            quarantined.append({'record': r, 'error': e.message, 'path': list(e.path)})  
    if quarantined:  
        Path(quarantine\_path).mkdir(parents=True, exist\_ok=True)  
        with open(quarantine\_path \+ 'quarantine.jsonl', 'a') as f:  
            for q in quarantined: f.write(json.dumps(q) \+ '\\n')  
    return {'valid': len(valid), 'quarantined': len(quarantined), 'records': valid}

**In the real world:**  *Anthropic's tool use (function calling) with input\_schema defined enforces this check at the model level — the API rejects structurally malformed tool inputs. But semantic validation (is this doc\_id a real document in our system?) still requires your own check. The quarantine pattern is critical: silent drops corrupt your evaluation metrics. Every quarantined record must be traceable.*

### **Extension 3: LLM Output Schema Violation Rate**

def check\_output\_violation\_rate(outputs, expected\_enum\_field, expected\_values,  
                               baseline\_rate=None, warn\_threshold=0.02):  
    total \= len(outputs)  
    violations \= sum(1 for o in outputs if o.get(expected\_enum\_field) not in expected\_values)  
    rate \= violations / max(total, 1\)  
    trend \= 'unknown'  
    if baseline\_rate is not None:  
        if rate \> baseline\_rate \* 1.5: trend \= 'rising'  
        elif rate \< baseline\_rate \* 0.5: trend \= 'falling'  
        else: trend \= 'stable'  
    return {  
        'total\_outputs': total,  
        'schema\_violations': violations,  
        'violation\_rate': round(rate, 4),  
        'trend': trend,  
        'status': 'WARN' if (trend \== 'rising' or rate \> warn\_threshold) else 'PASS',  
        'baseline\_rate': baseline\_rate  
    }

**In the real world:**  *Tracking violation rate per prompt version (not just overall) is what makes this metric actionable. If the rate rises after a prompt change, the prompt is likely the cause. If it rises without a prompt change, suspect a model update from the provider. OpenAI and Anthropic both roll out model updates without advance notice to API users — a rising violation rate with no code change is frequently the first signal of a silent model update.*

# **Day 3 — Hours 48–72**

Target: full system integrated, Enforcer Report generated, README complete, Saturday submission ready.

**Hours 48–58: Integration — Run Full Pipeline**

\# Step 1: Generate contracts (two systems minimum)  
python contracts/generator.py \--source outputs/week3/extractions.jsonl \\  
  \--contract-id week3-document-refinery-extractions \\  
  \--lineage outputs/week4/lineage\_snapshots.jsonl \\  
  \--registry contract\_registry/subscriptions.yaml \--output generated\_contracts/

python contracts/generator.py \--source outputs/week5/events.jsonl \\  
  \--contract-id week5-event-records \\  
  \--lineage outputs/week4/lineage\_snapshots.jsonl \\  
  \--registry contract\_registry/subscriptions.yaml \--output generated\_contracts/

\# Step 2: Baseline validation on clean data  
python contracts/runner.py \--contract generated\_contracts/week3\_extractions.yaml \\  
  \--data outputs/week3/extractions.jsonl \--mode AUDIT \--output validation\_reports/clean.json

\# Step 3: Inject violation and validate again  
python create\_violation.py  
python contracts/runner.py \--contract generated\_contracts/week3\_extractions.yaml \\  
  \--data outputs/week3/extractions\_violated.jsonl \--mode ENFORCE \\  
  \--output validation\_reports/violated.json

\# Step 4: Attribute the violation  
python contracts/attributor.py \\  
  \--violation validation\_reports/violated.json \\  
  \--lineage outputs/week4/lineage\_snapshots.jsonl \\  
  \--registry contract\_registry/subscriptions.yaml \\  
  \--output violation\_log/violations.jsonl

\# Step 5: Run schema evolution analysis (two snapshots needed)  
python contracts/schema\_analyzer.py \\  
  \--contract-id week3-document-refinery-extractions \\  
  \--output validation\_reports/schema\_evolution.json

\# Step 6: Run AI extensions  
python contracts/ai\_extensions.py \\  
  \--extractions outputs/week3/extractions.jsonl \\  
  \--verdicts outputs/week2/verdicts.jsonl \\  
  \--output validation\_reports/ai\_extensions.json

**⚠  If schema\_analyzer finds no diff, you have only one snapshot. Run the generator again on the violated data — it produces a second snapshot with different statistics. The diff between the two will show the confidence range change as a BREAKING classification.**

**Hours 58–64: Generate the Enforcer Report**

The Enforcer Report must be generated programmatically. Do not write it by hand.

\# contracts/report\_generator.py  
import json, glob, yaml  
from pathlib import Path  
from datetime import datetime, timedelta

SEVERITY\_DEDUCTIONS \= {'CRITICAL': 20, 'HIGH': 10, 'MEDIUM': 5, 'LOW': 1}

def load\_all\_reports(reports\_dir='validation\_reports/'):  
    reports \= \[\]  
    for p in glob.glob(f'{reports\_dir}\*.json'):  
        with open(p) as f:  
            try: reports.append(json.load(f))  
            except: pass  
    return reports

def compute\_health\_score(reports):  
    all\_fails \= \[r for rep in reports for r in rep.get('results', \[\])  
                 if r.get('status') in ('FAIL', 'ERROR')\]  
    score \= 100  
    for f in all\_fails:  
        score \-= SEVERITY\_DEDUCTIONS.get(f.get('severity', 'LOW'), 1\)  
    return max(0, min(100, score)), all\_fails

def plain\_language(result, registry\_path='contract\_registry/subscriptions.yaml'):  
    with open(registry\_path) as f: reg \= yaml.safe\_load(f)  
    contract\_id \= result.get('check\_id', '').split('.')\[0\]  
    subs \= \[s\['subscriber\_id'\] for s in reg.get('subscriptions', \[\])  
            if s\['contract\_id'\] \== contract\_id\]  
    sub\_str \= ', '.join(subs) if subs else 'no registered subscribers'  
    return (f"The '{result\['column\_name'\]}' field failed its {result\['check\_type'\]} check. "  
            f"Expected {result\['expected'\]}, found {result\['actual\_value'\]}. "  
            f"Downstream subscribers affected: {sub\_str}. "  
            f"Records failing: {result.get('records\_failing', 'unknown')}.")

def generate\_report():  
    reports \= load\_all\_reports()  
    violations \= \[\]  
    vpath \= Path('violation\_log/violations.jsonl')  
    if vpath.exists():  
        with open(vpath) as f:  
            violations \= \[json.loads(l) for l in f if l.strip()\]  
    score, all\_fails \= compute\_health\_score(reports)  
    top3 \= sorted(all\_fails, key=lambda x: \['CRITICAL','HIGH','MEDIUM','LOW'\].index(x.get('severity','LOW')))\[:3\]  
    ai \= {}  
    ai\_path \= Path('validation\_reports/ai\_extensions.json')  
    if ai\_path.exists():  
        with open(ai\_path) as f: ai \= json.load(f)  
    return {  
        'generated\_at': datetime.utcnow().isoformat(),  
        'period': f'{(datetime.utcnow()-timedelta(days=7)).date()} to {datetime.utcnow().date()}',  
        'data\_health\_score': score,  
        'health\_narrative': f'Score {score}/100. ' \+ (  
            'All systems operating within contract parameters.' if score \>= 90 else  
            f'{len(\[v for v in all\_fails if v.get("severity")=="CRITICAL"\])} critical issues require immediate action.'),  
        'top\_violations': \[plain\_language(v) for v in top3\],  
        'violations\_by\_severity': {'CRITICAL': len(\[v for v in all\_fails if v.get('severity')=='CRITICAL'\]),  
                                   'HIGH': len(\[v for v in all\_fails if v.get('severity')=='HIGH'\]),  
                                   'MEDIUM': len(\[v for v in all\_fails if v.get('severity')=='MEDIUM'\])},  
        'ai\_risk': {'embedding\_drift': ai.get('embedding\_drift',{}).get('drift\_score','N/A'),  
                    'output\_violation\_rate': ai.get('output\_violation\_rate',{}).get('violation\_rate','N/A'),  
                    'status': ai.get('output\_violation\_rate',{}).get('status','UNKNOWN')},  
        'recommendations': \[  
            f'Update the confidence field in src/week3/extractor.py to output float 0.0-1.0 per contract week3-document-refinery-extractions',  
            'Add contracts/runner.py as a required CI step before any Week 3 deployment',  
            'Schedule monthly baseline refresh for statistical drift thresholds'  
        \]  
    }

**⚠  The Enforcer Report must reference real numbers from your validation runs. If data\_health\_score is always 100 or recommendations are generic, evaluators will re-run your system and compare. The numbers must match.**

**Hours 64–70: README and Repository Final Check**

Write README.md as a recipe card for evaluators. Format: numbered steps, each with the exact command, the expected output, and how to verify success.

\#\# Data Contract Enforcer — Running the System

\#\#\# Prerequisites  
pip install \-r requirements.txt  
\# Requires: outputs/week3/extractions.jsonl (\>=50 records)  
\# Requires: outputs/week4/lineage\_snapshots.jsonl  
\# Requires: outputs/week5/events.jsonl (\>=50 records)

\#\#\# Step 1: Bootstrap registry  
\# Edit contract\_registry/subscriptions.yaml — already committed  
\# Verify: cat contract\_registry/subscriptions.yaml | grep subscriber\_id  
\# Expected: at least 4 subscriber entries

\#\#\# Step 2: Generate contracts  
python contracts/generator.py \\  
  \--source outputs/week3/extractions.jsonl \\  
  \--contract-id week3-document-refinery-extractions \\  
  \--lineage outputs/week4/lineage\_snapshots.jsonl \\  
  \--registry contract\_registry/subscriptions.yaml \--output generated\_contracts/  
\# Expected: generated\_contracts/week3\_extractions.yaml (\>=8 clauses)  
\#           generated\_contracts/week3\_extractions\_dbt.yml  
\#           schema\_snapshots/week3-document-refinery-extractions/\<timestamp\>.yaml

\#\#\# Step 3: Validate clean data (establishes baseline)  
python contracts/runner.py \\  
  \--contract generated\_contracts/week3\_extractions.yaml \\  
  \--data outputs/week3/extractions.jsonl \--mode AUDIT \\  
  \--output validation\_reports/clean.json  
\# Expected: all structural checks PASS, baselines written

Continue through all steps. End with: 'Open enforcer\_report/report\_data.json. Verify data\_health\_score is between 0 and 100 and recommendations reference real file paths from this repository.'

Run through your README yourself on a fresh clone of your own repository. If any step fails, fix it before submitting. Evaluators follow the README exactly.

# **Client Engagement Playbook**

The 72-hour deployment sequence below assumes you have repository access and at least one stakeholder who can answer questions about the data. Every step includes what you say to the client — not just what you do.

## **Hour 0–4: Discovery — What Do You Say?**

Do not open a code editor. Ask these questions and document every answer. The answers determine your contract priority order.

4. 'What are your primary data sources that feed your AI system?' — List every database table, S3 path, Kafka topic, and API endpoint.

5. 'Which data failures have you experienced in the past 6 months?' — Any wrong output, silent failure, or incident that turned out to be a data issue. Ask for the incident postmortems if they exist.

6. 'Who owns each data source?' — Name and team. This becomes the contact field in the registry.

7. 'How often does the schema change? Is there a review process?' — If the answer is 'anyone can change it anytime', that is your risk finding.

8. 'What is the business cost of a silent data failure in your AI system?' — This answer tells you which validation mode to start in (AUDIT vs ENFORCE) and justifies the deployment cost.

The answers to Questions 2 and 5 determine your contract priority order. Start with the interfaces that have already caused failures and where the cost of failure is highest.

**In the real world:**  *The most common answer to Question 3 ('who owns this data?') is 'I think it's the data engineering team, but I'm not sure.' This is itself a finding — data with unclear ownership has no one to notify when a schema changes. The first entry in the registry for such a dataset should have contact: triage@org.com as a placeholder, flagged for resolution. Undocumented ownership is a data governance risk independent of schema quality.*

## **Which Trust Tier Is This Client?**

Before generating any contracts, determine which tier you are operating in. Your answer changes which blast radius method you use.

| QUESTION TO ASK | TIER 1 ANSWER | TIER 2 OR 3 ANSWER |
| :---- | :---- | :---- |
| Do you own all the systems that consume this data? | Yes — internal systems only. We built them all. | Some are third-party or from another business unit. |
| Can you see the lineage graph of all consuming systems? | Yes — same repo, same team. | No — different teams, different repos, or external companies. |
| What blast radius method should you use? | Registry (primary) \+ lineage graph traversal (enrichment). This project's full model. | Registry only. Lineage traversal limited to your own systems. External consumers compute their own blast radius independently. |
| What tooling upgrade should you recommend? | subscriptions.yaml is sufficient. Upgrade to DataHub when team count \> 10\. | DataHub or OpenMetadata immediately. Manual YAML will not scale across teams. Propose Pact for any cross-company API boundary. |

## **Hour 4–8: Schema Extraction and Registry Bootstrap**

Ask for read access to the three highest-risk data sources identified in discovery. Run the ContractGenerator immediately. Show the output to the client in real time.

What you say: 'I'm going to generate a contract from your existing data. For each field, I'll show you what I inferred. Tell me anything I got wrong — those corrections are the most valuable part of this session.' The clauses the client pushes back on or expands represent tribal knowledge being formalised for the first time. Every correction is a risk mitigation.

After generating contracts, ask the client to name two or three teams that consume this data. Add them to subscriptions.yaml with the client's help. Ask each consumer team: 'If this field changed type or was renamed, what would break in your system?' The answers become breaking\_fields.

**In the real world:**  *A common client objection at this stage: 'We already have dbt tests for this.' The correct response: 'dbt tests enforce structural constraints within your dbt project. They do not compute blast radius across teams, they do not do statistical drift detection, and they do not catch AI-specific failures like embedding drift or prompt input schema violations. We are complementing your dbt tests, not replacing them.' Then show them the AI extensions — this is always the moment the conversation changes.*

## **Hour 8–24: First Validation Run — Find Something Real**

Run the ValidationRunner against a historical data snapshot — not the live production system. A historical run lets you find violations that already exist before you deploy. Finding a violation that already exists is far more compelling than demonstrating a hypothetical future one.

When you find a violation (and you will), present it in this exact structure:

* The check that failed: \[check\_id in plain English\]

* The data that failed it: \[sample records\]

* Who is affected: \[registry subscriber list\]

* What would have happened if this reached production: \[consequence in business terms\]

* When this was introduced: \[blame chain with git commit, if available\]

This five-part structure is what converts a technical finding into a business justification. The client does not care about z-scores. They care that this specific data issue would have caused this specific wrong output for this many records, and that the commit that caused it was made three weeks ago by this person.

## **Hour 24–48: Deploy and Integrate**

Integrate the ValidationRunner as a pre-pipeline step. The integration point depends on the client's stack:

* **Airflow:** Add a Python operator before the first data-consuming task. The operator runs the ValidationRunner and sets the DAG's pipeline\_action based on the report. BLOCK → fail the DAG. QUARANTINE → pass with warning and annotate downstream records.

* **dbt:** The generated dbt schema.yml can be used directly. Run dbt test before dbt run. Contract violations become test failures. Note: dbt test only catches structural contracts — statistical drift detection requires a separate step.

* **Prefect / Dagster:** Add a contract validation task that runs before any LLM-consuming task. Use the structured violation JSON to populate run metadata — this gives you violation history in your orchestration UI.

* **No orchestration (script-based):** Add contracts/runner.py as the first import in the main pipeline script. Raise an exception on pipeline\_action \== BLOCK. This is the minimum viable integration.

**In the real world:**  *Start in AUDIT mode for at least two weeks before switching to ENFORCE. AUDIT mode is safe — it logs everything and blocks nothing. ENFORCE mode will block your pipeline if a contract has a false positive. False positives in new contracts are common. The calibration period between AUDIT and ENFORCE is not optional — it is the step that determines whether the system is trusted or disabled.*

## **Hour 48–72: Stakeholder Report and Handoff**

Generate the Enforcer Report from the first 48 hours of validation data. Hand it to the client lead before leaving the first sprint.

The most important number in the report is the Data Health Score. If the client says 'that seems too low' — calibrate the severity weights and regenerate. The score is a communication tool, not a mathematical truth. The goal is a number that the client's team will rally around improving.

The Recommended Actions section is the most actionable part of the report. Each action must be specific enough that an engineer can open a ticket for it without asking a follow-up question. If you write 'fix the schema', you have not written a recommendation — you have written a complaint. Write 'update src/pipeline/extractor.py line 47 to output confidence as float 0.0–1.0. This fixes the CRITICAL violation in contract week3-document-refinery-extractions clause extracted\_facts.confidence.range.' That is a recommendation.

# **Common Failures and How to Fix Them**

| SYMPTOM | LIKELY CAUSE | FIX |
| :---- | :---- | :---- |
| runner.py crashes with KeyError | Contract YAML keys don't match DataFrame columns after flattening. Nested fields need the array\_key\_ prefix. | Print df.columns after flatten\_records(). Compare to contract schema keys. Add array\_key\_ prefix to nested field names in the contract. |
| Blast radius is always empty | ViolationAttributor is querying registry with the wrong contract\_id or field path format. | Print contract\_id and failing\_field before the registry query. Verify they match exactly what is in subscriptions.yaml — including dot notation for nested fields (extracted\_facts.confidence, not extracted\_facts\[\*\].confidence). |
| Statistical drift not firing on violated data | Baseline was not written after first run, OR the violated data was used as the first run (establishing a bad baseline). | Delete schema\_snapshots/baselines.json. Run runner on clean data first to write a correct baseline. Then run on violated data. The drift check compares current mean to the clean baseline. |
| schema\_analyzer finds no diff | Only one snapshot exists — generator was run only once. | Run generator again on the violated data: python contracts/generator.py \--source outputs/week3/extractions\_violated.jsonl. This produces a second snapshot with different statistics. The analyzer diffs the two. |
| git log returns empty in attributor | The cwd for subprocess.run is the enforcer repo, not the producer repo. git sees no commits. | Pass cwd=Path('/path/to/week3/repo') to subprocess.run(). Or use GitPython: repo \= Repo('/path/to/week3/repo'); repo.git.log('--follow', ..., file\_path). |
| Embedding drift is 0.0 on every run | baseline\_path check is not working — baseline is being overwritten on every run instead of being read. | Add a print statement: print(f'Baseline exists: {Path(baseline\_path).exists()}'). If False every time, the path is wrong. Check that baseline\_path is an absolute path or relative to the working directory, not the script directory. |
| Enforcer Report health score is always 100 | load\_all\_reports() is not finding the violated run report, or the violated run was not written to the reports directory. | Print the list of report files: glob.glob('validation\_reports/\*.json'). Verify violated.json is in the list. Verify it contains results with status FAIL. If not, the runner is not writing FAIL entries — add explicit fail appends to the check functions. |

# **Submission Checklists**

## **Wednesday Checklist  (20:00 UTC)**

|  | ITEM | WHERE |
| :---- | :---- | :---- |
| \[ \] | GitHub link submitted — repo is public or evaluator added as collaborator | Submission form |
| \[ \] | Google Drive PDF link submitted — opens without login | Submission form |
| \[ \] | DOMAIN\_NOTES.md — all 5 questions answered. Q3 includes trust boundary sequence diagram. Q5 identifies process failure, not technical failure. | GitHub root |
| \[ \] | contract\_registry/subscriptions.yaml — minimum 4 subscriptions, breaking\_fields populated for each | GitHub |
| \[ \] | generated\_contracts/week3\_extractions.yaml — min 8 clauses, Bitol-compatible, confidence field has minimum/maximum 0.0/1.0 | GitHub |
| \[ \] | generated\_contracts/week5\_events.yaml — min 6 clauses | GitHub |
| \[ \] | contracts/generator.py — evaluator runs it; produces YAML \+ snapshot without errors | GitHub |
| \[ \] | contracts/runner.py — evaluator runs it; produces validation report JSON with correct schema | GitHub |
| \[ \] | validation\_reports/ — at least one real validation report (not fabricated). Includes baselines.json. | GitHub |
| \[ \] | PDF: data flow diagram with 6 systems, arrows annotated with schema names and breaking fields | Google Drive |
| \[ \] | PDF: contract coverage table for all inter-system interfaces | Google Drive |
| \[ \] | PDF: registry snapshot — full subscriptions.yaml content with rationale for breaking\_fields choices | Google Drive |
| \[ \] | PDF: first validation run results — real numbers from real data | Google Drive |
| \[ \] | PDF: reflection — what assumption about your own systems turned out to be wrong | Google Drive |

## **Saturday Checklist  (20:00 UTC)**

|  | ITEM | WHERE |
| :---- | :---- | :---- |
| \[ \] | All Wednesday items present and up to date | GitHub |
| \[ \] | contracts/attributor.py — uses registry as primary blast radius source; produces violation log entry with blame chain | GitHub |
| \[ \] | contracts/schema\_analyzer.py — diffs two snapshots; classifies the confidence scale change as BREAKING | GitHub |
| \[ \] | contracts/ai\_extensions.py — all three checks; embedding drift runs on real text; output schema runs on real verdicts | GitHub |
| \[ \] | violation\_log/violations.jsonl — min 3 entries. At least 1 real (found in your data). At least 1 injected with injection\_note: true. | GitHub |
| \[ \] | schema\_snapshots/ — min 2 timestamped snapshots per contract demonstrating evolution tracking | GitHub |
| \[ \] | enforcer\_report/report\_data.json — machine-generated. data\_health\_score 0–100. Recommendations reference real file paths. | GitHub |
| \[ \] | README.md — evaluator can reproduce all steps on fresh clone. Expected output shown for each command. | GitHub root |
| \[ \] | PDF: Enforcer Report embedded or linked — auto-generated label present | Google Drive |
| \[ \] | PDF: violation deep-dive — registry blast radius query result shown, blame chain traced to git commit | Google Drive |
| \[ \] | PDF: AI extension results — real numbers for drift score and violation rate with trends | Google Drive |
| \[ \] | PDF: schema evolution case study — diff, compatibility verdict, migration checklist, rollback plan | Google Drive |
| \[ \] | PDF: trust boundary reflection — what changes when you deploy this on a client you do not fully control? Which real tool would you recommend to replace subscriptions.yaml for a 50-team org? | Google Drive |

