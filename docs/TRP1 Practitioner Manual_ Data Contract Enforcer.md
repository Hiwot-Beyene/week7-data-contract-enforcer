Data Contract Enforcer

Practitioner Manual

*A step-by-step field guide from first commit to client-ready report.*

*Use this manual alongside the project document. The project document defines **what** to build. This manual tells you **how** to build it, in what order, with what commands, and how to know when each step is done.*

# Prerequisites — Before Hour 0

Complete these before the week begins. If any prerequisite is missing, you will lose hours on Day 1 resolving environment issues instead of building the system.

## **Environment**

* **Python 3.11+:** Run python \--version. If below 3.11, install via pyenv.

* **Node.js 18+ (optional, only for dbt output validation):** Run node \--version.

* **Required Python packages:** Install with: pip install ydata-profiling pandas numpy scikit-learn jsonschema pyyaml openai anthropic langsmith gitpython soda-core

* **LangSmith account:** You should already have this from integrating LangSmith into your Week 3–5 agents. Export at least 50 traces to outputs/traces/runs.jsonl before starting.

* **Git:** All five prior week repositories must be cloned locally. The ViolationAttributor runs git commands on them directly.

## **Data Readiness Check**

Before writing any code, verify that your prior-week outputs are in the correct location and format. Run these checks manually:

\# Check each output file exists and has content  
wc \-l outputs/week1/intent\_records.jsonl   \# expect \>= 10 lines  
wc \-l outputs/week3/extractions.jsonl       \# expect \>= 50 lines  
wc \-l outputs/week4/lineage\_snapshots.jsonl \# expect \>= 1 line  
wc \-l outputs/week5/events.jsonl            \# expect \>= 50 lines  
wc \-l outputs/traces/runs.jsonl             \# expect \>= 50 lines

If any file has fewer records than expected, run the corresponding prior-week system on additional inputs before proceeding. The contract enforcement is only meaningful if the data is real.

\# Quick schema sanity check — run this Python snippet  
import json, sys  
for path in \['outputs/week3/extractions.jsonl', 'outputs/week5/events.jsonl'\]:  
    with open(path) as f:  
        first \= json.loads(f.readline())  
    print(f'{path}: keys \= {list(first.keys())}')  
\# Compare the printed keys against the canonical schemas in the project document

**⚠  If the keys in your actual files differ from the canonical schemas in the project document, document every deviation in DOMAIN\_NOTES.md and write a migration script in outputs/migrate/ before proceeding. Do not silently redefine the contract to match broken data.**

# **Day 1 — Hours 0–24  (Monday / Tuesday)**

Day 1 target: DOMAIN\_NOTES.md complete, ContractGenerator running, first contracts generated from real data, first ValidationRunner report produced.

**Hours 0–2: Draw the Data Flow Diagram**

Open a blank document (Miro, Excalidraw, or paper — format does not matter). Draw one box per system:

* Week 1 Intent Correlator

* Week 2 Digital Courtroom

* Week 3 Document Refinery

* Week 4 Brownfield Cartographer

* Week 5 Event Sourcing Platform

* LangSmith (external, but it consumes your agents)

Draw an arrow for every data dependency. For each arrow, write:

1. The file path of the data being transferred (e.g. outputs/week3/extractions.jsonl)

2. The top-level keys of the schema (e.g. {doc\_id, extracted\_facts\[\], entities\[\]})

3. Whether this interface has ever caused a failure, even a small one (yes/no)

This diagram is required in your Thursday report. It also directly drives the contract priority order in Hours 2–8. Start with the two arrows that have caused failures or that have the most downstream consumers.

**⚠  Do not skip this step to start coding faster. The diagram is the engineering work. The code is the implementation of decisions already made.**

**Hours 2–4: Write DOMAIN\_NOTES.md**

Open DOMAIN\_NOTES.md in your editor. Answer all five Phase 0 questions from the project document. The answers must be specific to your own systems — general answers score 2/5 on the rubric, system-specific answers with correct Bitol YAML score 5/5.

For Question 2 (the confidence scale change), work through this exact example with your own data:

\# Step 1: find the current confidence distribution in your extractions  
import json, statistics  
with open('outputs/week3/extractions.jsonl') as f:  
    facts \= \[json.loads(l) for l in f\]  
confs \= \[f2\['confidence'\] for f in facts for f2 in f.get('extracted\_facts', \[\])\]  
print(f'min={min(confs):.3f} max={max(confs):.3f} mean={statistics.mean(confs):.3f}')  
\# If max \> 1.0, your data already has the scale problem. Document it.

Your DOMAIN\_NOTES.md must include the actual output of this script. This demonstrates the difference between asserting a fact and measuring it.

**Hours 4–10: Build ContractGenerator**

Create contracts/generator.py. The generator must be callable from the command line as:

python contracts/generator.py \\  
  \--source outputs/week3/extractions.jsonl \\  
  \--contract-id week3-document-refinery-extractions \\  
  \--lineage outputs/week4/lineage\_snapshots.jsonl \\  
  \--output generated\_contracts/

Implement the generator in four stages. Do not skip stages — each one builds on the previous.

### **Stage 1 (1 hour): Load and profile the data**

\# contracts/generator.py — Stage 1  
import json, pandas as pd, yaml, uuid, hashlib  
from pathlib import Path

def load\_jsonl(path):  
    with open(path) as f:  
        return \[json.loads(l) for l in f if l.strip()\]

def flatten\_for\_profile(records):  
    """Flatten nested JSONL to a flat DataFrame for profiling.  
    For arrays like extracted\_facts\[\], explode to one row per item."""  
    rows \= \[\]  
    for r in records:  
        base \= {k: v for k, v in r.items() if not isinstance(v, (list, dict))}  
        for fact in r.get('extracted\_facts', \[{}\]):  
            rows.append({\*\*base, \*\*{f'fact\_{k}': v for k, v in fact.items()}})  
    return pd.DataFrame(rows)

records \= load\_jsonl('outputs/week3/extractions.jsonl')  
df \= flatten\_for\_profile(records)  
print(df.describe())           \# check numeric ranges  
print(df.dtypes)               \# check inferred types

**⚠  If fact\_confidence appears as dtype object (not float64), your data has mixed types. Document this as a contract violation before generating the contract.**

### **Stage 2 (1.5 hours): Structural profiling per column**

def profile\_column(series, col\_name):  
    result \= {  
        'name': col\_name,  
        'dtype': str(series.dtype),  
        'null\_fraction': float(series.isna().mean()),  
        'cardinality\_estimate': int(series.nunique()),  
        'sample\_values': \[str(v) for v in series.dropna().unique()\[:5\]\],  
    }  
    if pd.api.types.is\_numeric\_dtype(series):  
        result\['stats'\] \= {  
            'min': float(series.min()), 'max': float(series.max()),  
            'mean': float(series.mean()), 'p25': float(series.quantile(0.25)),  
            'p50': float(series.quantile(0.50)), 'p75': float(series.quantile(0.75)),  
            'p95': float(series.quantile(0.95)), 'p99': float(series.quantile(0.99)),  
            'stddev': float(series.std())  
        }  
    return result

column\_profiles \= {col: profile\_column(df\[col\], col) for col in df.columns}

### **Stage 3 (1.5 hours): Translate profiles to Bitol YAML clauses**

For each column profile, generate a contract clause. The mapping rules:

* **null\_fraction \== 0.0 → required: true**

* **dtype \== float64 AND column name contains 'confidence' → minimum: 0.0, maximum: 1.0, type: number**

* **cardinality\_estimate \<= 10 AND dtype \== object → type: string, enum: \[list of sample values\]**  — but only if sample\_values covers the full cardinality

* **column name ends with '\_id' → format: uuid (add pattern: ^\[0-9a-f-\]{36}$)**

* **column name ends with '\_at' → format: date-time**

  def column\_to\_clause(profile):

      clause \= {'type': infer\_type(profile\['dtype'\]), 'required': profile\['null\_fraction'\] \== 0.0}

      if 'confidence' in profile\['name'\] and clause\['type'\] \== 'number':

          clause\['minimum'\] \= 0.0

          clause\['maximum'\] \= 1.0

          clause\['description'\] \= 'Confidence score. Must remain 0.0-1.0 float. BREAKING if changed to 0-100.'

      if profile\['name'\].endswith('\_id'):

          clause\['format'\] \= 'uuid'

      if profile\['name'\].endswith('\_at'):

          clause\['format'\] \= 'date-time'

      return clause


  def infer\_type(dtype\_str):

      mapping \= {'float64': 'number', 'int64': 'integer',

                 'bool': 'boolean', 'object': 'string'}

      return mapping.get(dtype\_str, 'string')

### **Stage 4 (1 hour): Inject lineage context and write YAML**

def inject\_lineage(contract, lineage\_path):  
    with open(lineage\_path) as f:  
        snapshot \= json.loads(f.readlines()\[-1\])  \# latest snapshot  
    \# Find nodes that consume week3 output  
    consumers \= \[  
        e\['target'\] for e in snapshot\['edges'\]  
        if 'week3' in e\['source'\] or 'extraction' in e\['source'\]  
    \]  
    contract\['lineage'\] \= {  
        'upstream': \[\],  
        'downstream': \[{'id': c, 'fields\_consumed': \['doc\_id', 'extracted\_facts'\]}  
                       for c in consumers\]  
    }  
    return contract

\# Final write  
contract \= build\_contract(column\_profiles)  
contract \= inject\_lineage(contract, args.lineage)  
output\_path \= Path(args.output) / f'{args.contract\_id}.yaml'  
with open(output\_path, 'w') as f:  
    yaml.dump(contract, f, default\_flow\_style=False, sort\_keys=False)

**⚠  After generating the contract, open it and read it. If you cannot understand a clause without the code that generated it, the clause is not good enough. Rewrite it in plain language.**

**Hours 10–16: Build ValidationRunner**

Create contracts/runner.py. Implement checks in this order: structural first, statistical second. Never crash — always produce a report even on broken input.

### **Structural checks (implement these first)**

* **required field present:** For every field with required: true in the contract, check null\_fraction \== 0.0 in the data. Emit CRITICAL if any nulls found.

* **type match:** For every field with type: number, verify the column is numeric in pandas (df\[col\].dtype is float64 or int64). Emit CRITICAL if mismatch.

* **enum conformance:** For every field with enum: \[...\], verify all non-null values are in the enum list. Report the count and sample of non-conforming values.

* **UUID pattern:** For every field with format: uuid, verify the regex ^\[0-9a-f-\]{36}$ matches all non-null values. Sample 100 if \> 10,000 records.

* **date-time format:** For every field with format: date-time, verify values parse with datetime.fromisoformat(). Report count of unparseable values.

### **Statistical checks (implement after structural)**

* **range check:** For every field with minimum/maximum defined, verify data min \>= contract minimum and data max \<= contract maximum. This is the check that catches the 0.0–1.0 → 0–100 scale change.

* **statistical drift:** Load baselines from schema\_snapshots/baselines.json. For each numeric column with a stored baseline mean and stddev, emit WARN if |current\_mean \- baseline\_mean| \> 2 \* baseline\_stddev, and FAIL if \> 3 \* baseline\_stddev.

  \# Implement the statistical drift check exactly like this

  def check\_statistical\_drift(column, current\_mean, current\_std, baselines):

      if column not in baselines:

          return None  \# no baseline yet; will be written after this run

      b \= baselines\[column\]

      z\_score \= abs(current\_mean \- b\['mean'\]) / max(b\['stddev'\], 1e-9)

      if z\_score \> 3:

          return {'status': 'FAIL', 'z\_score': round(z\_score, 2),

                  'message': f'{column} mean drifted {z\_score:.1f} stddev from baseline'}

      elif z\_score \> 2:

          return {'status': 'WARN', 'z\_score': round(z\_score, 2),

                  'message': f'{column} mean within warning range ({z\_score:.1f} stddev)'}

      return {'status': 'PASS', 'z\_score': round(z\_score, 2)}

### **Write the baselines file after first run**

\# After first successful ValidationRunner run, write baselines  
baselines \= {}  
for col in df.select\_dtypes(include='number').columns:  
    baselines\[col\] \= {'mean': float(df\[col\].mean()), 'stddev': float(df\[col\].std())}  
with open('schema\_snapshots/baselines.json', 'w') as f:  
    json.dump({'written\_at': datetime.utcnow().isoformat(), 'columns': baselines}, f, indent=2)

✓  Once the ValidationRunner produces its first report, you have passed the minimum Thursday threshold for this component. Commit and push before continuing.

**Hours 16–24: DOMAIN\_NOTES.md polish \+ Thursday prep**

Review your DOMAIN\_NOTES.md against the five questions. For each question, confirm you have:

4. A concrete example from your own week 1–5 schemas (not a hypothetical)

5. At minimum one schema name and field name from the canonical schemas in the project document

6. For Question 4 (the LangSmith trace contract), a syntactically valid Bitol YAML snippet

Run the generator and runner one more time on clean data and capture the output to include in your Thursday report:

python contracts/generator.py \\  
  \--source outputs/week3/extractions.jsonl \\  
  \--contract-id week3-document-refinery-extractions \\  
  \--lineage outputs/week4/lineage\_snapshots.jsonl \\  
  \--output generated\_contracts/

python contracts/runner.py \\  
  \--contract generated\_contracts/week3\_extractions.yaml \\  
  \--data outputs/week3/extractions.jsonl \\  
  \--output validation\_reports/thursday\_baseline.json

The validation report JSON is your proof of work for Thursday. Include the file in your repo and reference it in the PDF report.

# **Day 2 — Hours 24–48  (Wednesday / Thursday)**

Day 2 target: ViolationAttributor working on a real or injected violation, SchemaEvolutionAnalyzer producing a classification, AI Contract Extensions producing real numbers.

**Hours 24–30: Inject a Known Violation (then find it)**

Before building the ViolationAttributor, you need a violation to attribute. If your data is clean (no real violations detected in Day 1), inject one deliberately. This is a required exercise — the rubric requires at least one attributed violation.

Choose one of these injection methods:

### **Injection Method A: Scale Change (recommended)**

\# create\_violation.py — run this once to inject a scale violation  
import json, random  
records \= \[\]  
with open('outputs/week3/extractions.jsonl') as f:  
    for line in f:  
        r \= json.loads(line)  
        \# Change confidence from 0.0-1.0 to 0-100 scale  
        for fact in r.get('extracted\_facts', \[\]):  
            fact\['confidence'\] \= round(fact\['confidence'\] \* 100, 1\)  
        records.append(r)  
with open('outputs/week3/extractions\_violated.jsonl', 'w') as f:  
    for r in records:  
        f.write(json.dumps(r) \+ '\\n')  
\# Document the injection  
print('INJECTION: confidence scale changed from 0.0-1.0 to 0-100')

Now run the ValidationRunner against the violated file:

python contracts/runner.py \\  
  \--contract generated\_contracts/week3\_extractions.yaml \\  
  \--data outputs/week3/extractions\_violated.jsonl \\  
  \--output validation\_reports/injected\_violation.json

The runner must produce a FAIL result for the extracted\_facts.confidence.range check. If it does not, your range check is not working. Fix it before continuing.

### **Injection Method B: Enum Violation**

\# Add an invalid entity type to a week3 record  
\# Change one entity's type from 'ORG' to 'INSTITUTION' (not in enum)  
\# Run runner against it — must produce FAIL for entity.type enum check  
\# Document in violation\_log/violations.jsonl with injection\_note: true

**Hours 30–38: Build ViolationAttributor**

Create contracts/attributor.py. It takes a violation from validation\_reports/ and produces a blame chain.

### **Step 1: Lineage Traversal**

\# contracts/attributor.py  
import json, subprocess, re  
from pathlib import Path

def find\_upstream\_files(failing\_column, lineage\_snapshot):  
    """Walk lineage graph to find files that produce the failing column.  
    Start from week3 node, traverse READS/PRODUCES edges upstream."""  
    column\_system \= failing\_column.split('.')\[0\]  \# e.g. 'week3'  
    candidates \= \[\]  
    for node in lineage\_snapshot\['nodes'\]:  
        if column\_system in node\['node\_id'\] and node\['type'\] \== 'FILE':  
            candidates.append(node\['metadata'\]\['path'\])  
    return candidates

### **Step 2: Git Blame Integration**

def get\_recent\_commits(file\_path, days=14):  
    """Run git log on the file and parse structured output."""  
    cmd \= \[  
        'git', 'log', '--follow',  
        f'--since={days} days ago',  
        '--format=%H|%ae|%ai|%s',  
        '--', file\_path  
    \]  
    result \= subprocess.run(cmd, capture\_output=True, text=True)  
    commits \= \[\]  
    for line in result.stdout.strip().split('\\n'):  
        if '|' in line:  
            hash\_, author, ts, msg \= line.split('|', 3\)  
            commits.append({'commit\_hash': hash\_, 'author': author,  
                            'commit\_timestamp': ts.strip(), 'commit\_message': msg})  
    return commits

### **Step 3: Confidence Scoring and Output**

def score\_candidates(commits, violation\_timestamp, lineage\_distance):  
    from datetime import datetime, timezone  
    scored \= \[\]  
    v\_time \= datetime.fromisoformat(violation\_timestamp.replace('Z', '+00:00'))  
    for rank, commit in enumerate(commits\[:5\], start=1):  
        c\_time \= datetime.fromisoformat(commit\['commit\_timestamp'\].replace(' \+', '+').replace(' \-', '-'))  
        days\_diff \= abs((v\_time \- c\_time).days)  
        score \= max(0.0, 1.0 \- (days\_diff \* 0.1) \- (lineage\_distance \* 0.2))  
        scored.append({\*\*commit, 'rank': rank, 'confidence\_score': round(score, 3)})  
    return sorted(scored, key=lambda x: x\['confidence\_score'\], reverse=True)

### **Step 4: Write Violation Log**

Every attributed violation must be written to violation\_log/violations.jsonl in the schema specified in the project document. Include the blast\_radius computed from the lineage graph's downstream\_consumers list from the contract YAML.

\# The blast radius comes from the contract, not from re-traversing the lineage  
def compute\_blast\_radius(contract\_path, violation\_id):  
    with open(contract\_path) as f:  
        contract \= yaml.safe\_load(f)  
    downstream \= contract.get('lineage', {}).get('downstream', \[\])  
    return {  
        'violation\_id': violation\_id,  
        'affected\_nodes': \[d\['id'\] for d in downstream\],  
        'affected\_pipelines': \[d\['id'\] for d in downstream if 'pipeline' in d\['id'\]\],  
        'estimated\_records': None  \# set from runner report records\_failing field  
    }

**Hours 38–44: Build SchemaEvolutionAnalyzer**

Create contracts/schema\_analyzer.py. It diffs two schema snapshots and classifies each change.

### **Schema Snapshot Format**

First, ensure ContractGenerator writes a snapshot on every run:

\# Add to end of generator.py, in the write step  
from datetime import datetime  
snapshot\_dir \= Path('schema\_snapshots') / args.contract\_id  
snapshot\_dir.mkdir(parents=True, exist\_ok=True)  
ts \= datetime.utcnow().strftime('%Y%m%d\_%H%M%S')  
snapshot\_path \= snapshot\_dir / f'{ts}.yaml'  
shutil.copy(output\_path, snapshot\_path)

### **Diff and Classification**

def classify\_change(field\_name, old\_clause, new\_clause):  
    """Classify a schema change using the taxonomy from the project document."""  
    if old\_clause is None:  
        \# New field added  
        if new\_clause.get('required', False):  
            return 'BREAKING', 'Add non-nullable column — coordinate with all producers'  
        return 'COMPATIBLE', 'Add nullable column — consumers can ignore'  
    if new\_clause is None:  
        return 'BREAKING', 'Remove column — deprecation period mandatory'  
    if old\_clause.get('type') \!= new\_clause.get('type'):  
        return 'BREAKING', f'Type change {old\_clause\["type"\]} \-\> {new\_clause\["type"\]}'  
    if old\_clause.get('maximum') \!= new\_clause.get('maximum'):  
        return 'BREAKING', f'Range change maximum {old\_clause.get("maximum")} \-\> {new\_clause.get("maximum")}'  
    if old\_clause.get('enum') \!= new\_clause.get('enum'):  
        added \= set(new\_clause.get('enum',\[\]))-set(old\_clause.get('enum',\[\]))  
        removed \= set(old\_clause.get('enum',\[\]))-set(new\_clause.get('enum',\[\]))  
        if removed: return 'BREAKING', f'Enum values removed: {removed}'  
        return 'COMPATIBLE', f'Enum values added: {added}'  
    return 'COMPATIBLE', 'No material change'

**Hours 44–48: Build AI Contract Extensions**

Create contracts/ai\_extensions.py. Three checks, each testable independently.

### **Embedding Drift Check**

\# pip install anthropic openai numpy scikit-learn  
import numpy as np  
from openai import OpenAI

def embed\_sample(texts, n=200, model='text-embedding-3-small'):  
    sample \= texts\[:n\] if len(texts) \> n else texts  
    client \= OpenAI()  
    resp \= client.embeddings.create(input=sample, model=model)  
    return np.array(\[e.embedding for e in resp.data\])

def check\_embedding\_drift(texts, baseline\_path='schema\_snapshots/embedding\_baselines.npz',  
                          threshold=0.15):  
    current\_vecs \= embed\_sample(texts)  
    current\_centroid \= current\_vecs.mean(axis=0)  
    if not Path(baseline\_path).exists():  
        np.savez(baseline\_path, centroid=current\_centroid)  
        return {'status': 'BASELINE\_SET', 'drift\_score': 0.0}  
    baseline \= np.load(baseline\_path)\['centroid'\]  
    dot \= np.dot(current\_centroid, baseline)  
    norm \= np.linalg.norm(current\_centroid) \* np.linalg.norm(baseline)  
    cosine\_sim \= dot / (norm \+ 1e-9)  
    drift \= 1 \- cosine\_sim  
    return {  
        'status': 'FAIL' if drift \> threshold else 'PASS',  
        'drift\_score': round(float(drift), 4),  
        'threshold': threshold,  
        'interpretation': 'semantic content of text has shifted' if drift \> threshold else 'stable'  
    }

### **Prompt Input Schema Validation**

\# validate every week3 extraction prompt input before it hits the LLM  
from jsonschema import validate, ValidationError  
import json

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

def validate\_prompt\_inputs(records, quarantine\_path='outputs/quarantine/'):  
    valid, quarantined \= \[\], \[\]  
    for r in records:  
        try:  
            validate(instance=r, schema=PROMPT\_INPUT\_SCHEMA)  
            valid.append(r)  
        except ValidationError as e:  
            quarantined.append({'record': r, 'error': e.message})  
    if quarantined:  
        Path(quarantine\_path).mkdir(exist\_ok=True)  
        with open(quarantine\_path \+ 'quarantine.jsonl', 'a') as f:  
            for q in quarantined: f.write(json.dumps(q) \+ '\\n')  
    return valid, quarantined

### **LLM Output Schema Violation Rate**

def check\_output\_schema\_violation\_rate(verdict\_records,  
                                        baseline\_rate=None, warn\_threshold=0.02):  
    total \= len(verdict\_records)  
    violations \= sum(1 for v in verdict\_records  
                     if v.get('overall\_verdict') not in ('PASS', 'FAIL', 'WARN')),  
    rate \= violations / max(total, 1\)  
    trend \= 'unknown'  
    if baseline\_rate is not None:  
        trend \= 'rising' if rate \> baseline\_rate \* 1.5 else 'stable'  
    return {  
        'total\_outputs': total,  
        'schema\_violations': violations,  
        'violation\_rate': round(rate, 4),  
        'trend': trend,  
        'status': 'WARN' if rate \> warn\_threshold else 'PASS'  
    }

# **Day 3 — Hours 48–72  (Friday / Saturday)**

Day 3 target: full system integrated, Enforcer Report generated, README complete, Sunday submission prepared.

**Hours 48–56: Integration — Run Everything End-to-End**

Run the full pipeline in sequence and confirm each step produces its output file. This is the integration test your evaluators will replicate:

\# Step 1: Generate contracts for both required systems  
python contracts/generator.py \--source outputs/week3/extractions.jsonl \\  
  \--contract-id week3-document-refinery-extractions \\  
  \--lineage outputs/week4/lineage\_snapshots.jsonl \--output generated\_contracts/

python contracts/generator.py \--source outputs/week5/events.jsonl \\  
  \--contract-id week5-event-records \\  
  \--lineage outputs/week4/lineage\_snapshots.jsonl \--output generated\_contracts/

\# Step 2: Run validation on clean data (establish baselines)  
python contracts/runner.py \--contract generated\_contracts/week3\_extractions.yaml \\  
  \--data outputs/week3/extractions.jsonl \--output validation\_reports/clean\_run.json

\# Step 3: Inject violation and run again  
python create\_violation.py  \# produces outputs/week3/extractions\_violated.jsonl  
python contracts/runner.py \--contract generated\_contracts/week3\_extractions.yaml \\  
  \--data outputs/week3/extractions\_violated.jsonl \--output validation\_reports/violated\_run.json

\# Step 4: Attribute the violation  
python contracts/attributor.py \\  
  \--violation validation\_reports/violated\_run.json \\  
  \--lineage outputs/week4/lineage\_snapshots.jsonl \\  
  \--contract generated\_contracts/week3\_extractions.yaml \\  
  \--output violation\_log/violations.jsonl

\# Step 5: Run schema evolution analysis  
python contracts/schema\_analyzer.py \\  
  \--contract-id week3-document-refinery-extractions \\  
  \--output validation\_reports/schema\_evolution.json

If any step fails, fix it before continuing. Do not generate the Enforcer Report from partial data.

**⚠  Common failure at this stage: the schema\_analyzer finds no snapshots to diff because the generator was only run once. Run the generator again after the violation injection — the two runs produce two different snapshots to diff.**

**Hours 56–64: Generate the Enforcer Report**

The Enforcer Report must be generated programmatically from the files in violation\_log/ and validation\_reports/. It must not be written by hand.

\# contracts/report\_generator.py  
import json, yaml, glob  
from pathlib import Path  
from datetime import datetime, timedelta

def compute\_health\_score(validation\_reports):  
    """0-100 score. Start at 100\. Subtract per violation severity."""  
    deductions \= {'CRITICAL': 20, 'HIGH': 10, 'MEDIUM': 5, 'LOW': 1}  
    score \= 100  
    for report in validation\_reports:  
        for result in report.get('results', \[\]):  
            if result\['status'\] in ('FAIL', 'ERROR'):  
                score \-= deductions.get(result.get('severity', 'LOW'), 1\)  
    return max(0, min(100, score))

def plain\_language\_violation(result):  
    return (f"The {result\['column\_name'\]} field in {result\['check\_id'\].split('.')\[0\]} "  
            f"failed its {result\['check\_type'\]} check. "  
            f"Expected {result\['expected'\]} but found {result\['actual\_value'\]}. "  
            f"This affects {result.get('records\_failing', 'unknown')} records.")

def generate\_report(reports\_dir='validation\_reports/', violations\_dir='violation\_log/'):  
    reports \= \[json.load(open(p)) for p in glob.glob(f'{reports\_dir}\*.json')\]  
    violations \= \[\]  
    if Path(f'{violations\_dir}violations.jsonl').exists():  
        with open(f'{violations\_dir}violations.jsonl') as f:  
            violations \= \[json.loads(l) for l in f if l.strip()\]  
    health\_score \= compute\_health\_score(reports)  
    all\_failures \= \[r for rep in reports for r in rep.get('results',\[\])  
                    if r\['status'\] in ('FAIL','ERROR')\]  
    top\_3 \= sorted(all\_failures,  
                   key=lambda x: \['CRITICAL','HIGH','MEDIUM','LOW'\].index(x.get('severity','LOW')))\[:3\]  
    return {  
        'generated\_at': datetime.utcnow().isoformat(),  
        'period': f'{(datetime.utcnow()-timedelta(days=7)).date()} to {datetime.utcnow().date()}',  
        'data\_health\_score': health\_score,  
        'health\_narrative': f'Score of {health\_score}/100. ' \+ (  
            'No critical violations.' if health\_score \>= 90 else  
            f'{len(\[v for v in all\_failures if v.get("severity")=="CRITICAL"\])} critical issues require immediate action.'),  
        'top\_violations': \[plain\_language\_violation(v) for v in top\_3\],  
        'total\_violations\_by\_severity': {  
            sev: len(\[v for v in all\_failures if v.get('severity')==sev\])  
            for sev in \['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'\]},  
        'violation\_count': len(violations),  
        'recommendations': \[  
            'Update src/week3/extractor.py to output confidence as float 0.0-1.0',  
            'Add contract enforcement step to week3 CI pipeline',  
            'Review week5 event schema for enum completeness'  
        \]  
    }

if \_\_name\_\_ \== '\_\_main\_\_':  
    report \= generate\_report()  
    with open('enforcer\_report/report\_data.json', 'w') as f:  
        json.dump(report, f, indent=2)

**⚠  The Enforcer Report must reference real numbers from your validation runs. If you hard-code the health score or violation count, evaluators will notice immediately when they run your system and get different numbers.**

**Hours 64–68: Run AI Contract Extensions on Real Data**

\# Run all three AI extensions and capture output  
python contracts/ai\_extensions.py \\  
  \--mode all \\  
  \--extractions outputs/week3/extractions.jsonl \\  
  \--verdicts outputs/week2/verdicts.jsonl \\  
  \--output validation\_reports/ai\_extensions.json

The AI extensions output must appear in the Enforcer Report under the 'AI system risk assessment' section. If embedding drift is 0.0 (because you only have one run's worth of data), note this explicitly and explain what the baseline represents.

**Hours 68–72: README and Final Checks**

Write README.md as a test script for your evaluators. Format it as a numbered sequence of commands they will run, with the expected output for each:

\#\# How to Run the Data Contract Enforcer

\#\#\# Prerequisites  
pip install \-r requirements.txt

\#\#\# Step 1: Generate contracts  
python contracts/generator.py \--source outputs/week3/extractions.jsonl \\  
  \--contract-id week3-document-refinery-extractions \\  
  \--lineage outputs/week4/lineage\_snapshots.jsonl \--output generated\_contracts/

Expected output: generated\_contracts/week3\_extractions.yaml (min 8 clauses)  
                 generated\_contracts/week3\_extractions\_dbt.yml

\#\#\# Step 2: Run validation (clean data)  
python contracts/runner.py \--contract generated\_contracts/week3\_extractions.yaml \\  
  \--data outputs/week3/extractions.jsonl \--output validation\_reports/clean\_run.json

Expected output: validation\_reports/clean\_run.json — all structural checks PASS

Continue for all five steps. End with: 'After running all steps, open enforcer\_report/report\_data.json and verify data\_health\_score is between 0 and 100.'

Run through your README yourself on a clean clone. If any step fails, fix it. This is the minimum bar — if the evaluator cannot follow your README, the submission cannot be fully evaluated.

# **Common Failures and How to Fix Them**

| SYMPTOM | LIKELY CAUSE | FIX |
| :---- | :---- | :---- |
| runner.py crashes with KeyError on first run | Contract YAML key names don't match the DataFrame column names after flattening | Print df.columns after flatten\_for\_profile(). Update contract field names to match. Prefix nested fields with fact\_ if needed. |
| Embedding drift check returns 0.0 every time | baseline\_path doesn't exist so baseline is always overwritten | Check Path(baseline\_path).exists() before overwriting. If the baseline file is missing, create it on first run and return BASELINE\_SET, not PASS. |
| git log returns empty results in attributor | Working directory is not the prior-week repo; git runs in the wrong directory | Pass cwd= to subprocess.run() pointing at the repo root. Or use GitPython: repo \= Repo('/path/to/week3/repo'). |
| schema\_analyzer finds no diff (single snapshot) | Generator was only run once; no second snapshot to compare | Run create\_violation.py, then run generator again. Now two snapshots exist. The diff should show the confidence range change. |
| Enforcer Report health score is always 100 | ValidationRunner is not writing FAIL results to the report JSON correctly | Print report\['results'\] after a violated run. If no FAIL entries appear, the runner is swallowing errors. Add explicit FAIL entries for every failed assertion. |
| LangSmith export is empty | Runs exported without the correct project name filter | In LangSmith UI: Settings \> Projects \> select your project \> Export. Or via API: langsmith.Client().list\_runs(project\_name='your\_project\_name'). |

# **Client Engagement Playbook**

This section describes how to apply the Data Contract Enforcer in a real client engagement. The 72-hour timeline assumes you have repository access and at least one stakeholder who can answer questions about the data.

## **Hour 0–4: Discovery**

Do not open a code editor. Ask the following questions and document every answer:

7. What are your primary data sources? List every database table, data lake path, and API endpoint that feeds your AI system.

8. Which data failures have you experienced in the past 6 months? Any wrong output, silent failure, or incident that turned out to be a data issue?

9. Who owns each data source? Name and team.

10. How often does the schema change? Is there a schema registry? Who approves schema changes?

11. What is the cost of a silent data failure? (Hour of bad outputs, customer impact, rollback cost.)

The answers to questions 2 and 5 determine your contract priority order. Start with the interfaces that have already caused failures and where the cost of failure is highest.

## **Hour 4–8: Schema Extraction**

Ask for read access to the three highest-risk data sources identified in discovery. Run the ContractGenerator immediately:

python contracts/generator.py \\  
  \--source s3://client-bucket/production/events/ \\  \# or local sample  
  \--contract-id client-production-events \\  
  \--lineage \<path\_to\_cartographer\_output\_if\_available\> \\  
  \--output generated\_contracts/

Show the generated contract to your stakeholder contact in real time. For each clause, ask: 'Is this correct? Is there anything this doesn't capture?' The clauses they push back on or expand are the most valuable — they represent tribal knowledge being formalised for the first time.

## **Hour 8–24: First Validation Run**

Run the ValidationRunner against a historical data snapshot (not the live production system — request a copy). A historical run lets you find violations that already exist before you deploy the system. Finding a violation that already exists is far more compelling to a client than finding a hypothetical future one.

When you find a violation (and you will), present it as:

* Here is the check that failed: \[check\_id in plain language\]

* Here is the data that failed it: \[sample records\]

* Here is which downstream system is affected: \[blast radius\]

* Here is what would have happened if this data reached production: \[consequence in business terms\]

This moment — showing a real violation caught on historical data — is the demonstration that wins the engagement.

## **Hour 24–48: Deploy and Demonstrate**

Integrate the ValidationRunner as a pre-pipeline step. The integration point depends on the client's stack:

* **Airflow:** Add a Python operator before the first data-consuming task. The operator runs the ValidationRunner and fails the DAG if any CRITICAL violations are found.

* **dbt:** Use the generated dbt schema.yml directly. Run dbt test before dbt run. Contract violations become dbt test failures.

* **Prefect / Dagster:** Add a contract validation step as a task that runs before any LLM-consuming step. Use the structured violation output to populate the Prefect/Dagster run metadata.

* **No orchestration:** Add a pre-commit hook or CI pipeline step. Less real-time but still valuable for catching violations before deployment.

## **Hour 48–72: Stakeholder Report**

Generate the Enforcer Report from the first 48 hours of validation data. Hand it to the client lead before you leave the first engagement sprint. The report is the artifact that justifies your fee and that gets you invited back.

The single most important number in the report is the Data Health Score. Spend the time to make sure the score is calibrated correctly — if the client says 'that seems too low' or 'that seems too high,' adjust the severity weights and regenerate. The score is a communication tool, not a mathematical truth.

# **Submission Checklists**

## **Thursday Checklist  (03:00 UTC)**

|  | ITEM | WHERE TO CHECK |
| :---- | :---- | :---- |
| \[ \] | GitHub link submitted — repo is public or evaluator has been added as collaborator | Submission form |
| \[ \] | Google Drive PDF link submitted — link opens without login required | Submission form |
| \[ \] | DOMAIN\_NOTES.md — all 5 questions answered with examples from own systems | GitHub root |
| \[ \] | generated\_contracts/week3\_extractions.yaml — min 8 clauses, Bitol-compatible YAML | GitHub |
| \[ \] | generated\_contracts/week5\_events.yaml — min 6 clauses | GitHub |
| \[ \] | contracts/generator.py — evaluator runs it; produces YAML without errors | GitHub |
| \[ \] | contracts/runner.py — evaluator runs it; produces validation report JSON | GitHub |
| \[ \] | validation\_reports/ — contains at least one real validation report (not fabricated) | GitHub |
| \[ \] | PDF: Data flow diagram with schemas annotated on each arrow | Google Drive PDF |
| \[ \] | PDF: Contract coverage table (all inter-system interfaces listed) | Google Drive PDF |
| \[ \] | PDF: First validation run results — real numbers from real data | Google Drive PDF |
| \[ \] | PDF: Reflection section — what assumption about your own systems turned out to be wrong | Google Drive PDF |

## **Sunday Checklist  (03:00 UTC)**

|  | ITEM | WHERE TO CHECK |
| :---- | :---- | :---- |
| \[ \] | All Thursday items still present and updated if needed | GitHub |
| \[ \] | contracts/attributor.py — evaluator runs it against a violation; produces blame chain JSON | GitHub |
| \[ \] | contracts/schema\_analyzer.py — evaluator diffs two snapshots; produces compatibility verdict | GitHub |
| \[ \] | contracts/ai\_extensions.py — all three checks run on real data | GitHub |
| \[ \] | violation\_log/violations.jsonl — min 3 violations; at least 1 real, at least 1 documented injection | GitHub |
| \[ \] | schema\_snapshots/ — at least 2 snapshots per contract for diff | GitHub |
| \[ \] | enforcer\_report/report\_data.json — auto-generated, data\_health\_score between 0 and 100 | GitHub |
| \[ \] | README.md — step-by-step guide; evaluator can reproduce all steps on fresh clone | GitHub root |
| \[ \] | PDF: Enforcer Report section — auto-generated report embedded or linked | Google Drive PDF |
| \[ \] | PDF: Violation deep-dive — one full blame chain traced from failing check to git commit | Google Drive PDF |
| \[ \] | PDF: AI Contract Extension results — embedding drift score, violation rate, with actual numbers | Google Drive PDF |
| \[ \] | PDF: Schema evolution case study — diff shown, compatibility verdict given, migration checklist | Google Drive PDF |
| \[ \] | PDF: What Would Break Next — highest-risk interface identified with specific reasoning | Google Drive PDF |

