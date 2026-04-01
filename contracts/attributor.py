#!/usr/bin/env python3
"""
ViolationAttributor (Phase 2B) — traces FAIL results to upstream lineage + git history.

Usage:
  python contracts/attributor.py \\
    --report validation_reports/week3_report.json \\
    --contract generated_contracts/week3_extractions.yaml \\
    --lineage outputs/migrate/migrated_lineage_snapshots.jsonl

Or invoked automatically from runner.py when checks fail (unless --no-attributor).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import yaml


def _rel_under_repo(fp: Path, repo: Path) -> Path:
    try:
        return fp.resolve().relative_to(repo.resolve())
    except ValueError:
        return fp


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_lineage_snapshot(path: Path) -> Optional[dict]:
    """Week 4 graph: supports single pretty-printed JSON or one-json-per-line JSONL."""
    if not path.is_file():
        return None
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return None
    if text.startswith("{"):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in reversed(lines):
        try:
            return json.loads(ln)
        except json.JSONDecodeError:
            continue
    return None


def default_lineage_paths(repo_root: Path) -> List[Path]:
    """Prefer Week 4 artifact path from the brief, then migrated compact JSONL."""
    return [
        repo_root / "outputs" / "week4" / "lineage_snapshots.jsonl",
        repo_root / "outputs" / "migrate" / "migrated_lineage_snapshots.jsonl",
    ]


def resolve_lineage_path(explicit: Optional[Path]) -> Optional[Path]:
    if explicit and explicit.is_file():
        return explicit
    for p in default_lineage_paths(REPO_ROOT):
        if p.is_file():
            return p
    return None


_UPSTREAM_EDGE_RELS = frozenset(
    {"READS", "PRODUCES", "WRITES", "IMPORTS", "CALLS", "CONSUMES"}
)


def build_reverse_graph(
    edges: List[dict],
    relationships: Optional[Set[str]] = None,
) -> Dict[str, List[str]]:
    """Edge source -> target; reverse: target -> [sources] (upstream producers)."""
    rel_filter = relationships if relationships is not None else _UPSTREAM_EDGE_RELS
    rev: Dict[str, List[str]] = {}
    for e in edges:
        if not isinstance(e, dict):
            continue
        rel = e.get("relationship")
        if rel not in rel_filter:
            continue
        s, t = e.get("source"), e.get("target")
        if not s or not t:
            continue
        rev.setdefault(str(t), []).append(str(s))
    return rev


def bfs_upstream(
    seeds: List[str],
    reverse_adj: Dict[str, List[str]],
    max_nodes: int = 80,
) -> Tuple[List[str], Dict[str, int]]:
    """Returns visited node ids in BFS order (excluding seeds first), hop depth from seed."""
    seen: Set[str] = set(seeds)
    depth: Dict[str, int] = {s: 0 for s in seeds}
    q: deque[str] = deque(seeds)
    order: List[str] = []
    while q and len(order) < max_nodes:
        cur = q.popleft()
        dcur = depth.get(cur, 0)
        for up in reverse_adj.get(cur, []):
            if up in seen:
                continue
            seen.add(up)
            depth[up] = dcur + 1
            order.append(up)
            q.append(up)
    return order, depth


def node_id_to_repo_path(codebase_root: Path, node_id: str) -> Optional[Path]:
    if "::" not in node_id:
        return None
    kind, rest = node_id.split("::", 1)
    if kind not in ("pipeline", "file"):
        return None
    p = Path(rest)
    if p.is_absolute():
        return p if p.is_file() else None
    cand = codebase_root / p
    return cand if cand.is_file() else None


def git_recent_commits(repo: Path, relpath: Path, since_days: int = 14) -> List[dict]:
    """git log --follow --since=... --format=..."""
    try:
        rel = relpath.resolve().relative_to(repo.resolve())
    except ValueError:
        rel = relpath.name
    fmt = "%H|%an|%ae|%ai|%s"
    cmd = [
        "git",
        "-C",
        str(repo),
        "log",
        "--follow",
        f"--since={since_days} days ago",
        f"--format={fmt}",
        "--",
        str(rel),
    ]
    try:
        out = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except (subprocess.SubprocessError, OSError):
        return []
    if out.returncode != 0 or not out.stdout.strip():
        return []
    rows = []
    for line in out.stdout.strip().splitlines():
        parts = line.split("|", 4)
        if len(parts) < 5:
            continue
        rows.append(
            {
                "commit_hash": parts[0],
                "author_name": parts[1],
                "author_email": parts[2],
                "commit_timestamp": parts[3],
                "commit_message": parts[4],
            }
        )
    return rows


def _rel_str_for_git(repo: Path, file_path: Path) -> str:
    try:
        return str(file_path.resolve().relative_to(repo.resolve()))
    except ValueError:
        return str(file_path.name)


def git_blame_porcelain_commit(
    repo: Path,
    relpath: str,
    line_start: int,
    line_end: int,
) -> Optional[str]:
    """First 40-hex commit from `git blame -L --porcelain` (line-level ownership)."""
    cmd = [
        "git",
        "-C",
        str(repo),
        "blame",
        "-L",
        f"{line_start},{line_end}",
        "--porcelain",
        "--",
        relpath,
    ]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=45, check=False)
    except (subprocess.SubprocessError, OSError):
        return None
    if out.returncode != 0 or not out.stdout:
        return None
    for line in out.stdout.splitlines():
        if len(line) >= 40 and all(c in "0123456789abcdef" for c in line[:40].lower()):
            return line[:40]
    return None


def git_commit_at(repo: Path, rev: str) -> Optional[dict]:
    """Single commit metadata for a hash (when blame hash differs from log --follow tip)."""
    fmt = "%H|%an|%ae|%ai|%s"
    cmd = ["git", "-C", str(repo), "log", "-1", f"--format={fmt}", rev]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False)
    except (subprocess.SubprocessError, OSError):
        return None
    if out.returncode != 0 or not out.stdout.strip():
        return None
    parts = out.stdout.strip().split("|", 4)
    if len(parts) < 5:
        return None
    return {
        "commit_hash": parts[0],
        "author_name": parts[1],
        "author_email": parts[2],
        "commit_timestamp": parts[3],
        "commit_message": parts[4],
    }


def _parse_ts(s: str) -> Optional[datetime]:
    if not s or not isinstance(s, str):
        return None
    try:
        t = s.strip()
        if t.endswith("Z"):
            return datetime.fromisoformat(t.replace("Z", "+00:00"))
        return datetime.fromisoformat(t.replace(" +", "+").replace(" -", "-"))
    except ValueError:
        return None


def confidence_score(detected_at_iso: str, commit_iso: str, hop_depth: int) -> float:
    """base = 1.0 - (days_since_commit * 0.1); then subtract 0.2 per lineage hop (spec)."""
    det = _parse_ts(detected_at_iso) or datetime.now(timezone.utc)
    c_time = _parse_ts(commit_iso)
    if det.tzinfo is None:
        det = det.replace(tzinfo=timezone.utc)
    if c_time is None:
        base = 0.35
    else:
        if c_time.tzinfo is None:
            c_time = c_time.replace(tzinfo=timezone.utc)
        days_since_commit = max(0, (det - c_time).days)
        base = max(0.0, 1.0 - (days_since_commit * 0.1))
    score = base - (hop_depth * 0.2)
    return max(0.0, min(1.0, score))


def collect_seeds_from_contract(contract: dict) -> List[str]:
    down = (contract.get("lineage") or {}).get("downstream") or []
    seeds: List[str] = []
    for d in down:
        if isinstance(d, dict) and d.get("id"):
            seeds.append(str(d["id"]))
    return seeds


def seeds_for_failing_schema_element(
    check_id: str,
    column_name: str,
    nodes: List[dict],
    contract: dict,
) -> List[str]:
    """BFS seeds from the failing schema element: match graph nodes to check/column semantics."""
    text = f"{check_id} {column_name}".lower()
    keywords: List[str] = []
    if any(k in text for k in ("confidence", "extracted_facts", "fact_id", "primary_fact")):
        keywords.extend(["extract", "refinery", "week3", "extraction", "document", "fact"])
    if "doc_id" in text or "source_hash" in text or "source_path" in text:
        keywords.extend(["document", "refinery", "extract", "ingest"])
    if "lineage" in text or "snapshot_id" in text or "git_commit" in text or "edges" in text:
        keywords.extend(["cartograph", "lineage", "migration", "snapshot", "week4"])
    if "event" in text or "sequence" in text or "aggregate" in text:
        keywords.extend(["event", "week5", "sourcing"])
    seeds: List[str] = []
    for n in nodes:
        if not isinstance(n, dict):
            continue
        nid = str(n.get("node_id") or "")
        low = nid.lower()
        if keywords and any(k in low for k in keywords):
            seeds.append(nid)
    seeds = list(dict.fromkeys(seeds))
    if seeds:
        return seeds[:24]
    seeds = collect_seeds_from_contract(contract)
    if seeds:
        return seeds
    for n in nodes:
        if not isinstance(n, dict):
            continue
        nid = str(n.get("node_id") or "")
        if nid.startswith("file::") or "extract" in nid.lower():
            seeds.append(nid)
        if len(seeds) >= 8:
            break
    return list(dict.fromkeys(seeds))[:24]


def collect_seeds_from_lineage_week3(snapshot: Optional[dict]) -> List[str]:
    """FILE / pipeline nodes tied to week3 or extraction when contract seeds are empty."""
    if not snapshot:
        return []
    out: List[str] = []
    for n in snapshot.get("nodes") or []:
        if not isinstance(n, dict):
            continue
        nid = str(n.get("node_id") or "")
        low = nid.lower()
        if "week3" in low or "extraction" in low:
            out.append(nid)
    return list(dict.fromkeys(out))[:12]


def blast_radius_from_contract(contract: dict, estimated_records: int) -> Dict[str, Any]:
    downstream = (contract.get("lineage") or {}).get("downstream") or []
    ids: List[str] = []
    for d in downstream:
        if isinstance(d, dict) and d.get("id"):
            ids.append(str(d["id"]))
    pipelines = [i for i in ids if "pipeline" in i.lower()]
    return {
        "affected_nodes": ids,
        "affected_pipelines": pipelines,
        "estimated_records": estimated_records,
    }


def run_attribution(
    *,
    report_path: Path,
    contract_path: Path,
    lineage_path: Optional[Path],
    violation_log: Path,
) -> None:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    contract = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    resolved_lineage = None
    if lineage_path and lineage_path.is_file():
        resolved_lineage = lineage_path
    else:
        resolved_lineage = resolve_lineage_path(lineage_path)

    snapshot = load_lineage_snapshot(resolved_lineage) if resolved_lineage else None
    fails = [r for r in report.get("results", []) if r.get("status") == "FAIL"]
    if not fails:
        return

    violation_ts = str(report.get("run_timestamp") or utc_now_iso())

    codebase_root: Optional[Path] = None
    rev: Dict[str, List[str]] = {}
    nodes: List[dict] = []
    if snapshot:
        cr = snapshot.get("codebase_root")
        if cr:
            codebase_root = Path(cr)
        rev = build_reverse_graph(snapshot.get("edges") or [])
        nodes = [n for n in (snapshot.get("nodes") or []) if isinstance(n, dict)]

    repo = codebase_root if codebase_root and (codebase_root / ".git").is_dir() else REPO_ROOT
    if not (repo / ".git").is_dir():
        repo = REPO_ROOT

    violation_log.parent.mkdir(parents=True, exist_ok=True)
    line_start, line_end = 1, 40

    for fr in fails:
        check_id = str(fr.get("check_id", ""))
        column_name = str(fr.get("column_name", ""))
        records_failing = int(fr.get("records_failing") or 0)

        seeds = seeds_for_failing_schema_element(check_id, column_name, nodes, contract)
        if not seeds:
            seeds = collect_seeds_from_contract(contract)
        if not seeds:
            seeds = collect_seeds_from_lineage_week3(snapshot)
        if not seeds and snapshot:
            seeds = [
                str(e.get("target"))
                for e in snapshot.get("edges") or []
                if isinstance(e, dict) and e.get("target")
            ]
            seeds = list(dict.fromkeys(seeds))[:5]

        visited, depth_map = bfs_upstream(seeds, rev) if seeds else ([], {})
        file_candidates: List[Tuple[Path, int]] = []
        if codebase_root and codebase_root.is_dir():
            for nid in visited[:50]:
                p = node_id_to_repo_path(codebase_root, nid)
                if p:
                    file_candidates.append((p, depth_map.get(nid, 0)))
            for s in seeds:
                p = node_id_to_repo_path(codebase_root, s)
                if p:
                    file_candidates.append((p, 0))

        seen_fp: Set[str] = set()
        unique_files: List[Tuple[Path, int]] = []
        for p, h in file_candidates:
            key = str(p.resolve())
            if key in seen_fp:
                continue
            seen_fp.add(key)
            unique_files.append((p, h))
            if len(unique_files) >= 5:
                break

        blame_chain: List[dict] = []
        rank = 0
        for fp, hop in unique_files[:5]:
            commits = git_recent_commits(repo, fp)
            if not commits:
                continue
            c0 = commits[0]
            rel_git = _rel_str_for_git(repo, fp)
            blame_hash = git_blame_porcelain_commit(repo, rel_git, line_start, line_end)
            meta = c0
            if blame_hash and blame_hash != c0.get("commit_hash"):
                alt = git_commit_at(repo, blame_hash)
                meta = alt if alt else {**c0, "commit_hash": blame_hash}
            rank += 1
            author = meta.get("author_email") or meta.get("author_name") or ""
            blame_chain.append(
                {
                    "rank": rank,
                    "file_path": str(_rel_under_repo(fp, repo)),
                    "commit_hash": meta.get("commit_hash", ""),
                    "author": author,
                    "commit_timestamp": meta.get("commit_timestamp", ""),
                    "commit_message": meta.get("commit_message", ""),
                    "confidence_score": round(
                        confidence_score(violation_ts, str(meta.get("commit_timestamp", "")), hop),
                        4,
                    ),
                }
            )
        if not blame_chain:
            blame_chain.append(
                {
                    "rank": 1,
                    "file_path": "unknown",
                    "commit_hash": "",
                    "author": "",
                    "commit_timestamp": "",
                    "commit_message": "No upstream file resolved; check Week 4 lineage path and codebase_root.",
                    "confidence_score": 0.1,
                }
            )

        br = blast_radius_from_contract(contract, records_failing)
        viol: Dict[str, Any] = {
            "violation_id": str(uuid.uuid4()),
            "check_id": check_id,
            "detected_at": violation_ts,
            "blame_chain": blame_chain[:5],
            "blast_radius": br,
        }
        with violation_log.open("a", encoding="utf-8") as fq:
            fq.write(json.dumps(viol, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="ViolationAttributor (Phase 2B)")
    parser.add_argument("--report", "-r", type=Path, required=True)
    parser.add_argument("--contract", "-c", type=Path, required=True)
    parser.add_argument(
        "--lineage",
        "-l",
        type=Path,
        default=REPO_ROOT / "outputs/migrate/migrated_lineage_snapshots.jsonl",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=REPO_ROOT / "violation_log/violations.jsonl",
    )
    args = parser.parse_args()
    rp = args.report if args.report.is_absolute() else REPO_ROOT / args.report
    cp = args.contract if args.contract.is_absolute() else REPO_ROOT / args.contract
    lp = args.lineage if args.lineage.is_absolute() else REPO_ROOT / args.lineage
    op = args.output if args.output.is_absolute() else REPO_ROOT / args.output
    run_attribution(report_path=rp, contract_path=cp, lineage_path=lp, violation_log=op)
    print(f"Appended violations to {op}", file=sys.stderr)


if __name__ == "__main__":
    main()
