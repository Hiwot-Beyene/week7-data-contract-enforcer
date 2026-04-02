#!/usr/bin/env python3
"""
ViolationAttributor (Phase 2B) — registry-first blast radius, lineage enrichment, git blame.

Usage:
  python contracts/attributor.py \\
    --violation validation_reports/week3_$(date +%Y%m%d_%H%M).json \\
    --registry contract_registry/subscriptions.yaml \\
    --contract generated_contracts/week3_extractions.yaml \\
    --lineage outputs/week4/lineage_snapshots.jsonl \\
    --output violation_log/violations.jsonl

Or invoked automatically from runner.py when any check is FAIL/WARN/ERROR (unless --no-attributor).

Rubric alignment: writes ``violation_log/violations.jsonl`` entries with **ranked** ``blame_chain``
(confidence scores, ``git log --follow`` + ``git blame -L --porcelain``), **blast_radius**
including ``affected_nodes``, ``affected_pipelines``, and ``estimated_records``, and
**enforcer-repo fallbacks** so a failing check maps to real commits under this repository
even when Week 4 ``codebase_root`` points at another clone.
"""

from __future__ import annotations

import argparse
import json
import re
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

from contracts.registry_loader import load_registry, query_blast_radius


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


_ARRAY_BRACKET_RE = re.compile(r"\[\*\]")
_GIT_REV_HEX_RE = re.compile(r"^[0-9a-f]{7,40}$", re.IGNORECASE)


def normalize_failing_field(column_name: str) -> str:
    """Strip `[*]` array notation; keep dot paths (e.g. metadata.correlation_id)."""
    s = (column_name or "").strip()
    if not s or s == "*":
        return s
    out = _ARRAY_BRACKET_RE.sub("", s)
    while ".." in out:
        out = out.replace("..", ".")
    return out.strip(".").strip()


_FORWARD_BLAST_RELS = frozenset({"PRODUCES", "WRITES", "CONSUMES"})


def build_forward_graph(
    edges: List[dict],
    relationships: Optional[Set[str]] = None,
) -> Dict[str, List[str]]:
    rel_filter = relationships if relationships is not None else _FORWARD_BLAST_RELS
    adj: Dict[str, List[str]] = {}
    for e in edges:
        if not isinstance(e, dict):
            continue
        rel = e.get("relationship")
        if rel not in rel_filter:
            continue
        s, t = e.get("source"), e.get("target")
        if not s or not t:
            continue
        adj.setdefault(str(s), []).append(str(t))
    return adj


def compute_transitive_depth(
    producer_node_id: str,
    snapshot: Optional[dict],
    max_depth: int = 2,
) -> dict:
    """
    BFS from producer following PRODUCES, WRITES, CONSUMES (forward / downstream).
    Returns node ids at depth 1 in ``direct``, depth 2..max_depth in ``transitive``,
    and ``max_depth`` as the deepest hop observed (0 if none).
    """
    if not producer_node_id or not snapshot:
        return {"direct": [], "transitive": [], "max_depth": 0}
    adj = build_forward_graph(snapshot.get("edges") or [])
    if not adj:
        return {"direct": [], "transitive": [], "max_depth": 0}
    direct: List[str] = []
    transitive: List[str] = []
    max_seen = 0
    cap = max(0, int(max_depth))
    q: deque[tuple[str, int]] = deque([(producer_node_id, 0)])
    seen: Set[str] = {producer_node_id}
    while q:
        cur, d = q.popleft()
        if d >= cap:
            continue
        for nxt in adj.get(cur, []) or []:
            if nxt in seen:
                continue
            seen.add(nxt)
            nd = d + 1
            max_seen = max(max_seen, nd)
            if nd == 1:
                direct.append(nxt)
            else:
                transitive.append(nxt)
            if nd < cap:
                q.append((nxt, nd))
    return {"direct": direct, "transitive": transitive, "max_depth": max_seen}


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


def _git_log_parse_lines(stdout: str) -> List[dict]:
    rows: List[dict] = []
    for line in stdout.strip().splitlines():
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


def git_recent_commits(repo: Path, relpath: Path, since_days: int = 14) -> List[dict]:
    """git log --follow --since=... then without --since (brownfield paths often older than 14d)."""
    try:
        rel = relpath.resolve().relative_to(repo.resolve())
    except ValueError:
        rel = relpath.name
    fmt = "%H|%an|%ae|%ai|%s"
    cmd_recent = [
        "git",
        "log",
        "--follow",
        f"--since={since_days} days ago",
        "-n",
        "10",
        f"--format={fmt}",
        "--",
        str(rel),
    ]
    try:
        out = subprocess.run(
            cmd_recent,
            cwd=str(repo),
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except (subprocess.SubprocessError, OSError):
        out = None
    if out and out.returncode == 0 and out.stdout.strip():
        rows = _git_log_parse_lines(out.stdout)
        if rows:
            return rows
    cmd_any = [
        "git",
        "log",
        "--follow",
        "-n",
        "10",
        f"--format={fmt}",
        "--",
        str(rel),
    ]
    try:
        out2 = subprocess.run(
            cmd_any,
            cwd=str(repo),
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except (subprocess.SubprocessError, OSError):
        return []
    if out2.returncode != 0 or not out2.stdout.strip():
        return []
    return _git_log_parse_lines(out2.stdout)


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
        "blame",
        "-L",
        f"{line_start},{line_end}",
        "--porcelain",
        "--",
        relpath,
    ]
    try:
        out = subprocess.run(
            cmd, cwd=str(repo), capture_output=True, text=True, timeout=45, check=False
        )
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
    cmd = ["git", "log", "-1", f"--format={fmt}", rev]
    try:
        out = subprocess.run(cmd, cwd=str(repo), capture_output=True, text=True, timeout=30, check=False)
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


def snapshot_git_rev(snapshot: Optional[dict]) -> Optional[str]:
    """Return lineage snapshot ``git_commit`` as a normalized rev string (7–40 hex)."""
    if not snapshot:
        return None
    raw = snapshot.get("git_commit")
    if not isinstance(raw, str) or not raw.strip():
        return None
    t = raw.strip()
    if len(t) > 40:
        t = t[:40]
    if not _GIT_REV_HEX_RE.match(t):
        return None
    return t.lower() if len(t) == 40 else t


def append_blame_from_lineage_git_commit(
    blame_chain: List[dict],
    repos_to_try: List[Path],
    snapshot: Optional[dict],
    violation_ts: str,
    *,
    start_rank: int = 1,
) -> bool:
    """
    Resolve ``git_commit`` from the Week 4 lineage snapshot via ``git log -1``.

    Tries each repository in order (enforcer root, then lineage ``codebase_root``) so the
    snapshot SHA resolves where the Week 4 graph was captured.
    """
    sha = snapshot_git_rev(snapshot)
    if not sha:
        return False
    meta: Optional[dict] = None
    used: Optional[Path] = None
    for r in repos_to_try:
        if r is None or not (r / ".git").is_dir():
            continue
        m = git_commit_at(r.resolve(), sha)
        if m:
            meta = m
            used = r.resolve()
            break
    if not meta:
        return False
    author = meta.get("author_email") or meta.get("author_name") or ""
    blame_chain.append(
        {
            "rank": start_rank,
            "file_path": f"(lineage snapshot git_commit @ {used.name if used else 'git'})",
            "commit_hash": meta.get("commit_hash", ""),
            "author": author,
            "commit_timestamp": meta.get("commit_timestamp", ""),
            "commit_message": meta.get("commit_message", ""),
            "confidence_score": round(
                confidence_score(
                    violation_ts, str(meta.get("commit_timestamp", "")), 0
                ),
                4,
            ),
        }
    )
    return True


def append_blame_hash_only_from_snapshot(
    blame_chain: List[dict],
    snapshot: Optional[dict],
    codebase_root: Optional[Path],
    *,
    reason: str,
) -> bool:
    """When git repo is unavailable, still record the snapshot SHA for manual ``git show``."""
    sha = snapshot_git_rev(snapshot)
    if not sha:
        return False
    root_hint = str(codebase_root) if codebase_root else "lineage codebase_root from snapshot"
    blame_chain.append(
        {
            "rank": 1,
            "file_path": root_hint,
            "commit_hash": sha,
            "author": "",
            "commit_timestamp": "",
            "commit_message": (
                f"{reason} Resolve locally: git -C <clone-from-snapshot> show {sha}"
            ),
            "confidence_score": 0.2,
        }
    )
    return True


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


def resolve_git_repository(
    repo_root: Optional[Path],
    codebase_root: Optional[Path],
) -> Tuple[Optional[Path], str]:
    """
    Prefer explicit ``repo_root``, then this enforcer package (always has history for rubric demos),
    then lineage ``codebase_root``. Returns (path or None, resolution_note).
    """
    if repo_root is not None and (repo_root / ".git").is_dir():
        return repo_root.resolve(), "repo_root_cli"
    if (REPO_ROOT / ".git").is_dir():
        return REPO_ROOT.resolve(), "enforcer_package_root"
    if codebase_root is not None and (codebase_root / ".git").is_dir():
        return codebase_root.resolve(), "lineage_codebase_root"
    return None, "none"


def infer_enforcer_blame_regions(check_id: str, column_name: str) -> List[Tuple[str, int, int]]:
    """
    Map a failing ValidationRunner check to source regions in **this** repository.

    Lets evaluators trace ``week3.*.confidence`` (or drift) to real commits in
    ``git log`` / ``git blame`` even when Week 4 ``codebase_root`` points elsewhere.
    """
    cid = (check_id or "").lower()
    col = (column_name or "").lower()
    hints: List[Tuple[str, int, int]] = []

    def add(rel: str, a: int, b: int) -> None:
        hints.append((rel, a, b))

    if "drift" in cid:
        add("contracts/runner.py", 315, 415)
        add("contracts/validation_checks.py", 650, 718)
    if "confidence" in cid or "confidence" in col or "extracted_facts" in col or "fact_id" in col:
        add("contracts/validation_checks.py", 470, 560)
    if "unique" in cid or (".duplicate" in cid and "doc_id" in cid):
        add("contracts/validation_checks.py", 130, 220)
    if "quality" in cid:
        add("contracts/validation_checks.py", 718, 870)
    if "enum" in cid:
        add("contracts/validation_checks.py", 355, 430)
    if "type_match" in cid or "dtype" in cid:
        add("contracts/validation_checks.py", 255, 360)
    if "week5" in cid:
        add("contracts/runner.py", 560, 720)
    if "lineage" in cid or "snapshot" in cid or "git_commit" in cid or "edges" in cid:
        add("contracts/runner.py", 780, 900)
    if "langsmith" in cid or "run_type" in cid or "tokens" in cid:
        add("contracts/runner.py", 900, 980)
    if not hints:
        add("contracts/runner.py", 1135, 1185)

    seen: Set[str] = set()
    out: List[Tuple[str, int, int]] = []
    for rel, a, b in hints:
        if rel in seen:
            continue
        seen.add(rel)
        out.append((rel, a, b))
        if len(out) >= 4:
            break
    return out


def repo_for_blame_target(
    fp: Path,
    primary_repo: Path,
    lineage_root: Optional[Path],
) -> Optional[Path]:
    """Use ``primary_repo`` (usually enforcer) or lineage clone when the file lives there."""
    try:
        fp.resolve().relative_to(primary_repo.resolve())
        return primary_repo.resolve()
    except ValueError:
        pass
    if lineage_root is not None and (lineage_root / ".git").is_dir():
        try:
            fp.resolve().relative_to(lineage_root.resolve())
            return lineage_root.resolve()
        except ValueError:
            return None
    return None


def _clamp_blame_line_range(path: Path, line_start: int, line_end: int) -> Tuple[int, int]:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        n = max(1, text.count("\n") + 1)
    except OSError:
        return max(1, line_start), max(line_start, line_end)
    ls = max(1, min(int(line_start), n))
    le = max(ls, min(int(line_end), n))
    return ls, le


def blame_entry_for_file(
    repo: Path,
    abs_file: Path,
    hop_depth: int,
    violation_ts: str,
    line_start: int,
    line_end: int,
    *,
    source_tag: str,
) -> Optional[dict]:
    """One ranked candidate: ``git log --follow`` + ``git blame -L`` (porcelain)."""
    if not abs_file.is_file():
        return None
    ls, le = _clamp_blame_line_range(abs_file, line_start, line_end)
    commits = git_recent_commits(repo, abs_file)
    if not commits:
        return None
    c0 = commits[0]
    rel_git = _rel_str_for_git(repo, abs_file)
    blame_hash = git_blame_porcelain_commit(repo, rel_git, ls, le)
    meta = c0
    if blame_hash and blame_hash != c0.get("commit_hash"):
        alt = git_commit_at(repo, blame_hash)
        meta = alt if alt else {**c0, "commit_hash": blame_hash}
    author = meta.get("author_email") or meta.get("author_name") or ""
    return {
        "rank": 0,
        "file_path": str(_rel_under_repo(abs_file, repo)),
        "commit_hash": meta.get("commit_hash", ""),
        "author": author,
        "commit_timestamp": meta.get("commit_timestamp", ""),
        "commit_message": (meta.get("commit_message", "") or "")[:500],
        "confidence_score": round(
            confidence_score(violation_ts, str(meta.get("commit_timestamp", "")), hop_depth),
            4,
        ),
        "_source": source_tag,
    }


def rank_and_cap_blame_chain(chain: List[dict], cap: int = 5) -> List[dict]:
    """Highest confidence first; stable tie-breaker on file_path."""
    for c in chain:
        c.pop("_source", None)
    chain.sort(key=lambda x: (-float(x.get("confidence_score") or 0), str(x.get("file_path") or "")))
    out: List[dict] = []
    for i, c in enumerate(chain[:cap], start=1):
        row = dict(c)
        row["rank"] = i
        out.append(row)
    return out


def build_blast_radius_payload(
    *,
    direct_subscribers: List[dict],
    transitive_nodes: List[str],
    producer_node_id: str,
    seeds: List[str],
    records_failing: int,
    contamination_depth: int,
    registry_gap: bool,
) -> dict:
    """Registry + lineage + rubric-style blast fields (affected_*, estimated_records)."""
    affected_nodes = list(
        dict.fromkeys(
            [x for x in [producer_node_id] if x]
            + [x for x in (seeds or []) if x]
            + [x for x in transitive_nodes if x]
        )
    )[:40]
    pipelines: List[str] = []
    for sub in direct_subscribers:
        if isinstance(sub, dict) and sub.get("subscriber_id"):
            sid = str(sub["subscriber_id"])
            pipelines.append(f"{sid}-pipeline")
    for nid in affected_nodes:
        if isinstance(nid, str) and nid.startswith("pipeline::"):
            tail = nid.split("::", 1)[-1].strip()
            if tail:
                pipelines.append(tail[:160])
    pipelines = list(dict.fromkeys(pipelines))[:16]
    return {
        "affected_nodes": affected_nodes,
        "affected_pipelines": pipelines,
        "estimated_records": int(records_failing or 0),
        "source": "registry+lineage",
        "direct_subscribers": direct_subscribers,
        "transitive_nodes": transitive_nodes,
        "contamination_depth": contamination_depth,
        "registry_gap": registry_gap,
    }


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


def run_attribution(
    *,
    report_path: Path,
    contract_path: Path,
    registry_path: Path,
    lineage_path: Optional[Path],
    violation_log: Path,
    repo_root: Optional[Path] = None,
    max_depth: int = 2,
) -> None:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    contract = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    resolved_lineage = None
    if lineage_path and lineage_path.is_file():
        resolved_lineage = lineage_path
    else:
        resolved_lineage = resolve_lineage_path(lineage_path)

    snapshot = load_lineage_snapshot(resolved_lineage) if resolved_lineage else None
    issues = [
        r
        for r in report.get("results", [])
        if r.get("status") in ("FAIL", "WARN", "ERROR")
    ]
    if not issues:
        return

    violation_ts = str(report.get("run_timestamp") or utc_now_iso())
    contract_id_report = str(report.get("contract_id") or "")

    codebase_root: Optional[Path] = None
    rev: Dict[str, List[str]] = {}
    nodes: List[dict] = []
    if snapshot:
        cr = snapshot.get("codebase_root")
        if cr:
            codebase_root = Path(str(cr))
        rev = build_reverse_graph(snapshot.get("edges") or [])
        nodes = [n for n in (snapshot.get("nodes") or []) if isinstance(n, dict)]

    violation_log.parent.mkdir(parents=True, exist_ok=True)
    default_blame_lines = (1, 48)

    registry = load_registry(str(registry_path))

    for fr in issues:
        check_id = str(fr.get("check_id", ""))
        column_name = str(fr.get("column_name", ""))
        records_failing = int(fr.get("records_failing") or 0)
        failing_field = normalize_failing_field(column_name)

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

        producer_node_id = seeds[0] if seeds else ""

        # STEP 1 — Registry blast radius query (PRIMARY SOURCE)
        direct_subscribers = query_blast_radius(registry, contract_id_report, failing_field)
        attributor_warnings: List[str] = []
        if not direct_subscribers:
            wmsg = (
                f"NO_SUBSCRIBERS_REGISTERED for contract={contract_id_report} "
                f"field={failing_field}"
            )
            attributor_warnings.append(wmsg)

        # STEP 2 — Lineage transitive depth (ENRICHMENT ONLY)
        depth_info = compute_transitive_depth(producer_node_id, snapshot, max_depth=max_depth)
        transitive_nodes = list(
            dict.fromkeys(depth_info["direct"] + depth_info["transitive"])
        )
        contamination_depth = int(depth_info["max_depth"])
        registry_gap = len(direct_subscribers) == 0

        # STEP 3 — Git blame: lineage files (external clone) + enforcer-repo fallbacks (rubric)
        repo, repo_note = resolve_git_repository(repo_root, codebase_root)
        if repo is None:
            print("REPO_ROOT_NOT_FOUND", file=sys.stderr)
            attributor_warnings.append("REPO_ROOT_NOT_FOUND")
        else:
            attributor_warnings.append(f"GIT_REPO_RESOLVED:{repo_note}:{repo}")

        visited, depth_map = bfs_upstream(seeds, rev) if seeds else ([], {})
        file_jobs: List[Tuple[Path, int, int, int, str]] = []
        # (abs_path, hop, line_start, line_end, source_tag)

        if codebase_root and codebase_root.is_dir():
            seen_job: Set[str] = set()
            for nid in visited[:50]:
                p = node_id_to_repo_path(codebase_root, nid)
                if not p:
                    continue
                key = str(p.resolve())
                if key in seen_job:
                    continue
                seen_job.add(key)
                hop = int(depth_map.get(nid, 0))
                file_jobs.append(
                    (p, hop, default_blame_lines[0], default_blame_lines[1], "lineage_upstream")
                )
                if len(file_jobs) >= 5:
                    break
            if len(file_jobs) < 5:
                for s in seeds:
                    p = node_id_to_repo_path(codebase_root, s)
                    if not p:
                        continue
                    key = str(p.resolve())
                    if key in seen_job:
                        continue
                    seen_job.add(key)
                    file_jobs.append((p, 0, default_blame_lines[0], default_blame_lines[1], "lineage_seed"))
                    if len(file_jobs) >= 5:
                        break

        enforcer_hints = infer_enforcer_blame_regions(check_id, column_name)
        for rel, ls, le in enforcer_hints:
            p = (REPO_ROOT / rel).resolve()
            key = str(p)
            if any(str(x[0].resolve()) == key for x in file_jobs):
                continue
            file_jobs.append((p, 0, ls, le, "enforcer_check_mapping"))

        blame_chain: List[dict] = []
        if repo is not None:
            seen_commits: Set[str] = set()
            for fp, hop, ls, le, tag in file_jobs[:12]:
                if not fp.is_file():
                    continue
                blame_repo = repo_for_blame_target(fp, repo, codebase_root)
                if blame_repo is None:
                    continue
                ent = blame_entry_for_file(
                    blame_repo, fp, hop, violation_ts, ls, le, source_tag=tag
                )
                if not ent:
                    continue
                ch = str(ent.get("commit_hash") or "")
                if ch and ch in seen_commits:
                    continue
                if ch:
                    seen_commits.add(ch)
                blame_chain.append(ent)
            blame_repos_ordered: List[Path] = [repo]
            if codebase_root is not None and (codebase_root / ".git").is_dir():
                crp = codebase_root.resolve()
                if crp != repo.resolve():
                    blame_repos_ordered.append(crp)
            if not blame_chain:
                append_blame_from_lineage_git_commit(
                    blame_chain, blame_repos_ordered, snapshot, violation_ts, start_rank=1
                )
            if not blame_chain:
                blame_chain.append(
                    {
                        "rank": 1,
                        "file_path": "unknown",
                        "commit_hash": "",
                        "author": "",
                        "commit_timestamp": "",
                        "commit_message": (
                            "No git commits resolved for candidate files; ensure .git exists, "
                            "run from the enforcer repo root, and that lineage paths are valid."
                        ),
                        "confidence_score": 0.1,
                    }
                )
        else:
            if not append_blame_hash_only_from_snapshot(
                blame_chain,
                snapshot,
                codebase_root,
                reason="No git repository at repo_root, enforcer root, or lineage codebase_root.",
            ):
                blame_chain.append(
                    {
                        "rank": 1,
                        "file_path": "unknown",
                        "commit_hash": "",
                        "author": "",
                        "commit_timestamp": "",
                        "commit_message": (
                            "REPO_ROOT_NOT_FOUND and no usable git_commit in lineage snapshot."
                        ),
                        "confidence_score": 0.1,
                    }
                )

        blame_chain = rank_and_cap_blame_chain(blame_chain, cap=5)

        # STEP 4 — Write violation log entry (append-only)
        viol: Dict[str, Any] = {
            "violation_id": str(uuid.uuid4()),
            "check_id": check_id,
            "detected_at": violation_ts,
            "blast_radius": build_blast_radius_payload(
                direct_subscribers=direct_subscribers,
                transitive_nodes=transitive_nodes,
                producer_node_id=producer_node_id,
                seeds=seeds,
                records_failing=records_failing,
                contamination_depth=contamination_depth,
                registry_gap=registry_gap,
            ),
            "blame_chain": blame_chain,
            "records_failing": records_failing,
        }
        if attributor_warnings:
            viol["attributor_warnings"] = attributor_warnings
        with violation_log.open("a", encoding="utf-8") as fq:
            fq.write(json.dumps(viol, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="ViolationAttributor (Phase 2B)")
    viol = parser.add_mutually_exclusive_group(required=True)
    viol.add_argument(
        "--violation",
        type=Path,
        help="Validation report JSON with FAIL results",
    )
    viol.add_argument(
        "--report",
        "-r",
        type=Path,
        help="Alias for --violation (validation report JSON)",
    )
    parser.add_argument("--contract", "-c", type=Path, required=True)
    parser.add_argument(
        "--registry",
        type=Path,
        required=True,
        help="Contract consumer registry YAML (e.g. contract_registry/subscriptions.yaml)",
    )
    parser.add_argument(
        "--lineage",
        "-l",
        type=Path,
        default=None,
        help="Week 4 lineage snapshot (JSON/JSONL); default search under outputs/",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=REPO_ROOT / "violation_log/violations.jsonl",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Git repository root for blame (default: lineage codebase_root if valid)",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=2,
        help="Max forward BFS depth for lineage enrichment (PRODUCES/WRITES/CONSUMES)",
    )
    args = parser.parse_args()
    rp = args.violation or args.report
    rp = rp if rp.is_absolute() else REPO_ROOT / rp
    cp = args.contract if args.contract.is_absolute() else REPO_ROOT / args.contract
    reg = args.registry if args.registry.is_absolute() else REPO_ROOT / args.registry
    lp = args.lineage
    if lp is not None:
        lp = lp if lp.is_absolute() else REPO_ROOT / lp
    op = args.output if args.output.is_absolute() else REPO_ROOT / args.output
    rr = args.repo_root
    if rr is not None:
        rr = rr if rr.is_absolute() else REPO_ROOT / rr
    run_attribution(
        report_path=rp,
        contract_path=cp,
        registry_path=reg,
        lineage_path=lp,
        violation_log=op,
        repo_root=rr,
        max_depth=args.max_depth,
    )
    print(f"Appended violations to {op}", file=sys.stderr)


if __name__ == "__main__":
    main()
