"""Bounded JSONL ingestion and a presentation contract over the existing analyzer."""
from __future__ import annotations

import json
import time
from math import ceil
from collections import Counter, defaultdict
from dataclasses import asdict
from statistics import mean, median
from typing import BinaryIO, Any

from .analyzer import SparkPerformanceAnalyzer
from .heuristics import run_all_rules

MAX_BYTES = 25 * 1024 * 1024
MAX_LINE = 1024 * 1024
MAX_EVENTS = 150_000
MAX_TASKS = 25_000
MAX_STAGES = 2_000
ANALYSIS_SECONDS = 30


class LogError(ValueError):
    def __init__(self, message: str, code: str = "invalid_log", status: int = 422):
        super().__init__(message)
        self.code, self.status = code, status


def number(obj: dict, key: str) -> int | None:
    value = obj.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 2**53 - 1:
        raise LogError(f"{key} must be a non-negative integer within JavaScript's safe range.")
    return value


def obj(value: Any, field: str) -> dict:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise LogError(f"{field} must be an object.")
    return value


def text(value: Any, default: str = "Unknown") -> str:
    return str(value if value is not None else default)[:240]


def read_events(handle: BinaryIO, deadline: float) -> tuple[list[dict], int]:
    """Keep only supported fields; never retain SQL, properties, stack traces, or raw logs."""
    events, size, count, tasks, stages = [], 0, 0, 0, 0
    while True:
        if time.monotonic() > deadline:
            raise LogError("Analysis exceeded 30 seconds. Export a smaller event log.", "analysis_timeout", 408)
        raw = handle.readline(MAX_LINE + 1)
        if not raw:
            break
        size += len(raw)
        if size > MAX_BYTES:
            raise LogError("Event log exceeds the 25 MiB upload limit.", "too_large", 413)
        if len(raw) > MAX_LINE:
            raise LogError("One event exceeds the 1 MiB line limit.", "event_too_large", 413)
        if not raw.strip():
            continue
        count += 1
        if count > MAX_EVENTS:
            raise LogError("Event log exceeds 150,000 events.", "too_many_events", 413)
        try:
            e = json.loads(raw.decode("utf-8-sig") if count == 1 else raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError, RecursionError) as exc:
            raise LogError(f"Invalid UTF-8 JSON on event line {count}. Upload uncompressed JSON Lines.") from exc
        if not isinstance(e, dict) or not isinstance(e.get("Event"), str):
            raise LogError(f"Event line {count} must be an object with an Event name.")
        kind = e["Event"]
        clean = {"Event": kind}
        if kind in ("SparkListenerTaskStart", "SparkListenerTaskEnd"):
            tasks += 1
            if tasks > MAX_TASKS * 2:
                raise LogError("Too many task events; limit is 25,000 task attempts.", "too_many_tasks", 413)
            ti = obj(e.get("Task Info"), "Task Info")
            for key in ("Stage ID", "Stage Attempt ID"):
                clean[key] = number(e, key)
            if clean["Stage ID"] is None or number(ti, "Task ID") is None:
                raise LogError("Task events require Stage ID and Task Info.Task ID.")
            clean["Task Info"] = {k: number(ti, k) for k in
                                  ("Task ID", "Index", "Attempt", "Launch Time", "Finish Time")}
            clean["Task Info"]["Executor ID"] = text(ti.get("Executor ID"))
            # Spark emits Finish Time=0 on TaskStart, meaning not yet finished.
            for timestamp in ("Launch Time", "Finish Time"):
                if clean["Task Info"][timestamp] == 0:
                    clean["Task Info"][timestamp] = None
            launch, finish = clean["Task Info"]["Launch Time"], clean["Task Info"]["Finish Time"]
            if launch is not None and finish is not None and finish < launch:
                raise LogError("Task Finish Time precedes Launch Time.")
            clean["Task Metrics"] = {}
            metrics = obj(e.get("Task Metrics"), "Task Metrics")
            for key in ("Executor Run Time", "JVM GC Time", "Memory Bytes Spilled", "Disk Bytes Spilled"):
                if number(metrics, key) is not None:
                    clean["Task Metrics"][key] = metrics[key]
            for key, fields in {
                "Shuffle Read Metrics": ("Remote Bytes Read", "Local Bytes Read", "Fetch Wait Time"),
                "Shuffle Write Metrics": ("Shuffle Bytes Written",),
                "Input Metrics": ("Bytes Read",), "Output Metrics": ("Bytes Written",),
            }.items():
                nested = obj(metrics.get(key), key)
                clean["Task Metrics"][key] = {k: number(nested, k) for k in fields if number(nested, k) is not None}
            reason = e.get("Task End Reason", "Unknown")
            clean["Task End Reason"] = text(reason.get("Reason") if isinstance(reason, dict) else reason)
        elif kind in ("SparkListenerStageSubmitted", "SparkListenerStageCompleted"):
            stages += 1
            if stages > MAX_STAGES * 2:
                raise LogError("Too many stage events; limit is 2,000 stage attempts.", "too_many_stages", 413)
            si = obj(e.get("Stage Info"), "Stage Info")
            info = {k: number(si, k) for k in ("Stage ID", "Stage Attempt ID", "Number of Tasks", "Submission Time", "Completion Time")}
            if info["Stage ID"] is None:
                raise LogError("Stage Info requires Stage ID.")
            parents = si.get("Parent IDs") or []
            if not isinstance(parents, list) or len(parents) > MAX_STAGES:
                raise LogError("Parent IDs must be a bounded array.")
            info["Parent IDs"] = [number({"id": p}, "id") for p in parents]
            if None in info["Parent IDs"]:
                raise LogError("Parent IDs cannot contain null.")
            info["Stage Name"] = text(si.get("Stage Name"), f"Stage {info['Stage ID']}")
            info["Failure Reason"] = bool(si.get("Failure Reason"))
            clean["Stage Info"] = info
        elif kind in ("SparkListenerJobStart", "SparkListenerJobEnd"):
            clean["Job ID"] = number(e, "Job ID")
            if clean["Job ID"] is None:
                raise LogError("Job events require Job ID.")
            ids = e.get("Stage IDs") or []
            if not isinstance(ids, list) or len(ids) > MAX_STAGES:
                raise LogError("Stage IDs must be a bounded array.")
            clean["Stage IDs"] = [number({"id": p}, "id") for p in ids]
            if None in clean["Stage IDs"]:
                raise LogError("Stage IDs cannot contain null.")
            for key in ("Submission Time", "Completion Time"):
                clean[key] = number(e, key)
            result = e.get("Job Result", {})
            clean["Job Result"] = text(result.get("Result") if isinstance(result, dict) else result)
        elif kind in ("SparkListenerApplicationStart", "SparkListenerApplicationEnd"):
            clean.update({"Timestamp": number(e, "Timestamp"), "App Name": text(e.get("App Name")),
                          "App ID": text(e.get("App ID"))})
        elif kind == "SparkListenerEnvironmentUpdate":
            props = obj(e.get("Spark Properties"), "Spark Properties")
            clean["Spark Properties"] = {"spark.app.id": text(props.get("spark.app.id"))}
        else:
            continue
        events.append(clean)
    if not events:
        raise LogError("No supported Spark listener events found. Use a Spark JSON Lines event log.")
    return events, count


def analyze_stream(handle: BinaryIO, *, sample: bool = False) -> dict:
    began = time.monotonic()
    deadline = began + ANALYSIS_SECONDS
    events, event_count = read_events(handle, deadline)
    if sum(e["Event"] == "SparkListenerApplicationStart" for e in events) > 1:
        raise LogError("Upload events from one Spark application at a time.")
    analyzer = SparkPerformanceAnalyzer()
    jobs, stages, tasks = {}, {}, {}
    app = {"id": "Unknown", "name": "Unknown", "start_ms": None, "end_ms": None}
    for e in events:
        kind = e["Event"]
        if kind == "SparkListenerApplicationStart":
            app.update(id=e["App ID"], name=e["App Name"], start_ms=e["Timestamp"])
        elif kind == "SparkListenerApplicationEnd":
            app["end_ms"] = e["Timestamp"]
        elif kind == "SparkListenerEnvironmentUpdate" and e["Spark Properties"]["spark.app.id"] != "Unknown":
            app["id"] = e["Spark Properties"]["spark.app.id"]
        elif kind.startswith("SparkListenerJob"):
            j = jobs.setdefault(e["Job ID"], {"id": e["Job ID"], "stage_ids": [], "start_ms": None, "end_ms": None, "status": "incomplete"})
            if kind == "SparkListenerJobStart":
                j.update(stage_ids=e["Stage IDs"], start_ms=e["Submission Time"])
            else:
                j.update(end_ms=e["Completion Time"], status=e["Job Result"])
        elif kind.startswith("SparkListenerStage"):
            si = e["Stage Info"]
            key = (si["Stage ID"], si["Stage Attempt ID"] or 0)
            s = stages.setdefault(key, {})
            s.update({k: v for k, v in si.items() if v is not None})
            if kind == "SparkListenerStageCompleted":
                s["status"] = "failed" if si["Failure Reason"] else "completed"
        elif kind.startswith("SparkListenerTask"):
            ti = e["Task Info"]
            key = (e["Stage ID"], e["Stage Attempt ID"] or 0, ti["Task ID"], ti["Attempt"] or 0)
            if key not in tasks or kind == "SparkListenerTaskEnd":
                tasks[key] = e
            stages.setdefault(key[:2], {})
        if len(tasks) > MAX_TASKS or len(stages) > MAX_STAGES or len(jobs) > MAX_STAGES:
            raise LogError("Log exceeds 25,000 task attempts or 2,000 jobs/stage attempts.", "record_limit", 413)
    if not stages and not jobs:
        raise LogError("No job, stage, or task records found. The log may contain only application metadata.")
    grouped = defaultdict(list)
    for key, e in tasks.items():
        grouped[key[:2]].append((key, e))
    stage_rows, task_rows, findings = [], [], []
    missing_runtime = missing_shuffle = missing_input = 0
    for (sid, attempt), meta in sorted(stages.items()):
        if time.monotonic() > deadline:
            raise LogError("Analysis exceeded 30 seconds. Export a smaller event log.", "analysis_timeout", 408)
        entries = grouped[(sid, attempt)]
        ended = [e for _, e in entries if e["Event"] == "SparkListenerTaskEnd"]
        job_ids = [j["id"] for j in jobs.values() if sid in j["stage_ids"]]
        core = analyzer._build_stage_metrics(sid, {"stage_name": meta.get("Stage Name", f"Stage {sid}"),
                     "num_tasks": meta.get("Number of Tasks"), "parent_ids": meta.get("Parent IDs", []), "job_ids": job_ids}, ended)
        # Baselines use one successful attempt per partition; retries/failures remain in totals.
        successful = {}
        for e in ended:
            if e["Task End Reason"] == "Success":
                ti = e["Task Info"]
                successful[ti["Index"] if ti["Index"] is not None else ti["Task ID"]] = e
        known = [e["Task Metrics"]["Executor Run Time"] for e in successful.values() if "Executor Run Time" in e["Task Metrics"]]
        baseline = mean(known) if known else None
        core.runtime_avg_ms = baseline or 0
        core.runtime_max_ms = max(known, default=0)
        core.runtime_skew_ratio = core.runtime_max_ms / baseline if baseline else 0
        core.runtime_p95_ms = sorted(known)[ceil(.95 * len(known)) - 1] if known else 0
        core.failed_tasks = sum(e["Task End Reason"] not in ("Success", "Unknown", "incomplete") for e in ended)
        loads = []
        for e in successful.values():
            m = e["Task Metrics"]
            if "Bytes Read" in m["Input Metrics"] and all(k in m["Shuffle Read Metrics"] for k in ("Remote Bytes Read", "Local Bytes Read")):
                loads.append(m["Input Metrics"]["Bytes Read"] + sum(m["Shuffle Read Metrics"][k] for k in ("Remote Bytes Read", "Local Bytes Read")))
        core.partition_imbalance_ratio = max(loads) / mean(loads) if len(loads) >= 2 and mean(loads) else 0
        start = meta.get("Submission Time")
        end = meta.get("Completion Time")
        launches = [e["Task Info"]["Launch Time"] for _, e in entries if e["Task Info"]["Launch Time"] is not None]
        finishes = [e["Task Info"]["Finish Time"] for _, e in entries if e["Task Info"]["Finish Time"] is not None]
        if start is None and launches:
            start = min(launches)
        if end is None and finishes:
            end = max(finishes)
        if start is not None and end is not None and end < start:
            raise LogError("Stage completion precedes its submission.")
        elapsed = max(0, end - start) if start is not None and end is not None else None
        row = {**asdict(core), "attempt": attempt, "key": f"{sid}.{attempt}", "start_ms": start, "end_ms": end,
               "duration_ms": elapsed, "task_time_ms": sum(e["Task Metrics"].get("Executor Run Time", 0) for e in ended),
               "status": meta.get("status", "incomplete"), "observed_tasks": len(entries), "stragglers": 0,
               "runtime_skew_ratio": core.runtime_skew_ratio if known else None,
               "partition_imbalance_ratio": core.partition_imbalance_ratio if len(loads) >= 2 else None,
               "runtime_avg_ms": baseline, "runtime_median_ms": median(known) if known else None}
        # Reuse the original rules. Findings are candidates, never a promised speedup.
        for diagnostic in run_all_rules(core, 512, 3.0):
            findings.append({**asdict(diagnostic), "stage_attempt": attempt, "stage_key": row["key"]})
        for key, e in entries:
            ti, m = e["Task Info"], e["Task Metrics"]
            runtime = m.get("Executor Run Time")
            sr = m["Shuffle Read Metrics"]
            read = sum(sr[k] for k in ("Remote Bytes Read", "Local Bytes Read")) if all(k in sr for k in ("Remote Bytes Read", "Local Bytes Read")) else None
            write = m["Shuffle Write Metrics"].get("Shuffle Bytes Written")
            inp = m["Input Metrics"].get("Bytes Read")
            missing_runtime += runtime is None
            missing_shuffle += read is None or write is None
            missing_input += inp is None
            slow = bool(e["Task End Reason"] == "Success" and baseline and runtime is not None and runtime >= baseline * 2.5)
            row["stragglers"] += slow
            task_rows.append({"id": ti["Task ID"], "attempt": ti["Attempt"] or 0, "partition": ti["Index"],
                              "stage_id": sid, "stage_attempt": attempt, "stage_key": row["key"], "executor": ti["Executor ID"],
                              "start_ms": ti["Launch Time"], "end_ms": ti["Finish Time"], "runtime_ms": runtime,
                              "shuffle_read_bytes": read, "shuffle_write_bytes": write, "input_bytes": inp,
                              "fetch_wait_ms": sr.get("Fetch Wait Time"), "gc_ms": m.get("JVM GC Time"),
                              "status": e["Task End Reason"] if e["Event"] == "SparkListenerTaskEnd" else "incomplete", "straggler": slow})
        stage_rows.append(row)
    executors = defaultdict(list)
    for t in task_rows:
        executors[t["executor"]].append(t)
    executor_rows = [{"id": eid, "tasks": len(ts), "failed": sum(t["status"] not in ("Success", "incomplete", "Unknown") for t in ts),
                      "runtime_ms": sum(t["runtime_ms"] or 0 for t in ts), "gc_ms": sum(t["gc_ms"] or 0 for t in ts),
                      "shuffle_read_bytes": sum(t["shuffle_read_bytes"] or 0 for t in ts),
                      "shuffle_write_bytes": sum(t["shuffle_write_bytes"] or 0 for t in ts),
                      "stragglers": sum(t["straggler"] for t in ts)} for eid, ts in sorted(executors.items())]
    starts = [r["start_ms"] for r in [*stage_rows, *jobs.values()] if r["start_ms"] is not None]
    ends = [r["end_ms"] for r in [*stage_rows, *jobs.values()] if r["end_ms"] is not None]
    start = app["start_ms"] if app["start_ms"] is not None else min(starts, default=None)
    end = app["end_ms"] if app["end_ms"] is not None else max(ends, default=None)
    if start is not None and end is not None and end < start:
        raise LogError("Application end precedes its start.")
    warnings = []
    if app["end_ms"] is None:
        warnings.append("ApplicationEnd is absent. Duration covers the observed event window; execution may be incomplete.")
    for count, label in ((missing_runtime, "runtime"), (missing_shuffle, "shuffle"), (missing_input, "input")):
        if count:
            warnings.append(f"{count} task attempts lack {label} metrics. Totals sum known values; missing measurements are shown as unavailable.")
    if any(s["status"] == "incomplete" for s in stage_rows):
        warnings.append("Some stages lack StageCompleted events. Timing may be inferred from observed task timestamps.")
    if any(t["partition"] is None for t in task_rows):
        warnings.append("Partition indexes are missing for some tasks; their retry counts cannot be reconstructed reliably.")
    if any(t["status"] == "Unknown" for t in task_rows):
        warnings.append("Some task outcomes are unknown; these are excluded from success and failure counts.")
    counts = Counter(t["status"] for t in task_rows)
    summary = {"jobs": len(jobs), "stages": len({s["stage_id"] for s in stage_rows}), "stage_attempts": len(stage_rows),
               "tasks": len(task_rows), "completed_tasks": counts["Success"],
               "failed_tasks": sum(v for k, v in counts.items() if k not in ("Success", "Unknown", "incomplete")),
               "incomplete_tasks": counts["incomplete"] + counts["Unknown"],
               "duration_ms": max(0, end - start) if start is not None and end is not None else None,
               "duration_source": "application timestamps" if app["start_ms"] is not None and app["end_ms"] is not None else "observed event window",
               "shuffle_read_bytes": sum(t["shuffle_read_bytes"] or 0 for t in task_rows),
               "shuffle_write_bytes": sum(t["shuffle_write_bytes"] or 0 for t in task_rows),
               "stragglers": sum(t["straggler"] for t in task_rows),
               "skewed_stages": sum(s["runtime_skew_ratio"] is not None and s["runtime_skew_ratio"] >= 3 for s in stage_rows),
               "executors": len(executor_rows)}
    return {"schema_version": 1, "source": "synthetic sample" if sample else "uploaded event log", "application": app,
            "summary": summary, "jobs": list(jobs.values()), "stages": stage_rows, "tasks": task_rows,
            "executors": executor_rows, "diagnostics": findings, "warnings": warnings,
            "event_count": event_count, "supported_event_count": len(events), "processing_ms": round((time.monotonic() - began) * 1000, 2),
            "thresholds": {"shuffle_mib": 512, "runtime_skew": 3, "straggler": 2.5, "partition_imbalance": 2}}
