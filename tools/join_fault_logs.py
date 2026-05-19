#!/usr/bin/env python3
# Copyright 2023-2026 SGLang Team
# Licensed under the Apache License, Version 2.0 (the "License");
# ==============================================================================
"""Offline join tool for fault-driven logs.

Walks the shared fault-log directory, groups dump files by
``bootstrap_room``, merges router (JSON) + Python (pkl) views into a
single chronologically-sorted timeline per room, and prints / writes a
compact summary.

Usage::

    python join_fault_logs.py /shared/fault_log
    python join_fault_logs.py /shared/fault_log --room 1234567890
    python join_fault_logs.py /shared/fault_log --since 2026-05-19 --json out.json

File naming convention (produced by the router and tokenizer_manager)::

    <date>/<source>__<bootstrap_room>__<rid_or_notify_label>.{pkl,json}

Examples::

    20260519/router@host1__1234567890__abc.json
    20260519/prefill@host2__1234567890__abc.pkl
    20260519/decode@host3__1234567890__notified_by_prefill.pkl
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


def _parse_filename(path: str) -> Optional[Tuple[str, str, str, str]]:
    """Return (date_segment, source, room, suffix) parsed from filename."""
    parent = os.path.basename(os.path.dirname(path))
    fname = os.path.basename(path)
    stem, _ext = os.path.splitext(fname)
    parts = stem.split("__", 2)
    if len(parts) < 3:
        return None
    source, room, suffix = parts[0], parts[1], parts[2]
    return parent, source, room, suffix


def _load_dump(path: str) -> Optional[Dict[str, Any]]:
    """Load a fault-log dump file. Tolerates the two on-disk shapes:

    * Python tokenizer_manager: pickle of `{server_args, requests=[payload]}`.
    * Rust router: JSON file directly.
    """
    try:
        if path.endswith(".json"):
            with open(path, "r") as f:
                return json.load(f)
        else:
            with open(path, "rb") as f:
                obj = pickle.load(f)
            # tokenizer_manager wraps payload in {"server_args":..., "requests":[<payload>]}
            if isinstance(obj, dict) and "requests" in obj and obj["requests"]:
                req0 = obj["requests"][0]
                if isinstance(req0, dict):
                    return req0
                # legacy tuple shape from upstream dump_requests
                if isinstance(req0, tuple) and len(req0) >= 2:
                    return {
                        "obj": req0[0],
                        "out_dict": req0[1],
                        "events": [],
                    }
            return obj if isinstance(obj, dict) else None
    except Exception as e:
        print(f"[warn] failed to load {path}: {e!r}", file=sys.stderr)
        return None


def _events_from_dump(dump: Dict[str, Any], source: str) -> List[Tuple[float, str, str, dict]]:
    """Return [(ts_unix, source, stage, attrs)] from a dump dict.

    Python dumps store events as ``[(origin, stage, perf_counter_ts, attrs)]``
    (perf_counter, NOT unix); we approximate the unix time using
    ``finished_time`` as anchor when present. Router dumps store
    ``ts: <unix_seconds>`` directly.
    """
    out: List[Tuple[float, str, str, dict]] = []
    raw = dump.get("events") or []
    if not raw:
        return out

    # Detect router shape: list of {stage, ts, attrs}
    if isinstance(raw[0], dict) and "ts" in raw[0]:
        for ev in raw:
            ts = float(ev.get("ts") or 0.0)
            stage = str(ev.get("stage") or "?")
            attrs = ev.get("attrs") or {}
            out.append((ts, source, stage, attrs))
        return out

    # Python shape: list of (origin, stage, perf_counter_ts, attrs)
    finished_unix = dump.get("finished_time") or dump.get("created_time")
    last_perf = max(
        (float(ev[2]) for ev in raw if isinstance(ev, (list, tuple)) and len(ev) >= 3),
        default=0.0,
    )
    perf_to_unix_offset = (
        (float(finished_unix) - last_perf) if finished_unix and last_perf else 0.0
    )
    for ev in raw:
        if not isinstance(ev, (list, tuple)) or len(ev) < 3:
            continue
        if len(ev) == 3:
            origin, stage, ts = ev
            attrs = {}
        else:
            origin, stage, ts, attrs = ev[0], ev[1], ev[2], ev[3]
        unix_ts = float(ts) + perf_to_unix_offset
        out.append(
            (unix_ts, f"{source}/{origin}", str(stage), attrs if isinstance(attrs, dict) else {})
        )
    return out


def collect(root: str, since: Optional[datetime]) -> Dict[str, List[Tuple[str, Dict[str, Any]]]]:
    """Walk ``root`` and group dumps by ``bootstrap_room`` string."""
    by_room: Dict[str, List[Tuple[str, Dict[str, Any]]]] = defaultdict(list)
    for date_dir in sorted(os.listdir(root)):
        if since:
            try:
                date_obj = datetime.strptime(date_dir, "%Y%m%d")
                if date_obj < since:
                    continue
            except ValueError:
                # not a date directory, skip
                continue
        full = os.path.join(root, date_dir)
        if not os.path.isdir(full):
            continue
        for fname in os.listdir(full):
            if not (fname.endswith(".pkl") or fname.endswith(".json")):
                continue
            full_path = os.path.join(full, fname)
            parsed = _parse_filename(full_path)
            if parsed is None:
                continue
            _date, source, room, _suffix = parsed
            dump = _load_dump(full_path)
            if dump is None:
                continue
            by_room[room].append((source, dump))
    return by_room


def render_room_text(room: str, dumps: List[Tuple[str, Dict[str, Any]]]) -> str:
    """Produce a human-readable timeline for one room."""
    lines = [f"=== bootstrap_room={room} ({len(dumps)} dump(s)) ==="]
    sources = sorted({s for s, _ in dumps})
    lines.append(f"sources: {', '.join(sources)}")
    # Per-dump summary
    for source, dump in dumps:
        rid = dump.get("rid")
        msg = dump.get("error_message") or dump.get("error_summary") or ""
        sc = dump.get("status_code")
        lines.append(
            f"  - {source}: rid={rid} status={sc} msg={msg[:80]!r}"
        )
    # Merged event timeline
    all_events: List[Tuple[float, str, str, dict]] = []
    for source, dump in dumps:
        all_events.extend(_events_from_dump(dump, source))
    all_events.sort(key=lambda e: e[0])
    if all_events:
        lines.append("timeline:")
        first_ts = all_events[0][0]
        for ts, src, stage, attrs in all_events:
            delta_ms = (ts - first_ts) * 1000.0
            attr_str = (
                ", ".join(f"{k}={v}" for k, v in list(attrs.items())[:6]) if attrs else ""
            )
            lines.append(
                f"  {delta_ms:>8.2f}ms  {src:<28} {stage:<28} {attr_str}"
            )
    return "\n".join(lines) + "\n"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("root", help="Shared fault-log directory")
    p.add_argument("--room", help="Show only this bootstrap_room")
    p.add_argument(
        "--since", help="Only include dumps with date >= YYYY-MM-DD"
    )
    p.add_argument("--json", help="Write JSON-formatted output to this file")
    args = p.parse_args()

    if not os.path.isdir(args.root):
        print(f"error: {args.root} is not a directory", file=sys.stderr)
        return 1

    since_dt = None
    if args.since:
        try:
            since_dt = datetime.strptime(args.since, "%Y-%m-%d")
        except ValueError:
            print(f"error: bad --since '{args.since}', expected YYYY-MM-DD", file=sys.stderr)
            return 1

    by_room = collect(args.root, since_dt)
    if args.room:
        by_room = {k: v for k, v in by_room.items() if k == args.room}
    if not by_room:
        print("no fault-log dumps matched.", file=sys.stderr)
        return 0

    # Sort rooms by earliest event time so the most recent failures
    # are last in the output (chronological feed).
    def room_sort_key(item: Tuple[str, List[Tuple[str, Dict[str, Any]]]]) -> float:
        _room, dumps = item
        ts: List[float] = []
        for _src, dump in dumps:
            t = dump.get("created_time") or dump.get("ts_unix") or 0.0
            try:
                ts.append(float(t))
            except (TypeError, ValueError):
                pass
        return min(ts) if ts else 0.0

    rooms_sorted = sorted(by_room.items(), key=room_sort_key)

    if args.json:
        out_obj = []
        for room, dumps in rooms_sorted:
            events = []
            for source, dump in dumps:
                events.extend(_events_from_dump(dump, source))
            events.sort(key=lambda e: e[0])
            out_obj.append(
                {
                    "bootstrap_room": room,
                    "sources": sorted({s for s, _ in dumps}),
                    "dumps": [
                        {
                            "source": s,
                            "rid": d.get("rid"),
                            "status_code": d.get("status_code"),
                            "error_message": d.get("error_message")
                            or d.get("error_summary"),
                        }
                        for s, d in dumps
                    ],
                    "events": [
                        {"ts": ts, "source": src, "stage": stage, "attrs": attrs}
                        for (ts, src, stage, attrs) in events
                    ],
                }
            )
        with open(args.json, "w") as f:
            json.dump(out_obj, f, indent=2, default=str)
        print(f"wrote {args.json} with {len(out_obj)} rooms")
    else:
        for room, dumps in rooms_sorted:
            print(render_room_text(room, dumps))
    return 0


if __name__ == "__main__":
    sys.exit(main())
