# Fault-Driven Logging for SGLang (router + prefill + decode)

This document describes the **fault-driven per-request logging** feature
added on top of `sglang-main-0519newest`. The feature is opt-in via a
single startup flag; when off, the system behaves byte-for-byte
identically to upstream sglang.

The implementation lives in:

```
sglang-changed/
├── FAULT_LOG.md                                 ← this file
├── python/sglang/srt/
│   ├── server_args.py                           (+startup args)
│   ├── observability/
│   │   ├── req_time_stats.py                    (+events list + emit())
│   │   ├── room_event_buffer.py                 (NEW: per-room buffer)
│   │   └── fault_log_notifier.py                (NEW: async HTTP notify)
│   ├── disaggregation/utils.py                  (prepare_abort emits "abort")
│   ├── managers/tokenizer_manager.py            (dump on error + notify peers)
│   └── entrypoints/http_server.py               (+/fault-log/notify endpoint)
├── sgl-model-gateway/src/
│   ├── observability/
│   │   ├── mod.rs                               (+pub mod fault_log)
│   │   └── fault_log.rs                         (NEW: Rust per-room buffer)
│   ├── routers/http/pd_router.rs                (emit at dispatch + ROUTER_URL_HEADER)
│   ├── server.rs                                (+/fault-log/notify route)
│   └── main.rs                                  (+CLI args + fault_log::init())
└── tools/join_fault_logs.py                     (NEW: offline join)
```

## What it does

For every request that goes through the cluster, each component
(router, prefill server, decode server) records the request's path
through it into an in-memory **per-`bootstrap_room` ring buffer**.

* If the request finishes normally, nothing is persisted — the room's
  events age out of the buffer. Cost: a handful of `list.append`
  calls in memory.

* If the request finishes with an error on this component, the
  component:
  1. Writes a dump file to the shared fault-log directory.
  2. Fires asynchronous HTTP `POST /fault-log/notify` to its peers
     (router → prefill+decode; prefill → router; decode → router).

* When a peer receives `/fault-log/notify`, it dumps **its own**
  buffer for the same `bootstrap_room` to the shared dir, tagged as
  `notified_by_<peer>`.

* An offline tool (`tools/join_fault_logs.py`) walks the shared dir
  and reconstructs a chronologically merged timeline per room.

## ID model — what joins the dumps together?

Two existing IDs are reused; **no new ID is introduced**:

| ID | Where it lives | Cross-component? |
|---|---|---|
| `bootstrap_room` (uint64) | router generates → injects into prefill+decode HTTP body → both Python servers store on `Req` | **Yes** — primary join key (PD disagg only) |
| OpenTelemetry `trace_id` | propagated via `traceparent` HTTP header | Yes — orthogonal, plug-in OTel for live timeline |

The dump filename embeds `bootstrap_room` so offline join is just a
glob:

```
20260519/router@host1__1234567890__abc.json
20260519/prefill@host2__1234567890__abc.pkl
20260519/decode@host3__1234567890__notified_by_prefill.pkl
```

## Architecture

```
                                  ┌─ HTTP /fault-log/notify ─┐
                                  ▼                           │
   ┌──────────────────────┐   register_peer (header)   ┌──────┴──────────┐
   │   Router (Rust)      │ ──────────────────────────► │ Prefill (Python)│
   │ per-room ring buffer │                             │ per-room buffer │
   │ on error: dump+notify│ ◄────── notify ─────────── │ on error: dump+notify│
   └─────────┬────────────┘                             └───────┬─────────┘
             │ notify                                            │
             ▼                                                   │
       ┌─────────────────────────┐                               │
       │  Decode (Python)        │ ◄─────── notify ──────────────┘
       │  per-room ring buffer   │           (via mooncake bootstrap)
       │  on error: dump+notify  │
       └─────────────────────────┘
                             │
                             ▼
                     `<fault-log-dir>/YYYYMMDD/<source>__<room>__<rid>.{pkl,json}`
                             │
                             ▼
                  `python tools/join_fault_logs.py <dir>`
```

### Per-component event capture

| Component | Where events come from |
|---|---|
| Router (Rust) | Hook in `pd_router.rs` at `inject_bootstrap` (writes `dispatch` event with prefill+decode URLs) |
| Prefill / Decode tokenizer_manager | `_send_one_request` writes `api_dispatch`; the finish path writes `api_abort` on error |
| Prefill / Decode scheduler | `prepare_abort` emits `abort` with caller file:line; events ride back via `BatchTokenIDOutput.time_stats[i].events` |
| Mooncake (planned, not in this MVP) | Background threads keyed by `bootstrap_room` |

The per-room buffer is filled IN THIS PROCESS. Buffer events are
distinct from `req.time_stats.events` — the latter rides with the
request object and is naturally serialized between scheduler
processes; the former survives request lifetime so peer-notify can
recover events even after the local request is gone.

## Startup flags

### Python (prefill / decode)

```
--enable-fault-driven-log                # OFF by default
--fault-log-dir /shared/fault_log        # default /tmp/sglang_fault_log
--fault-log-error-whitelist 'bootstrap timeout' 'KVPoll.Failed' 500
--fault-log-token <shared_secret>        # auth for /fault-log/notify
--fault-log-router-url http://router:30000  # optional; auto-learned via header
--fault-log-room-buffer-max-rooms 5000
--fault-log-room-buffer-max-events-per-room 200
--fault-log-room-buffer-ttl-secs 300
```

### Rust router (sgl-model-gateway / smg)

```
--enable-fault-driven-log
--fault-log-dir /shared/fault_log
--fault-log-token <shared_secret>
--fault-log-self-url http://router:30000   # default: http://<host>:<port>
--fault-log-error-whitelist 'no healthy worker' 500
--fault-log-max-rooms 5000
--fault-log-max-events-per-room 200
--fault-log-ttl-secs 300
```

### Recommended deployment values

* **Token**: generate once per cluster, deploy via K8s `Secret` mounted
  as env var `SGLANG_FAULT_LOG_TOKEN` on all three components. Rotate
  on a quarterly cadence.
* **Dir**: shared mountpoint (e.g. `/mnt/observability/fault_log`)
  mounted by router and all Python servers.
* **TTL**: 300s is plenty for typical request lifetimes; bump to
  900s if you have very long context generations.

## Decisions captured

| Decision | Setting |
|---|---|
| Notify dispatch | **Async fire-and-forget**, 1 retry then blacklist peer for 5s |
| Storage layout | **Shared mountpoint**, flat per-day dirs |
| Notification topology | **Two-way** between router and each Python peer (router↔prefill, router↔decode); prefill↔decode skipped because they don't naturally know each other's HTTP URLs |
| Auth | **Required** via `X-Sgl-Fault-Log-Token` header. Empty token disables auth (dev only). |
| Service-discovery fallback | If a Python component can't recover the router URL (header missing AND `--fault-log-router-url` unset), it logs the abort locally only and skips notify, with the failure recorded in the events as `no_router_url_known`. |
| Successful requests | Never persisted. |
| Whitelist semantics | Empty / unset = capture all errors. Non-empty: case-insensitive substring match against the error message AND exact match against the status code. Any whitelist entry hitting either dimension wins. |

## On-disk dump shape

### Python (.pkl)

`pickle.load(f)` returns:

```python
{
    "server_args": <ServerArgs>,
    "requests": [
        {
            "rid": "abc-123",
            "host": "prefill-0",
            "pid": 12345,
            "bootstrap_room": 1234567890,
            "disagg_mode": "prefill",
            "source": "prefill",
            "obj": <GenerateReqInput>,           # original object, replayable
            "out_dict": {...},                   # what was streamed to client
            "finish_reason": {"type": "abort", "message": ..., "status_code": 500},
            "error_message": "...",
            "status_code": 500,
            "created_time": 1747641234.567,
            "finished_time": 1747641239.123,
            "events": [
                ("api_server", "tokenize_done", 0.001, {...}),
                ("scheduler",  "abort",         4.556, {"caller": "prefill.py:309", ...}),
                ("room_buffer","api_dispatch",  0.0023, {...}),
            ],
            "api_time_stats": <APIServerReqTimeStats>,
            "scheduler_time_stats": <SchedulerReqTimeStats>,
            "notified_by": None | {"peer_source": "router@host1", "peer_dump_path": "...", "received_at": ...},
        }
    ],
}
```

### Router (.json)

Plain JSON file (no pickle):

```json
{
  "schema": "sglang.fault_log/v1",
  "source": "router@host1",
  "host": "host1",
  "bootstrap_room": 1234567890,
  "rid": "abc-123",
  "error_message": "no healthy worker",
  "status_code": 503,
  "events": [
    {"stage": "dispatch", "ts": 1747641234.012,
     "attrs": {"prefill": "http://...", "decode": "http://...", "route": "/v1/chat/completions"}}
  ],
  "ts_unix": 1747641239.456,
  "triggered_by": {"kind": "local_error"}
}
```

## Offline join

```
python tools/join_fault_logs.py /shared/fault_log
python tools/join_fault_logs.py /shared/fault_log --room 1234567890
python tools/join_fault_logs.py /shared/fault_log --since 2026-05-19 --json out.json
```

The tool walks the directory, groups by `bootstrap_room`, and emits
either a human-readable timeline (default) or a structured JSON for
ingestion into a downstream pipeline.

## What's covered vs. what's not

| Failure surface | Captured? |
|---|---|
| Router observes 5xx from prefill / decode | ✅ Router buffer has `dispatch`; on Python peer notify, router dumps |
| Router can't reach worker / no healthy worker | ✅ Error path has no `bootstrap_room` (dispatch hasn't happened) — captured by router's plain `tracing` logs only; not in the room-keyed dump |
| Router stream-break to client mid-response | ⚠️ Caught only if router has hooks at the streaming layer (not in this MVP). Python servers will still dump if they hit a broken pipe. |
| Prefill input length > limit | ✅ `prepare_abort` → emit, dump, notify router |
| Prefill / decode KVPoll.Failed | ✅ Same path |
| Prefill forward NaN | ✅ `prepare_abort` site at scheduler covers it |
| Decode prebuilt failure | ✅ `prepare_abort` at decode |
| Mooncake background-thread failures (RDMA, transfer corruption) | ⚠️ Surfaces only when `prepare_abort` sees `KVPoll.Failed`. The threads themselves don't yet emit fine-grained stage events into the room buffer — see "Future work". |
| OOM / segfault before any abort | ❌ Process dies before dump can run; covered by `--crash-dump-folder` (existing upstream feature, complementary) |
| Hang / deadlock (no error fires) | ❌ Not detected; would need a watchdog |

## Performance

| Path | Cost (off) | Cost (on, success) | Cost (on, error) |
|---|---|---|---|
| `_send_one_request` | unchanged | + ~250 ns (one buffer.emit) | same |
| Scheduler `prepare_abort` | unchanged | n/a (success path doesn't call) | + ~1 µs (sys._getframe + emit) |
| Tokenizer finish path | + 1 attribute read | + 1 isinstance + dict lookup | + 1 pkl write (async, off-thread) + 1 HTTP POST per peer |
| Router dispatch | unchanged | + ~250 ns (one buffer.emit + add_peer) | + 1 JSON write (async) + 1 HTTP POST per peer |
| Pickle of `time_stats` | unchanged | empty-list field, < 50 bytes | events list, typically < 5 KB |

The pickle path keeps the legacy "no `events` field is sent when both
the events list and `enable_metrics` are empty" behavior intact, so
there's no on-the-wire bloat for healthy traffic.

## Auth

`X-Sgl-Fault-Log-Token` header is checked on every inbound
`/fault-log/notify`:

* Empty / unset token on this server: auth disabled, all requests
  accepted (only safe in single-tenant clusters with NetworkPolicy).
* Set token: incoming requests must carry an exact match. Comparison
  is constant-time. Mismatches → HTTP 401.

When the feature is enabled but the token isn't, the components log a
WARNING at startup and proceed with auth disabled.

## Future work

1. **Mooncake background-thread instrumentation.** The mooncake
   `bootstrap_thread`, `transfer_worker`, and `decode_thread` run in
   the scheduler process and are keyed only by `bootstrap_room`. To
   capture their events:
   - Add a per-process `RoomEventBuffer` instance shared between the
     scheduler and the mooncake threads (via a module-level global in
     `disaggregation/utils.py`).
   - Drain that buffer for each room when the request leaves the
     scheduler, merging into `req.time_stats.events` before pickle.
   - This would cover RDMA transfer chunk start/end, sync_status
     callbacks, and prefill/decode response tracking.
   Estimated work: ~80 lines, 1 day.

2. **Hang / deadlock watchdog.** A periodic task in the
   tokenizer_manager could scan `rid_to_state` for requests whose
   last event is older than N seconds (configurable) and proactively
   trigger the fault log path. This would catch the
   "request never errors but also never finishes" failure mode.

3. **Router stream-break instrumentation.** The router's streaming
   pipeline (`create_streaming_response`) doesn't currently emit on
   client-disconnect. A wrapper around the SSE stream that emits
   `stream_break` would close this gap.

4. **OpenTelemetry bridge.** When OTel is enabled cluster-wide
   (`SGLANG_TRACE_LEVEL=3` on Python, OTLP exporter on Rust), the
   fault log can additionally write a span with the same room+events
   so the failure shows up in Jaeger/Tempo automatically.

## Migration notes

* The feature is **strictly additive**. Default behavior is unchanged.
* Existing `--dump-requests-folder` / `--crash-dump-folder` continue to
  work and are complementary: those capture **all** finished requests
  / process-crash state; fault-driven log captures **only failed**
  requests but with much richer per-stage events.
* `req.time_stats.events` is a new field on `ReqTimeStatsBase`. It
  gets pickled when non-empty; all existing time_stats consumers
  ignore unknown attributes, so the change is backward-compatible
  with mixed-version clusters during rolling deploys.

## Quick verification recipe

```bash
# 1. Start a fault server (or use existing prefill/decode):
python -m sglang.launch_server \
    --disaggregation-mode prefill \
    --enable-fault-driven-log \
    --fault-log-dir /tmp/fault \
    --fault-log-token mytoken123

# 2. Send a request that will fail (e.g. input too long):
curl http://localhost:30000/generate -d '{"text": "...10x context...", ...}'

# 3. Check the dump:
ls /tmp/fault/$(date +%Y%m%d)/
python tools/join_fault_logs.py /tmp/fault
```
