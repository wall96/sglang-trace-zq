// Copyright 2023-2026 SGLang Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// =============================================================================

//! Fault-driven per-request logging for the router.
//!
//! Mirrors the Python-side feature in `sglang.srt.observability.room_event_buffer`
//! and `sglang.srt.observability.fault_log_notifier`:
//!
//! * Per-`bootstrap_room` ring buffer with TTL eviction. The buffer
//!   outlives the request future, so when an inbound `/fault-log/notify`
//!   arrives from a Python peer (prefill or decode) after the router has
//!   already streamed the response back to the client, we can still
//!   recover the router's view of that request and dump it to disk.
//!
//! * Async dump-on-error: when the router itself observes an error
//!   (no healthy worker, dispatch failure, stream break, ...), it
//!   serializes the room's events to a JSON file under the shared
//!   fault-log directory and fires fire-and-forget HTTP POSTs to
//!   `/fault-log/notify` on every peer it knows about (the prefill
//!   and decode that handled this request, if applicable).
//!
//! * Authenticated: the inbound endpoint validates the
//!   `X-Sgl-Fault-Log-Token` header before accepting a notify body.
//!
//! Disabled by default. When `FaultLogConfig::enabled` is false the
//! global state is `None`, every public function is a cheap early
//! return, and behavior is identical to upstream.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};

/// HTTP header used to authenticate inbound and outbound notifies.
pub const FAULT_LOG_TOKEN_HEADER: &str = "x-sgl-fault-log-token";
/// Header injected by the router on dispatch so prefill/decode can
/// learn the router's notify URL without separate config.
pub const ROUTER_URL_HEADER: &str = "x-sgl-router-fault-log-url";
/// Endpoint suffix appended to peer base URLs.
pub const NOTIFY_PATH: &str = "/fault-log/notify";

const DEFAULT_GC_INTERVAL_DIVISOR: f64 = 5.0;

// =============================================================================
//                                Config & state
// =============================================================================

#[derive(Clone, Debug, Default)]
pub struct FaultLogConfig {
    pub enabled: bool,
    pub dir: PathBuf,
    pub token: Option<String>,
    /// Component label written into dump filenames + notify bodies.
    pub source_label: String,
    /// Cap on total tracked rooms.
    pub max_rooms: usize,
    /// Cap on events kept per room.
    pub max_events_per_room: usize,
    /// Idle TTL before a room is GC'd from the buffer.
    pub ttl: Duration,
    /// Optional case-insensitive substring whitelist; empty = match all.
    pub error_whitelist: Vec<String>,
    /// Notify timeout per HTTP POST.
    pub notify_timeout: Duration,
    /// Notify retry count (after first attempt).
    pub notify_retries: u32,
    /// Router's externally-reachable base URL (scheme://host:port). Sent
    /// in the X-Sgl-Router-Fault-Log-Url header on every dispatch so
    /// prefill/decode learn where to call back. None = downstream peers
    /// must be configured statically with --fault-log-router-url.
    pub self_url: Option<String>,
}

impl FaultLogConfig {
    pub fn default_disabled() -> Self {
        Self {
            enabled: false,
            dir: PathBuf::from("/tmp/sglang_fault_log"),
            token: None,
            source_label: "router@unknown".to_string(),
            max_rooms: 5000,
            max_events_per_room: 200,
            ttl: Duration::from_secs(300),
            error_whitelist: Vec::new(),
            notify_timeout: Duration::from_secs(2),
            notify_retries: 1,
            self_url: None,
        }
    }

    pub fn matches_error(&self, message: &str, status_code: Option<u16>) -> bool {
        if self.error_whitelist.is_empty() {
            return true;
        }
        let m = message.to_ascii_lowercase();
        let sc = status_code.map(|x| x.to_string()).unwrap_or_default();
        for pat in &self.error_whitelist {
            let pat_lc = pat.to_ascii_lowercase();
            if !pat_lc.is_empty() && m.contains(&pat_lc) {
                return true;
            }
            if !sc.is_empty() && pat == &sc {
                return true;
            }
        }
        false
    }
}

/// One event in a room's ring buffer.
#[derive(Clone, Debug, Serialize)]
pub struct FaultEvent {
    pub stage: String,
    /// Wallclock unix-time seconds (f64) so consumers can join across
    /// heterogeneous clocks at human-readable resolution.
    pub ts: f64,
    pub attrs: Value,
}

#[derive(Default)]
struct RoomState {
    events: VecDeque<FaultEvent>,
    last_touched: Option<Instant>,
}

/// Combined buffer + config + outbound HTTP client.
pub struct FaultLogState {
    pub config: FaultLogConfig,
    rooms: Mutex<HashMap<u64, RoomState>>,
    /// Set of peer base URLs (router knows prefill / decode URLs from
    /// dispatch; prefill/decode call back to router via this set).
    peers: Mutex<Vec<String>>,
    last_gc: Mutex<Option<Instant>>,
    gc_interval: Duration,
    client: reqwest::Client,
    /// Per-peer cooldown so a flapping peer doesn't get hammered.
    peer_blacklist: Mutex<HashMap<String, Instant>>,
    blacklist_window: Duration,
}

impl FaultLogState {
    pub fn new(config: FaultLogConfig) -> Self {
        let gc_interval = Duration::from_secs_f64(
            (config.ttl.as_secs_f64() / DEFAULT_GC_INTERVAL_DIVISOR).max(1.0),
        );
        let client = reqwest::Client::builder()
            .timeout(config.notify_timeout)
            .build()
            .expect("reqwest client build");
        Self {
            config,
            rooms: Mutex::new(HashMap::new()),
            peers: Mutex::new(Vec::new()),
            last_gc: Mutex::new(None),
            gc_interval,
            client,
            peer_blacklist: Mutex::new(HashMap::new()),
            blacklist_window: Duration::from_secs(5),
        }
    }

    /// Router's own externally-reachable base URL (configured at init).
    pub fn self_url(&self) -> Option<&str> {
        self.config.self_url.as_deref()
    }

    /// Register a peer URL (idempotent). Idempotent because the router
    /// learns peers on every dispatch and we don't want a growing list.
    pub fn add_peer(&self, raw: &str) {
        let Some(norm) = normalize_url(raw) else {
            return;
        };
        let mut peers = self.peers.lock();
        if !peers.contains(&norm) {
            peers.push(norm);
        }
    }

    pub fn list_peers(&self) -> Vec<String> {
        self.peers.lock().clone()
    }

    // ------------------------------------------------------------------ Hot path

    pub fn emit(&self, room: u64, stage: &str, attrs: Value) {
        let now = Instant::now();
        let event = FaultEvent {
            stage: stage.to_string(),
            ts: unix_time_seconds(),
            attrs,
        };
        let mut rooms = self.rooms.lock();
        let state = rooms.entry(room).or_insert_with(RoomState::default);
        if state.events.len() >= self.config.max_events_per_room {
            state.events.pop_front();
        }
        state.events.push_back(event);
        state.last_touched = Some(now);

        if rooms.len() > self.config.max_rooms {
            // LRU eviction: pop the room with the oldest last_touched.
            let oldest = rooms
                .iter()
                .min_by_key(|(_, s)| s.last_touched.unwrap_or(now))
                .map(|(k, _)| *k);
            if let Some(k) = oldest {
                rooms.remove(&k);
            }
        }
        drop(rooms);
        self.maybe_gc(now);
    }

    fn maybe_gc(&self, now: Instant) {
        {
            let mut last = self.last_gc.lock();
            if let Some(t) = *last {
                if now.duration_since(t) < self.gc_interval {
                    return;
                }
            }
            *last = Some(now);
        }
        let cutoff = now - self.config.ttl;
        let mut rooms = self.rooms.lock();
        rooms.retain(|_, s| s.last_touched.map(|t| t >= cutoff).unwrap_or(false));
    }

    pub fn drain_room(&self, room: u64) -> Vec<FaultEvent> {
        let mut rooms = self.rooms.lock();
        rooms
            .remove(&room)
            .map(|s| s.events.into_iter().collect())
            .unwrap_or_default()
    }

    pub fn snapshot_room(&self, room: u64) -> Vec<FaultEvent> {
        let rooms = self.rooms.lock();
        rooms
            .get(&room)
            .map(|s| s.events.iter().cloned().collect())
            .unwrap_or_default()
    }

    // ------------------------------------------------------------------ Auth

    pub fn authenticate(&self, header_token: Option<&str>) -> bool {
        let Some(expected) = self.config.token.as_deref() else {
            return true; // No token configured = auth disabled.
        };
        if expected.is_empty() {
            return true;
        }
        let Some(got) = header_token else {
            return false;
        };
        constant_time_eq(expected.as_bytes(), got.trim().as_bytes())
    }

    // ------------------------------------------------------------------ Dump

    /// Build a payload for ``room`` and write it asynchronously to disk.
    /// Returns the filesystem path written (best-effort).
    pub async fn dump_local(
        &self,
        room: u64,
        rid: Option<&str>,
        error_message: &str,
        status_code: Option<u16>,
        triggered_by: TriggerKind,
    ) -> Option<PathBuf> {
        if !self.config.enabled {
            return None;
        }
        let events = match triggered_by {
            // Local error: drain, no peer-notify dump expected for this room here.
            TriggerKind::LocalError => self.drain_room(room),
            // Remote-notified: also drain (frees room storage); we won't
            // get more events for it on this side either way.
            TriggerKind::PeerNotify { .. } => self.drain_room(room),
        };

        let date = chrono::Local::now().format("%Y%m%d").to_string();
        let dir = self.config.dir.join(&date);
        if let Err(e) = fs::create_dir_all(&dir).await {
            warn!(
                target: "fault_log",
                "failed to mkdir {}: {:?}",
                dir.display(),
                e,
            );
            return None;
        }

        let safe_rid = sanitize_rid(rid.unwrap_or("unknown"));
        let suffix = match &triggered_by {
            TriggerKind::LocalError => safe_rid.clone(),
            TriggerKind::PeerNotify { peer_source } => {
                format!("notified_by_{}", sanitize_rid(peer_source))
            }
        };
        let filename = dir.join(format!(
            "{}__{}__{}.json",
            sanitize_label(&self.config.source_label),
            room,
            suffix
        ));

        let body = json!({
            "schema": "sglang.fault_log/v1",
            "source": &self.config.source_label,
            "host": hostname_string(),
            "bootstrap_room": room,
            "rid": rid,
            "error_message": error_message,
            "status_code": status_code,
            "events": events,
            "ts_unix": unix_time_seconds(),
            "triggered_by": triggered_by.as_value(),
        });

        let bytes = serde_json::to_vec_pretty(&body).unwrap_or_else(|_| b"{}".to_vec());
        let path = filename.clone();
        // Spawn the actual write off the calling task so request-handling
        // futures aren't blocked on disk.
        tokio::spawn(async move {
            match fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .await
            {
                Ok(mut f) => {
                    if let Err(e) = f.write_all(&bytes).await {
                        warn!(target: "fault_log", "write {} failed: {:?}", path.display(), e);
                    }
                }
                Err(e) => {
                    warn!(target: "fault_log", "open {} failed: {:?}", path.display(), e);
                }
            }
        });
        Some(filename)
    }

    // ------------------------------------------------------------------ Notify

    /// Fire a notify to every known peer. Fire-and-forget: never awaits
    /// the responses, never propagates errors back.
    pub fn notify_peers(
        self: &Arc<Self>,
        room: u64,
        rid: Option<String>,
        error_summary: String,
        status_code: Option<u16>,
        local_dump_path: Option<PathBuf>,
    ) {
        if !self.config.enabled {
            return;
        }
        let peers = self.list_peers();
        if peers.is_empty() {
            return;
        }
        let body = json!({
            "room": room,
            "error_summary": error_summary,
            "source": &self.config.source_label,
            "local_dump_path": local_dump_path.as_ref().map(|p| p.display().to_string()),
            "ts": unix_time_seconds(),
            "extra": {
                "rid": rid,
                "status_code": status_code,
            },
        });
        let me = Arc::clone(self);
        tokio::spawn(async move {
            me.notify_peers_inner(peers, body).await;
        });
    }

    async fn notify_peers_inner(self: Arc<Self>, peers: Vec<String>, body: Value) {
        let now = Instant::now();
        let mut alive: Vec<String> = Vec::new();
        {
            let bl = self.peer_blacklist.lock();
            for p in &peers {
                if let Some(until) = bl.get(p) {
                    if *until > now {
                        continue;
                    }
                }
                alive.push(p.clone());
            }
        }
        if alive.is_empty() {
            return;
        }
        let body_bytes = match serde_json::to_vec(&body) {
            Ok(b) => b,
            Err(e) => {
                warn!(target: "fault_log", "serialize notify body failed: {:?}", e);
                return;
            }
        };
        let token = self.config.token.clone();
        let mut futs = Vec::new();
        for peer in alive {
            let url = format!("{}{}", peer.trim_end_matches('/'), NOTIFY_PATH);
            futs.push(self.notify_one(url, peer, token.clone(), body_bytes.clone()));
        }
        futures::future::join_all(futs).await;
    }

    async fn notify_one(
        &self,
        url: String,
        peer_key: String,
        token: Option<String>,
        body: Vec<u8>,
    ) {
        let mut last_err: Option<String> = None;
        let attempts = self.config.notify_retries.saturating_add(1);
        for attempt in 0..attempts {
            let mut req = self
                .client
                .post(&url)
                .header(reqwest::header::CONTENT_TYPE, "application/json");
            if let Some(t) = token.as_deref() {
                req = req.header(FAULT_LOG_TOKEN_HEADER, t);
            }
            match req.body(body.clone()).send().await {
                Ok(r) if r.status().is_success() => {
                    debug!(target: "fault_log", "notify {} -> {}", url, r.status());
                    return;
                }
                Ok(r) => {
                    last_err = Some(format!("HTTP {}", r.status()));
                }
                Err(e) => {
                    last_err = Some(format!("{:?}", e));
                }
            }
            if attempt + 1 < attempts {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        warn!(
            target: "fault_log",
            "notify to {} failed: {:?}; blacklisting peer for {:?}",
            url, last_err, self.blacklist_window
        );
        let mut bl = self.peer_blacklist.lock();
        bl.insert(peer_key, Instant::now() + self.blacklist_window);
    }
}

/// Why was this dump triggered?
#[derive(Clone, Debug)]
pub enum TriggerKind {
    LocalError,
    PeerNotify { peer_source: String },
}

impl TriggerKind {
    fn as_value(&self) -> Value {
        match self {
            TriggerKind::LocalError => json!({"kind": "local_error"}),
            TriggerKind::PeerNotify { peer_source } => json!({
                "kind": "peer_notify",
                "peer_source": peer_source,
            }),
        }
    }
}

// =============================================================================
//                              Global handle
// =============================================================================

static FAULT_LOG: OnceLock<Arc<FaultLogState>> = OnceLock::new();

/// Initialize the global fault-log state. Call once at startup, before
/// any other fault-log entry point. Subsequent calls are no-ops.
pub fn init(config: FaultLogConfig) -> Arc<FaultLogState> {
    let state = Arc::new(FaultLogState::new(config));
    let _ = FAULT_LOG.set(state.clone());
    if let Some(installed) = FAULT_LOG.get() {
        if installed.config.enabled {
            info!(
                target: "fault_log",
                "fault-log enabled: dir={}, source={}, peers_at_init={}, token_configured={}",
                installed.config.dir.display(),
                installed.config.source_label,
                installed.list_peers().len(),
                installed.config.token.is_some(),
            );
        }
        return installed.clone();
    }
    state
}

/// Get the global state, if initialised. Returns None when the feature
/// is off OR `init()` was not called yet (during very early startup).
pub fn global() -> Option<Arc<FaultLogState>> {
    FAULT_LOG.get().cloned()
}

/// Convenience: emit an event for `room` if the feature is enabled.
pub fn emit_for_room(room: Option<u64>, stage: &str, attrs: Value) {
    let Some(state) = global() else {
        return;
    };
    if !state.config.enabled {
        return;
    }
    let Some(r) = room else {
        return;
    };
    state.emit(r, stage, attrs);
}

/// Convenience: register a peer URL (used when the router learns
/// prefill/decode URLs at dispatch time).
pub fn add_peer(url: &str) {
    if let Some(s) = global() {
        if s.config.enabled {
            s.add_peer(url);
        }
    }
}

// =============================================================================
//                         Inbound notify body shape
// =============================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct InboundNotify {
    pub room: u64,
    pub error_summary: Option<String>,
    pub source: Option<String>,
    pub local_dump_path: Option<String>,
    pub ts: Option<f64>,
    pub extra: Option<Value>,
}

/// Process an inbound /fault-log/notify body. Returns Ok with the dump
/// path on success, Err with an HTTP-friendly status code on failure.
pub async fn handle_inbound_notify(
    state: &Arc<FaultLogState>,
    token_header: Option<&str>,
    body: InboundNotify,
) -> Result<Option<PathBuf>, (u16, String)> {
    if !state.config.enabled {
        // Politely accept so peers don't blacklist us during partial deploys.
        return Ok(None);
    }
    if !state.authenticate(token_header) {
        return Err((401, "unauthorized".to_string()));
    }
    let rid = body
        .extra
        .as_ref()
        .and_then(|e| e.get("rid"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let status_code = body
        .extra
        .as_ref()
        .and_then(|e| e.get("status_code"))
        .and_then(|v| v.as_u64())
        .map(|x| x as u16);
    let peer_source = body.source.unwrap_or_else(|| "unknown_peer".to_string());
    let path = state
        .dump_local(
            body.room,
            rid.as_deref(),
            body.error_summary.as_deref().unwrap_or(""),
            status_code,
            TriggerKind::PeerNotify { peer_source },
        )
        .await;
    Ok(path)
}

// =============================================================================
//                                Helpers
// =============================================================================

fn unix_time_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn hostname_string() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "unknown".to_string())
}

fn sanitize_rid(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn sanitize_label(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '@') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn normalize_url(raw: &str) -> Option<String> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    let with_scheme = if s.contains("://") {
        s.to_string()
    } else {
        format!("http://{}", s)
    };
    // Use url crate via reqwest::Url for robust parsing.
    let url = reqwest::Url::parse(&with_scheme).ok()?;
    let host = url.host_str()?;
    let scheme = url.scheme();
    let port = url.port_or_known_default();
    let mut out = format!("{}://{}", scheme, host);
    if let Some(p) = port {
        // Only suffix the port when it isn't the default for the scheme.
        if !((scheme == "http" && p == 80) || (scheme == "https" && p == 443)) {
            out.push_str(&format!(":{}", p));
        }
    }
    Some(out)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
