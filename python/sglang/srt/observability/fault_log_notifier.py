# Copyright 2023-2026 SGLang Team
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# ==============================================================================
"""Async fire-and-forget HTTP notifier for cross-component fault logs.

Whenever a server (router / prefill / decode) finishes writing a fault
log for ``bootstrap_room=R``, it asks this notifier to POST
``/fault-log/notify`` on each of its peers so they too dump their local
view of room ``R``.

Properties
----------

* **Fully async**: returns immediately, runs the POST in the background
  via ``asyncio.create_task`` (one shared aiohttp session reused across
  all peers; falls back to per-call requests if aiohttp is not
  available).
* **Best-effort**: 1 retry on connection error, then give up. A failed
  notify never raises into the hot path — it's logged at WARNING level
  with the room id so operators can spot patterns.
* **Authenticated**: each request carries
  ``X-Sgl-Fault-Log-Token: <shared_secret>``. The receiver rejects
  bodies without a matching token. If no token is configured anywhere,
  notifies are skipped entirely (local dumps still happen).

The same FaultLogNotifier instance can serve multiple peers; it stores
peer URLs lazily so the router can register a peer URL the first time
it dispatches to a new prefill/decode.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import threading
import time
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlsplit, urlunsplit

logger = logging.getLogger(__name__)


# Header used by both directions. Kept short to minimise per-request bytes.
FAULT_LOG_TOKEN_HEADER = "X-Sgl-Fault-Log-Token"
# Header injected by the router on dispatch so prefill/decode can call back.
ROUTER_URL_HEADER = "X-Sgl-Router-Fault-Log-Url"
# Notify endpoint path (suffix appended to a peer base URL).
NOTIFY_PATH = "/fault-log/notify"


def _normalize_peer_url(raw: str) -> Optional[str]:
    """Coerce a peer base URL into ``scheme://host:port`` form.

    Accepts ``http://1.2.3.4:9000``, ``1.2.3.4:9000``, ``1.2.3.4``,
    or a full URL with a path. Returns None for inputs we can't parse.
    """
    if not raw:
        return None
    raw = raw.strip()
    if "://" not in raw:
        raw = "http://" + raw
    try:
        parts = urlsplit(raw)
        if not parts.netloc:
            return None
        # Strip path/query/fragment, keep scheme + netloc.
        return urlunsplit((parts.scheme, parts.netloc, "", "", ""))
    except Exception:
        return None


class FaultLogNotifier:
    """Owns a list of peer base URLs and a shared HTTP client.

    Single instance per server process; thread-safe.
    """

    def __init__(
        self,
        token: Optional[str],
        source_label: str,
        timeout_secs: float = 2.0,
        retries: int = 1,
    ):
        self.token: str = (token or "").strip()
        self.source_label: str = source_label or socket.gethostname()
        self.timeout_secs: float = float(timeout_secs)
        self.retries: int = max(0, int(retries))
        # Peer URL set + lock. We lazily register peers on first sight.
        self._peers: Set[str] = set()
        self._lock = threading.Lock()
        # Whether outbound notifies are allowed.
        self._enabled: bool = True
        # Per-peer cooldown so a flapping peer doesn't get hammered.
        self._peer_blacklist: Dict[str, float] = {}
        self._blacklist_secs: float = 5.0
        # Lazy aiohttp session; constructed inside the running loop.
        self._aiohttp_session = None
        self._aiohttp_available: Optional[bool] = None

    # ------------------------------------------------------------------ #

    def is_active(self) -> bool:
        """A notifier is active iff there's at least one known peer.

        Local-dump-only deployments (single node, no token, no router)
        never become active and the notify path is a true no-op.
        """
        if not self._enabled:
            return False
        with self._lock:
            return len(self._peers) > 0

    def disable(self) -> None:
        self._enabled = False

    def add_peer(self, url: Optional[str]) -> Optional[str]:
        """Register a peer URL (idempotent). Returns the normalised URL or None."""
        norm = _normalize_peer_url(url) if url else None
        if norm is None:
            return None
        with self._lock:
            self._peers.add(norm)
        return norm

    def remove_peer(self, url: str) -> None:
        norm = _normalize_peer_url(url)
        if norm is None:
            return
        with self._lock:
            self._peers.discard(norm)

    def list_peers(self) -> List[str]:
        with self._lock:
            return sorted(self._peers)

    # ------------------------------------------------------------------ #

    def authenticate(self, request_token: Optional[str]) -> bool:
        """Server-side check: validate an incoming notify's token.

        Returns True if our token is unset (auth disabled), or if the
        request token matches exactly. Constant-time comparison to avoid
        timing oracles, even though the secret is shared and short.
        """
        if not self.token:
            # No token configured = auth disabled. Useful for trusted
            # single-tenant clusters where NetworkPolicy already isolates.
            return True
        rt = (request_token or "").strip()
        if len(rt) != len(self.token):
            return False
        diff = 0
        for a, b in zip(rt, self.token):
            diff |= ord(a) ^ ord(b)
        return diff == 0

    # ------------------------------------------------------------------ #

    def notify_room_failure(
        self,
        room: int,
        error_summary: str,
        local_dump_path: Optional[str],
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Fire-and-forget notify to every registered peer.

        Schedules an asyncio task on the running loop. Safe to call from
        async context (typical: tokenizer_manager.dump_failed_request).
        """
        if not self._enabled:
            return
        peers = self.list_peers()
        if not peers:
            return
        body = {
            "room": int(room) if room is not None else None,
            "error_summary": str(error_summary)[:1024],
            "source": self.source_label,
            "local_dump_path": local_dump_path,
            "ts": time.time(),
        }
        if extra:
            body["extra"] = extra
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Caller is on a non-async thread (e.g. mooncake bg thread).
            # Spin up a tiny worker thread to do the notify.
            threading.Thread(
                target=self._notify_blocking_all,
                args=(peers, body),
                daemon=True,
                name="FaultLogNotifyThread",
            ).start()
            return
        loop.create_task(self._notify_async_all(peers, body))

    # ------------------------------------------------------------------ #
    #                       Internal: aiohttp / requests                  #
    # ------------------------------------------------------------------ #

    async def _notify_async_all(
        self, peers: List[str], body: Dict[str, Any]
    ) -> None:
        # Filter blacklisted peers.
        now = time.monotonic()
        live_peers: List[str] = []
        for p in peers:
            until = self._peer_blacklist.get(p, 0.0)
            if until > now:
                continue
            live_peers.append(p)
        if not live_peers:
            return
        await asyncio.gather(
            *[self._notify_async_one(p, body) for p in live_peers],
            return_exceptions=True,
        )

    async def _notify_async_one(self, peer: str, body: Dict[str, Any]) -> None:
        url = peer.rstrip("/") + NOTIFY_PATH
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers[FAULT_LOG_TOKEN_HEADER] = self.token
        if self._aiohttp_available is None:
            try:
                import aiohttp  # noqa: F401

                self._aiohttp_available = True
            except Exception:
                self._aiohttp_available = False
        for attempt in range(self.retries + 1):
            try:
                if self._aiohttp_available:
                    await self._post_aiohttp(url, headers, body)
                else:
                    # Fall back to requests in a thread.
                    await asyncio.to_thread(
                        self._post_requests, url, headers, body
                    )
                return
            except Exception as e:
                if attempt >= self.retries:
                    logger.warning(
                        "fault-log notify to %s failed (room=%s): %r; "
                        "blacklisting peer for %.0fs",
                        url,
                        body.get("room"),
                        e,
                        self._blacklist_secs,
                    )
                    self._peer_blacklist[peer] = (
                        time.monotonic() + self._blacklist_secs
                    )

    async def _post_aiohttp(
        self, url: str, headers: Dict[str, str], body: Dict[str, Any]
    ) -> None:
        import aiohttp

        if self._aiohttp_session is None or self._aiohttp_session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout_secs)
            self._aiohttp_session = aiohttp.ClientSession(timeout=timeout)
        async with self._aiohttp_session.post(
            url, headers=headers, data=json.dumps(body).encode("utf-8")
        ) as resp:
            # We don't act on the response body; just consume it so the
            # connection is returned to the pool cleanly.
            await resp.read()
            if resp.status >= 400:
                raise RuntimeError(f"peer returned HTTP {resp.status}")

    def _post_requests(
        self, url: str, headers: Dict[str, str], body: Dict[str, Any]
    ) -> None:
        import requests

        resp = requests.post(
            url, headers=headers, json=body, timeout=self.timeout_secs
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"peer returned HTTP {resp.status_code}")

    def _notify_blocking_all(
        self, peers: List[str], body: Dict[str, Any]
    ) -> None:
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers[FAULT_LOG_TOKEN_HEADER] = self.token
        for peer in peers:
            url = peer.rstrip("/") + NOTIFY_PATH
            for attempt in range(self.retries + 1):
                try:
                    self._post_requests(url, headers, body)
                    break
                except Exception as e:
                    if attempt >= self.retries:
                        logger.warning(
                            "fault-log notify (sync) to %s failed (room=%s): %r",
                            url,
                            body.get("room"),
                            e,
                        )
                        self._peer_blacklist[peer] = (
                            time.monotonic() + self._blacklist_secs
                        )

    async def aclose(self) -> None:
        if self._aiohttp_session is not None:
            try:
                await self._aiohttp_session.close()
            except Exception:
                pass
            self._aiohttp_session = None
