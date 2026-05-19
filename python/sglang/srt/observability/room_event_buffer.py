# Copyright 2023-2026 SGLang Team
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# ==============================================================================
"""Per-bootstrap_room ring buffer used by the fault-driven logging feature.

The buffer outlives the Req object so that:

1. When a peer (router / decode / prefill) sends an HTTP notify saying
   "room R failed on my side", this server can still recover its own
   in-process events for room R even if the local Req has already been
   freed.

2. Mooncake background threads (bootstrap_thread / transfer_worker /
   decode_thread) emit events keyed by bootstrap_room (they don't have
   the Req object), and the dump path can later merge those events
   with the per-Req `time_stats.events`.

Design constraints
------------------

* O(1) emit on the hot path (single dict lookup + deque append).
* Bounded memory: capped number of rooms AND capped events per room.
* TTL eviction so stale rooms don't pile up if a request leaks.
* Thread-safe (mooncake threads emit from non-asyncio threads).
* Zero overhead when fault-driven logging is disabled — callers should
  short-circuit on `server_args.enable_fault_driven_log` before they
  ever construct a buffer or call `emit_room`.

Each event is a 3-tuple ``(stage_name, perf_counter_ts, attrs_dict)``.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict, deque
from typing import Any, Deque, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


RoomEvent = Tuple[str, float, Dict[str, Any]]


class _RoomState:
    """One room's bounded event log + last-touched timestamp.

    Keeping this in a small class (instead of a tuple) lets us update
    last_touched in-place without rewriting the dict entry, which keeps
    the OrderedDict's LRU ordering meaningful.
    """

    __slots__ = ("events", "last_touched")

    def __init__(self, max_events: int):
        self.events: Deque[RoomEvent] = deque(maxlen=max_events)
        self.last_touched: float = time.monotonic()


class RoomEventBuffer:
    """Bounded per-room event ring buffer with TTL-based GC.

    Capacity policy:
      * At most ``max_rooms`` distinct rooms tracked. When the cap is
        reached, the oldest-touched room is evicted (LRU).
      * Each room keeps at most ``max_events_per_room`` events.
        Older events overflow the deque silently.
      * Idle rooms (no emit/touch within ``ttl_secs``) are GC'd lazily
        on emit/get/drain calls — there's no background thread.

    All public methods acquire a single coarse lock. Lock is held for
    O(1) work per call so contention is low even from many mooncake
    threads.
    """

    def __init__(
        self,
        max_rooms: int = 5000,
        max_events_per_room: int = 200,
        ttl_secs: float = 300.0,
    ):
        self._max_rooms = max(1, int(max_rooms))
        self._max_events_per_room = max(1, int(max_events_per_room))
        self._ttl_secs = max(1.0, float(ttl_secs))
        self._lock = threading.Lock()
        # OrderedDict gives us LRU eviction in O(1).
        self._rooms: "OrderedDict[int, _RoomState]" = OrderedDict()
        # Lightweight stats for visibility (tested via __repr__ in unit tests).
        self._evicted_lru_count: int = 0
        self._evicted_ttl_count: int = 0
        # GC bookkeeping: last time we walked the dict for TTL eviction.
        self._last_gc_at: float = time.monotonic()
        self._gc_interval_secs: float = max(1.0, ttl_secs / 5.0)

    # ------------------------------------------------------------------ #
    #                              Hot path                                #
    # ------------------------------------------------------------------ #

    def emit(self, room: Optional[int], stage: str, **attrs: Any) -> None:
        """Append an event for ``room``.

        ``room`` is allowed to be None (e.g. non-disagg mode); in that
        case we simply no-op rather than fall back to a global pseudo-
        room, because non-disagg requests already have the events in
        their own ``req.time_stats.events`` and don't need cross-
        component join.
        """
        if room is None:
            return
        try:
            ts = time.perf_counter()
            with self._lock:
                state = self._rooms.get(room)
                if state is None:
                    state = _RoomState(self._max_events_per_room)
                    self._rooms[room] = state
                    if len(self._rooms) > self._max_rooms:
                        # Drop the LRU entry. popitem(last=False) is O(1).
                        self._rooms.popitem(last=False)
                        self._evicted_lru_count += 1
                else:
                    # Move to MRU end so it's not immediately evicted.
                    self._rooms.move_to_end(room)
                state.events.append((stage, ts, attrs))
                state.last_touched = time.monotonic()
                self._maybe_gc_locked()
        except Exception as e:
            # Telemetry must never break the hot path.
            logger.warning("RoomEventBuffer.emit failed: %r", e)

    def touch(self, room: Optional[int]) -> None:
        """Refresh a room's last-touched time without adding an event.

        Useful for "I'm still working on this room" hints from places
        that don't have a useful event to log but want to extend TTL.
        """
        if room is None:
            return
        with self._lock:
            state = self._rooms.get(room)
            if state is not None:
                state.last_touched = time.monotonic()
                self._rooms.move_to_end(room)

    # ------------------------------------------------------------------ #
    #                            Read / drain                              #
    # ------------------------------------------------------------------ #

    def get(self, room: Optional[int]) -> List[RoomEvent]:
        """Return a snapshot copy of events for ``room`` (empty list if absent)."""
        if room is None:
            return []
        with self._lock:
            state = self._rooms.get(room)
            return list(state.events) if state is not None else []

    def drain(self, room: Optional[int]) -> List[RoomEvent]:
        """Return events for ``room`` AND remove the room from the buffer.

        Use this when the request has fully finished on this side and
        no more events for this room are expected — typically right
        before / after writing the fault log file.
        """
        if room is None:
            return []
        with self._lock:
            state = self._rooms.pop(room, None)
            return list(state.events) if state is not None else []

    def has(self, room: Optional[int]) -> bool:
        if room is None:
            return False
        with self._lock:
            return room in self._rooms

    # ------------------------------------------------------------------ #
    #                              GC                                      #
    # ------------------------------------------------------------------ #

    def _maybe_gc_locked(self) -> None:
        """Walk the dict once every gc_interval_secs and drop stale rooms.

        Caller must hold ``self._lock``. We bound walk frequency rather
        than walk on every emit — typical event rates are high (10k/s
        per process is realistic) and we only need the buffer to drain
        within minutes.
        """
        now = time.monotonic()
        if now - self._last_gc_at < self._gc_interval_secs:
            return
        self._last_gc_at = now
        cutoff = now - self._ttl_secs
        # OrderedDict iterates in insertion order which is approximately
        # touch order (move_to_end on update). Rooms at the front are
        # oldest. Stop at the first non-stale room.
        stale_rooms: List[int] = []
        for room, state in self._rooms.items():
            if state.last_touched > cutoff:
                break
            stale_rooms.append(room)
        for room in stale_rooms:
            self._rooms.pop(room, None)
            self._evicted_ttl_count += 1

    def force_gc(self) -> int:
        """Force a TTL sweep and return the number of rooms evicted."""
        with self._lock:
            before = len(self._rooms)
            self._last_gc_at = 0.0
            self._maybe_gc_locked()
            return before - len(self._rooms)

    # ------------------------------------------------------------------ #
    #                            Diagnostics                               #
    # ------------------------------------------------------------------ #

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "tracked_rooms": len(self._rooms),
                "max_rooms": self._max_rooms,
                "max_events_per_room": self._max_events_per_room,
                "ttl_secs": self._ttl_secs,
                "evicted_lru_count": self._evicted_lru_count,
                "evicted_ttl_count": self._evicted_ttl_count,
            }

    def __repr__(self) -> str:
        s = self.stats()
        return (
            f"RoomEventBuffer(rooms={s['tracked_rooms']}/{s['max_rooms']}, "
            f"ttl={s['ttl_secs']}s, lru_evicted={s['evicted_lru_count']}, "
            f"ttl_evicted={s['evicted_ttl_count']})"
        )
