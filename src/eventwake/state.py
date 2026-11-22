"""Client-owned subscription state, kept durably on disk.

The spec makes the client the owner of all subscription state: the cursor, the dedupe window and,
for webhooks, the signing secret. eventwake keeps it in one JSON file per state directory and
writes it atomically.

At-least-once to the agent: an accepted batch of events and the cursor that follows it are
committed in the *same* write. The cursor therefore never moves past an event that is not either
already delivered or waiting durably in ``pending``. A crash between accept and delivery replays
from ``pending`` on the next start, not from the server.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any

JsonObj = dict[str, Any]
SEEN_WINDOW = 2000
PENDING_CAP = 5000


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def subscription_key(endpoint_id: str, event: str, arguments: JsonObj) -> str:
    """Dedupe scope: one subscription is (server identity, event name, canonical arguments)."""
    raw = canonical_json({"server": endpoint_id, "event": event, "arguments": arguments})
    return "sub-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class Delivered:
    """One event ready for a sink."""

    key: str
    seq: int
    server: str
    event: str
    occurrence: JsonObj
    received_at: str

    def to_json(self) -> JsonObj:
        return {"key": self.key, "seq": self.seq, "server": self.server, "event": self.event,
                "occurrence": self.occurrence, "received_at": self.received_at}

    @classmethod
    def from_json(cls, raw: JsonObj) -> Delivered:
        return cls(str(raw["key"]), int(raw["seq"]), str(raw["server"]), str(raw["event"]),
                   dict(raw["occurrence"]), str(raw.get("received_at", "")))


class StateLocked(Exception):
    """Another eventwake process already owns this state directory."""


class Store:
    def __init__(self, directory: Path) -> None:
        self.dir = directory
        self.path = directory / "state.json"
        self._lock = threading.RLock()
        self._lockfile: IO[str] | None = None
        self.doc: JsonObj = self._load()

    # --- persistence ----------------------------------------------------------------------

    def _load(self) -> JsonObj:
        try:
            doc = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {"version": 1, "seq": 0, "subscriptions": {}, "pending": []}
        except (OSError, json.JSONDecodeError) as e:
            raise ValueError(f"{self.path}: unreadable state file: {e}") from e
        if not isinstance(doc, dict) or doc.get("version") != 1:
            raise ValueError(f"{self.path}: not an eventwake v1 state file")
        doc.setdefault("seq", 0)
        doc.setdefault("subscriptions", {})
        doc.setdefault("pending", [])
        return doc

    def _save(self) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".state.", dir=self.dir)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(self.doc, fh, indent=1, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.chmod(tmp, 0o600)  # webhook secrets live here
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def acquire(self) -> None:
        """Take the exclusive run lock for this directory (one daemon per state dir)."""
        self.dir.mkdir(parents=True, exist_ok=True)
        fh = open(self.dir / "run.lock", "a+", encoding="utf-8")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.close()
            raise StateLocked(f"another eventwake process is running on {self.dir}") from None
        fh.seek(0)
        fh.truncate()
        fh.write(str(os.getpid()))
        fh.flush()
        self._lockfile = fh

    def release(self) -> None:
        if self._lockfile is not None:
            fcntl.flock(self._lockfile.fileno(), fcntl.LOCK_UN)
            self._lockfile.close()
            self._lockfile = None

    @contextmanager
    def tx(self) -> Iterator[JsonObj]:
        with self._lock:
            yield self.doc
            self._save()

    # --- subscriptions ----------------------------------------------------------------------

    def sub(self, key: str) -> JsonObj:
        with self._lock:
            subs = self.doc["subscriptions"]
            rec = subs.get(key)
            if not isinstance(rec, dict):
                rec = subs[key] = {"cursor": None, "seen": [], "events_total": 0}
            out: JsonObj = rec
            return out

    def register(self, key: str, server: str, endpoint_id: str, event: str, arguments: JsonObj,
                 mode: str) -> JsonObj:
        with self.tx():
            rec = self.sub(key)
            rec.update({"server": server, "endpoint": endpoint_id, "event": event,
                        "arguments": arguments, "mode": mode})
            rec.pop("terminated", None)
        return rec

    def cursor(self, key: str) -> str | None:
        with self._lock:
            c = self.sub(key).get("cursor")
            return c if isinstance(c, str) else None

    def set_cursor(self, key: str, cursor: str | None, *, truncated: bool = False) -> None:
        """Persist a watermark that carried no events (heartbeat, empty poll, refresh response).

        ``cursor`` absent or null means the event type has no replay: nothing to persist, and the
        next connection starts from now (the spec forbids replaying from a null cursor).
        """
        with self._lock:
            rec = self.sub(key)
            changed = rec.get("cursor") != cursor or truncated
            if not changed:
                return
            with self.tx():
                rec["cursor"] = cursor
                if truncated:
                    rec["gaps"] = int(rec.get("gaps", 0)) + 1
                    rec["last_gap_at"] = now_iso()

    def accept(self, key: str, server: str, event: str, occurrences: list[JsonObj],
               cursor: str | None, *, cursor_known: bool = True) -> list[Delivered]:
        """Dedupe ``occurrences`` by eventId, queue the new ones and commit ``cursor`` with them.

        Returns the newly queued deliveries (duplicates are dropped silently, as the spec asks).
        """
        out: list[Delivered] = []
        with self.tx() as doc:
            rec = self.sub(key)
            seen: list[str] = [s for s in rec.get("seen", []) if isinstance(s, str)]
            seen_set = set(seen)
            for occ in occurrences:
                eid = occ.get("eventId")
                if not isinstance(eid, str) or not eid:
                    rec["malformed"] = int(rec.get("malformed", 0)) + 1
                    continue
                if eid in seen_set:
                    rec["duplicates"] = int(rec.get("duplicates", 0)) + 1
                    continue
                seen.append(eid)
                seen_set.add(eid)
                doc["seq"] = int(doc.get("seq", 0)) + 1
                d = Delivered(key, doc["seq"], server, event, occ, now_iso())
                out.append(d)
                doc["pending"].append(d.to_json())
            rec["seen"] = seen[-SEEN_WINDOW:]
            if cursor_known:
                rec["cursor"] = cursor
            if out:
                rec["events_total"] = int(rec.get("events_total", 0)) + len(out)
                rec["last_event_at"] = now_iso()
            overflow = len(doc["pending"]) - PENDING_CAP
            if overflow > 0:
                dropped = doc["pending"][:overflow]
                doc["pending"] = doc["pending"][overflow:]
                for raw in dropped:
                    s = self.sub(str(raw.get("key")))
                    s["dropped"] = int(s.get("dropped", 0)) + 1
        return out

    def note(self, key: str, **fields: Any) -> None:
        with self.tx():
            rec = self.sub(key)
            for k, v in fields.items():
                if v is None:
                    rec.pop(k, None)
                else:
                    rec[k] = v

    # --- pending queue (accepted, not yet delivered to a sink) ------------------------------

    def pending(self) -> list[Delivered]:
        with self._lock:
            return [Delivered.from_json(p) for p in self.doc["pending"] if isinstance(p, dict)]

    def ack(self, seqs: set[int]) -> None:
        if not seqs:
            return
        with self.tx() as doc:
            doc["pending"] = [p for p in doc["pending"] if int(p.get("seq", -1)) not in seqs]
