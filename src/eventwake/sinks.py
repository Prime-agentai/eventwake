"""Where woken events go: a headless agent command, a Claude Code channel, or JSON lines."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from collections import deque
from pathlib import Path
from typing import IO, Any, Protocol

from .config import RunSpec
from .framing import attr, frame, render_prompt
from .state import Delivered

JsonObj = dict[str, Any]


class SinkError(Exception):
    """The sink could not take the batch at all; keep it pending and retry later."""


class Sink(Protocol):
    batch_window: float
    max_batch: int

    def ready(self) -> bool: ...

    def next_slot(self) -> float:
        """Seconds until the sink may run again (0 = now)."""
        ...

    def deliver(self, batch: list[Delivered]) -> JsonObj: ...


def log(msg: str) -> None:
    sys.stderr.write(f"{time.strftime('%H:%M:%S')} eventwake: {msg}\n")
    sys.stderr.flush()


def occurrence_line(d: Delivered) -> JsonObj:
    occ = d.occurrence
    return {
        "server": d.server,
        "event": d.event,
        "subscription": d.key,
        "eventId": occ.get("eventId"),
        "timestamp": occ.get("timestamp"),
        "data": occ.get("data"),
        "received_at": d.received_at,
    }


class JsonlSink:
    """One JSON object per event on a stream (stdout by default). Pipe it anywhere."""

    batch_window = 0.0
    max_batch = 500

    def __init__(self, out: IO[str] | None = None) -> None:
        self.out = out or sys.stdout
        self._lock = threading.Lock()

    def ready(self) -> bool:
        return True

    def next_slot(self) -> float:
        return 0.0

    def deliver(self, batch: list[Delivered]) -> JsonObj:
        with self._lock:
            try:
                for d in batch:
                    self.out.write(json.dumps(occurrence_line(d), ensure_ascii=False) + "\n")
                self.out.flush()
            except (OSError, ValueError) as e:
                raise SinkError(f"cannot write events: {e}") from e
        return {"sink": "jsonl", "count": len(batch)}


class PreviewSink:
    """``--dry-run``: print the exact prompt a command sink would send, run nothing."""

    max_batch = 50

    def __init__(self, spec: RunSpec, out: IO[str] | None = None) -> None:
        self.spec = spec
        self.batch_window = spec.batch_window
        self.max_batch = spec.max_batch
        self.out = out or sys.stdout

    def ready(self) -> bool:
        return True

    def next_slot(self) -> float:
        return 0.0

    def deliver(self, batch: list[Delivered]) -> JsonObj:
        prompt = render_prompt(self.spec.prompt, batch)
        self.out.write(f"--- would run: {' '.join(self.spec.argv)} ({len(batch)} event(s)) ---\n")
        self.out.write(prompt + "\n--- end ---\n")
        self.out.flush()
        return {"sink": "preview", "count": len(batch)}


class CommandSink:
    """Run a headless agent (``claude -p``, ``codex exec``, any argv) once per batch.

    The prompt goes on stdin unless an argv element contains ``{prompt}``. The raw batch is also
    written to a JSON file named by ``$EVENTWAKE_EVENTS_FILE`` for tools that want structure.
    """

    def __init__(self, spec: RunSpec, watch: str, log_dir: Path) -> None:
        self.spec = spec
        self.watch = watch
        self.batch_window = spec.batch_window
        self.max_batch = spec.max_batch
        self.log_dir = log_dir
        self._runs: deque[float] = deque()

    def ready(self) -> bool:
        return True

    def next_slot(self) -> float:
        now = time.time()
        while self._runs and now - self._runs[0] >= 3600:
            self._runs.popleft()
        if len(self._runs) < self.spec.max_runs_per_hour:
            return 0.0
        return 3600 - (now - self._runs[0])

    def deliver(self, batch: list[Delivered]) -> JsonObj:
        prompt = render_prompt(self.spec.prompt, batch)
        templated = any("{prompt}" in a for a in self.spec.argv)
        argv = [a.replace("{prompt}", prompt) for a in self.spec.argv] if templated else list(self.spec.argv)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%dT%H%M%S")
        logfile = self.log_dir / f"{stamp}-{attr(self.watch, 60).replace('/', '_')}-{batch[0].seq}.log"
        fd, events_path = tempfile.mkstemp(prefix="eventwake-events-", suffix=".json")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump([d.occurrence | {"server": d.server} for d in batch], fh, ensure_ascii=False)
        env = dict(os.environ)
        env.update(self.spec.env)
        env.update({
            "EVENTWAKE_EVENTS_FILE": events_path,
            "EVENTWAKE_SERVER": batch[0].server,
            "EVENTWAKE_EVENT": batch[0].event,
            "EVENTWAKE_COUNT": str(len(batch)),
            "EVENTWAKE_WATCH": self.watch,
        })
        self._runs.append(time.time())
        started = time.monotonic()
        try:
            with open(logfile, "wb") as out:
                try:
                    proc = subprocess.Popen(
                        argv, stdin=subprocess.DEVNULL if templated else subprocess.PIPE,
                        stdout=out, stderr=subprocess.STDOUT, cwd=self.spec.cwd, env=env,
                        start_new_session=True,
                    )
                except OSError as e:
                    self._runs.pop()
                    raise SinkError(f"cannot start {argv[0]!r}: {e}") from e
                timed_out = False
                try:
                    proc.communicate(None if templated else prompt.encode("utf-8"),
                                     timeout=self.spec.timeout)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except OSError:
                        proc.kill()
                    proc.wait()
        finally:
            try:
                os.unlink(events_path)
            except OSError:
                pass
        return {
            "sink": "run",
            "count": len(batch),
            "exit": proc.returncode,
            "timed_out": timed_out,
            "seconds": round(time.monotonic() - started, 1),
            "log": str(logfile),
        }


class ChannelSink:
    """Push events into a live Claude Code session as ``notifications/claude/channel``.

    ``write`` is the channel server's stdout writer. Claude Code does not acknowledge channel
    notifications, so "delivered" means "written to the session's transport".
    """

    batch_window = 1.0
    max_batch = 20

    def __init__(self, write: Any, initialized: threading.Event, prompt: str | None = None) -> None:
        self._write = write
        self.initialized = initialized
        self.prompt = prompt

    def ready(self) -> bool:
        return self.initialized.is_set()

    def next_slot(self) -> float:
        return 0.0

    def deliver(self, batch: list[Delivered]) -> JsonObj:
        first = batch[0]
        meta = {
            "server": attr(first.server, 60),
            "event_type": attr(first.event, 80),
            "count": str(len(batch)),
            "first_event_id": attr(first.occurrence.get("eventId", ""), 80),
        }
        msg = {"jsonrpc": "2.0", "method": "notifications/claude/channel",
               "params": {"content": render_prompt(self.prompt, batch) if self.prompt else frame(batch),
                          "meta": meta}}
        try:
            self._write(msg)
        except (OSError, ValueError) as e:
            raise SinkError(f"channel transport closed: {e}") from e
        return {"sink": "channel", "count": len(batch)}
