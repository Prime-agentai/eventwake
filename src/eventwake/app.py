"""Wire config, state, runners, receiver, dispatcher and sinks into one running process."""

from __future__ import annotations

import signal
import threading
import time
from collections.abc import Callable
from pathlib import Path
from types import FrameType
from typing import Any

from .config import Config, ConfigError, Watch, resolve_server
from .engine import ClientPool, Dispatcher, Runner
from .receiver import Receiver
from .sinks import Sink, log
from .state import Delivered, Store

JsonObj = dict[str, Any]


class App:
    def __init__(self, cfg: Config, cwd: Path, make_sink: Callable[[Watch, str], Sink],
                 max_events: int | None = None, timeout: float = 30.0) -> None:
        self.cfg = cfg
        self.store = Store(cfg.state_dir)
        self.pool = ClientPool(timeout)
        self.receiver: Receiver | None = None
        self.runners: list[Runner] = []
        self.max_events = max_events
        self.delivered = 0
        self.done = threading.Event()
        sinks: dict[str, Sink] = {}
        titles: dict[str, str] = {}
        if not cfg.watches:
            raise ConfigError("nothing to watch: add a [[watch]] table to eventwake.toml")
        self.dispatcher = Dispatcher(self.store, sinks, titles, self._on_result)
        for w in cfg.watches:
            ep = resolve_server(w.server, cfg, cwd)
            r = Runner(w, ep, self.store, self.pool, self.dispatcher.wake, cfg)
            if r.key in sinks:
                raise ConfigError(f"{w.title}: duplicate watch (same server, event and arguments)")
            sinks[r.key] = make_sink(w, r.key)
            titles[r.key] = w.title
            self.runners.append(r)

    def _on_result(self, key: str, batch: list[Delivered], result: JsonObj) -> None:
        title = self.dispatcher.titles.get(key, key)
        if result.get("sink") == "run":
            status = "timed out" if result.get("timed_out") else f"exit {result.get('exit')}"
            log(f"{title}: ran agent for {len(batch)} event(s): {status} in {result.get('seconds')}s "
                f"(log {result.get('log')})")
        self.delivered += len(batch)
        if self.max_events is not None and self.delivered >= self.max_events:
            self.dispatcher.stop.set()  # --count N: deliver nothing beyond this batch
            self.done.set()

    def start(self) -> None:
        self.store.acquire()
        spec = self.cfg.webhook
        if spec.public_url is not None:
            self.receiver = Receiver(self.store, spec.listen_host, spec.listen_port)
            self.receiver.start()
            log(f"webhook receiver on {spec.listen_host}:{self.receiver.port}, "
                f"public URL {spec.public_url}/hooks/<subscription>")
        for r in self.runners:
            r.receiver = self.receiver
        self.dispatcher.start()
        if self.store.pending():
            log(f"{len(self.store.pending())} event(s) accepted before the last stop are pending; "
                "delivering them first")
            self.dispatcher.wake()
        for r in self.runners:
            r.start()

    def stop(self) -> None:
        for r in self.runners:
            r.stop.set()
        for r in self.runners:
            r.join(timeout=10)
        self.dispatcher.stop.set()
        self.dispatcher.wake()
        self.dispatcher.join(timeout=5)
        if self.receiver is not None:
            self.receiver.stop()
        self.pool.close()
        self.store.release()

    def wait(self) -> int:
        """Block until a signal, ``max_events`` delivered, or every runner has stopped."""
        def on_signal(_sig: int, _frame: FrameType | None) -> None:
            self.done.set()

        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, on_signal)
            signal.signal(signal.SIGTERM, on_signal)
        while not self.done.wait(0.5):
            if not any(r.alive() for r in self.runners):
                if not self.store.pending():
                    log("every watch has stopped; see `eventwake status`")
                    return 1
            time.sleep(0)
        return 0
