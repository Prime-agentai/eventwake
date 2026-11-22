from __future__ import annotations

import json
import sys
import threading
import time
import urllib.request
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from eventwake.config import Config, Watch, WebhookSpec
from eventwake.devserver import Hub, make_http
from eventwake.mcp import Endpoint

SRC = str(Path(__file__).resolve().parents[1] / "src")


class DevHttp:
    def __init__(self, **hub_kw: Any) -> None:
        self.hub = Hub(**hub_kw)
        self.httpd = make_http(self.hub, "127.0.0.1", 0)
        self.port = self.httpd.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}/mcp"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def control(self, **body: Any) -> Any:
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/_control",
                                     data=json.dumps(body).encode(), method="POST")
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read())

    def endpoint(self, label: str = "dev", headers: dict[str, str] | None = None) -> Endpoint:
        return Endpoint(label, "http", url=self.url, headers=headers or {})

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


@pytest.fixture
def dev() -> Iterator[DevHttp]:
    d = DevHttp(heartbeat_ms=500, next_poll_ms=100)
    yield d
    d.close()


@pytest.fixture
def make_dev() -> Iterator[Callable[..., DevHttp]]:
    made: list[DevHttp] = []

    def factory(**kw: Any) -> DevHttp:
        kw.setdefault("heartbeat_ms", 500)
        kw.setdefault("next_poll_ms", 100)
        d = DevHttp(**kw)
        made.append(d)
        return d

    yield factory
    for d in made:
        d.close()


def stdio_endpoint(*extra: str, label: str = "devio") -> Endpoint:
    return Endpoint(label, "stdio", command=sys.executable,
                    args=["-m", "eventwake", "dev-server", *extra], env={"PYTHONPATH": SRC})


def config(tmp: Path, servers: dict[str, Endpoint], watches: list[Watch],
           webhook: WebhookSpec | None = None) -> Config:
    return Config(None, tmp / "state", servers, watches, webhook or WebhookSpec(), poll_floor=0.1)


def wait_for(pred: Callable[[], bool], timeout: float = 10.0, what: str = "condition") -> None:
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}")


class Collect:
    """A sink that records batches."""

    batch_window = 0.0
    max_batch = 500

    def __init__(self) -> None:
        self.batches: list[list[Any]] = []
        self.lock = threading.Lock()

    def ready(self) -> bool:
        return True

    def next_slot(self) -> float:
        return 0.0

    def deliver(self, batch: list[Any]) -> dict[str, Any]:
        with self.lock:
            self.batches.append(list(batch))
        return {"sink": "collect", "count": len(batch)}

    @property
    def events(self) -> list[Any]:
        with self.lock:
            return [d for b in self.batches for d in b]

    def ids(self) -> list[str]:
        return [d.occurrence["eventId"] for d in self.events]
