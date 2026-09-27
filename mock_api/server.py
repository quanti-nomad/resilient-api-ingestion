"""A deliberately unreliable REST API, modeled on real clinic/salon platform APIs.

Behaviors an ingestion pipeline has to survive (all configurable in `Chaos`):

- rate limiting: HTTP 429 with a Retry-After header
- random server errors: HTTP 500 / 502 / 503
- slow responses that exceed the client timeout
- bearer tokens that expire mid-run (HTTP 401 token_expired)
- date-range endpoints that reject windows longer than 7 days (HTTP 400)
- page-size caps and opaque cursors
- unstable pagination: a record from the previous page sometimes repeats
- schema drift: a new field appears on newer records

Run standalone:  python -m mock_api.server --port 8765
"""

from __future__ import annotations

import argparse
import base64
import json
import random
import secrets
import threading
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .data import Dataset

CLIENT_ID, CLIENT_SECRET = "demo-client", "demo-secret"


@dataclass
class Chaos:
    rate_limit_per_sec: float = 20.0
    retry_after_seconds: float = 1.0
    error_rate: float = 0.05
    timeout_rate: float = 0.01
    timeout_seconds: float = 3.0
    page_overlap_rate: float = 0.05
    token_ttl_seconds: float = 300.0
    advertised_ttl_seconds: float | None = None  # if longer than the real TTL, tokens die "early"
    max_window_days: int = 7
    max_page_size: int = 100
    seed: int = 1


class MockApi:
    def __init__(self, dataset: Dataset, chaos: Chaos):
        self.data = dataset
        self.chaos = chaos
        self.rng = random.Random(chaos.seed)
        self.lock = threading.RLock()  # re-entrant: endpoint handlers call helpers that also lock
        self.tokens: dict[str, float] = {}
        self.bucket = chaos.rate_limit_per_sec
        self.bucket_at = time.monotonic()
        self.stats = {k: 0 for k in ("requests", "ok", "rate_limited", "server_errors", "timeouts",
                                     "token_expired", "bad_window", "overlapping_pages", "tokens_issued")}

    # --- state helpers -----------------------------------------------------------
    def count(self, key: str) -> None:
        with self.lock:
            self.stats[key] += 1

    def take_token(self) -> bool:
        with self.lock:
            now = time.monotonic()
            rate = self.chaos.rate_limit_per_sec
            self.bucket = min(rate, self.bucket + (now - self.bucket_at) * rate)
            self.bucket_at = now
            if self.bucket >= 1:
                self.bucket -= 1
                return True
            return False

    def roll(self, p: float) -> bool:
        with self.lock:
            return self.rng.random() < p

    # --- endpoint logic -----------------------------------------------------------
    def page(self, rows: list[dict], params: dict, sort_key) -> tuple[int, dict]:
        size = int(params.get("page_size", 50))
        if size > self.chaos.max_page_size:
            return 400, {"error": f"page_size may not exceed {self.chaos.max_page_size}"}
        rows = sorted(rows, key=sort_key)
        offset = int(base64.urlsafe_b64decode(params["cursor"]).decode()) if params.get("cursor") else 0
        start = offset
        if offset and self.roll(self.chaos.page_overlap_rate):
            start = offset - 1  # unstable pagination: repeat the last record of the previous page
            self.count("overlapping_pages")
        chunk = rows[start: offset + size]
        nxt = offset + size
        cursor = base64.urlsafe_b64encode(str(nxt).encode()).decode() if nxt < len(rows) else None
        return 200, {"data": chunk, "next_cursor": cursor, "total": len(rows)}

    def windowed(self, rows: list[dict], params: dict, date_of, sort_key) -> tuple[int, dict]:
        try:
            a, b = date.fromisoformat(params["start_date"]), date.fromisoformat(params["end_date"])
        except (KeyError, ValueError):
            return 400, {"error": "start_date and end_date (YYYY-MM-DD) are required"}
        if (b - a).days + 1 > self.chaos.max_window_days or b < a:
            self.count("bad_window")
            return 400, {"error": f"date range may not exceed {self.chaos.max_window_days} days"}
        subset = [r for r in rows if a <= date.fromisoformat(date_of(r)) <= b]
        return self.page(subset, params, sort_key)

    def handle(self, method: str, path: str, params: dict, headers, body: dict) -> tuple[int, dict, dict]:
        extra: dict = {}
        if path == "/admin/advance" and method == "POST":
            with self.lock:
                return 200, self.data.advance(int(params.get("days", 3))), extra
        if path == "/admin/stats":
            return 200, dict(self.stats), extra

        self.count("requests")
        if not self.take_token():
            self.count("rate_limited")
            return 429, {"error": "rate limit exceeded"}, {"Retry-After": str(self.chaos.retry_after_seconds)}
        if self.roll(self.chaos.error_rate):
            self.count("server_errors")
            return self.rng.choice([500, 502, 503]), {"error": "upstream failure"}, extra
        if self.roll(self.chaos.timeout_rate):
            self.count("timeouts")
            time.sleep(self.chaos.timeout_seconds)

        if path == "/v1/auth/token" and method == "POST":
            if body.get("client_id") != CLIENT_ID or body.get("client_secret") != CLIENT_SECRET:
                return 401, {"error": "invalid_client"}, extra
            token = secrets.token_hex(16)
            with self.lock:
                self.tokens[token] = time.monotonic()
                self.stats["tokens_issued"] += 1
            return 200, {"access_token": token,
                         "expires_in": self.chaos.advertised_ttl_seconds or self.chaos.token_ttl_seconds}, extra

        auth = headers.get("Authorization", "")
        token = auth.removeprefix("Bearer ").strip()
        issued = self.tokens.get(token)
        if issued is None:
            return 401, {"error": "invalid_token"}, extra
        if time.monotonic() - issued > self.chaos.token_ttl_seconds:
            self.count("token_expired")
            return 401, {"error": "token_expired"}, extra

        with self.lock:  # consistent snapshot while advance() may run
            d = self.data
            if path == "/v1/centers":
                status, payload = 200, {"data": list(d.centers), "next_cursor": None, "total": len(d.centers)}
            elif path == "/v1/guests":
                since = params.get("updated_since", "")
                rows = [g for g in d.guests.values() if g["updated_at"] > since]
                status, payload = self.page(rows, params, lambda r: (r["updated_at"], r["id"]))
            elif path == "/v1/appointments":
                status, payload = self.windowed(list(d.appointments.values()), params,
                                                lambda r: r["start_time"][:10], lambda r: (r["start_time"], r["id"]))
            elif path == "/v1/invoices":
                status, payload = self.windowed(list(d.invoices.values()), params,
                                                lambda r: r["invoice_date"], lambda r: (r["invoice_date"], r["id"]))
            else:
                return 404, {"error": "not found"}, extra
        payload = json.loads(json.dumps(payload))  # detach from live data
        if status == 200:
            self.count("ok")
        return status, payload, extra


def make_handler(api: MockApi):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _serve(self, method: str):
            u = urlparse(self.path)
            params = {k: v[0] for k, v in parse_qs(u.query).items()}
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length) or b"{}") if length else {}
            status, payload, extra = api.handle(method, u.path, params, self.headers, body)
            data = json.dumps(payload).encode()
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                for k, v in extra.items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass  # the client gave up (timeout); that is the point

        def do_GET(self):
            self._serve("GET")

        def do_POST(self):
            self._serve("POST")

        def log_message(self, *args):
            pass

    return Handler


def serve(dataset: Dataset, chaos: Chaos, port: int = 0) -> tuple[ThreadingHTTPServer, MockApi, str]:
    """Start the server on a background thread. Returns (server, api, base_url)."""
    api = MockApi(dataset, chaos)
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(api))
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, api, f"http://127.0.0.1:{server.server_address[1]}"


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=8765)
    a = ap.parse_args()
    srv, api, url = serve(Dataset(date(2026, 1, 1), date(2026, 3, 31), drift_from=date(2026, 3, 1)), Chaos(), a.port)
    print(f"Mock API on {url}  (client_id={CLIENT_ID}, client_secret={CLIENT_SECRET})")
    print(json.dumps(asdict(api.chaos), indent=1))
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        srv.shutdown()
