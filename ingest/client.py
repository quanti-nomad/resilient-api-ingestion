"""A resilient HTTP client for unreliable APIs.

- client-side rate limiter (token bucket) to stay under the API's limit
- honors Retry-After on 429 instead of hammering the API
- retries 5xx errors, timeouts and dropped connections with exponential
  backoff and full jitter, up to a limit
- refreshes the bearer token before it expires, and once more if the API
  says it expired anyway
- fails fast on errors that retrying cannot fix (400, 403, 404)
- counts everything, so every run reports exactly what it survived
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field

import requests


class ApiError(Exception):
    """A non-retryable error (bad request, permission, not found)."""


class RetriesExhausted(Exception):
    """The API kept failing after every allowed retry."""


@dataclass
class CallStats:
    requests: int = 0
    retries: int = 0
    rate_limited: int = 0
    server_errors: int = 0
    timeouts: int = 0
    token_refreshes: int = 0
    slept_seconds: float = 0.0

    def merge(self, other: "CallStats") -> None:
        for k, v in vars(other).items():
            setattr(self, k, getattr(self, k) + v)


class TokenBucket:
    def __init__(self, rate_per_sec: float, clock=time.monotonic, sleep=time.sleep):
        self.rate, self.capacity = rate_per_sec, max(1.0, rate_per_sec)
        self.tokens, self.clock, self.sleep = self.capacity, clock, sleep
        self.at = clock()
        self.lock = threading.Lock()

    def acquire(self) -> float:
        with self.lock:
            now = self.clock()
            self.tokens = min(self.capacity, self.tokens + (now - self.at) * self.rate)
            self.at = now
            wait = 0.0 if self.tokens >= 1 else (1 - self.tokens) / self.rate
            self.tokens -= 1
        if wait:
            self.sleep(wait)
        return wait


@dataclass
class ApiClient:
    base_url: str
    client_id: str
    client_secret: str
    rate_per_sec: float = 15.0
    timeout: float = 10.0
    max_attempts: int = 8           # budget for real failures (5xx, timeouts)
    max_rate_limit_waits: int = 30  # separate budget for 429s: slow down, don't give up
    backoff_base: float = 0.5
    backoff_cap: float = 30.0
    refresh_margin: float = 0.1  # refresh when 10% of the token's life is left
    sleep: callable = time.sleep
    stats: CallStats = field(default_factory=CallStats)

    def __post_init__(self):
        self.session = requests.Session()
        self.limiter = TokenBucket(self.rate_per_sec, sleep=self._sleep)
        self._token, self._expires_at = None, 0.0
        self._rng = random.Random()

    # --- internals -------------------------------------------------------------
    def _sleep(self, seconds: float) -> None:
        self.stats.slept_seconds += seconds
        self.sleep(seconds)

    def _backoff(self, attempt: int) -> None:
        self.stats.retries += 1
        self._sleep(self._rng.uniform(0, min(self.backoff_cap, self.backoff_base * 2 ** attempt)))

    def _refresh_token(self) -> None:
        payload = self._send("POST", "/v1/auth/token",
                             json={"client_id": self.client_id, "client_secret": self.client_secret},
                             authed=False)
        self._token = payload["access_token"]
        ttl = float(payload.get("expires_in", 300))
        self._expires_at = time.monotonic() + ttl * (1 - self.refresh_margin)
        self.stats.token_refreshes += 1

    def _send(self, method: str, path: str, params=None, json=None, authed=True) -> dict:
        failures = waits = 0
        refreshed_after_401 = False
        while True:
            if authed and (self._token is None or time.monotonic() >= self._expires_at):
                self._refresh_token()
            self.limiter.acquire()
            headers = {"Authorization": f"Bearer {self._token}"} if authed else {}
            self.stats.requests += 1
            try:
                r = self.session.request(method, self.base_url + path, params=params, json=json,
                                         headers=headers, timeout=self.timeout)
            except (requests.Timeout, requests.ConnectionError):
                self.stats.timeouts += 1
                failures += 1
                if failures >= self.max_attempts:
                    break
                self._backoff(failures)
                continue

            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                self.stats.rate_limited += 1
                waits += 1
                if waits > self.max_rate_limit_waits:
                    break
                self.stats.retries += 1
                retry_after = r.headers.get("Retry-After")
                self._sleep(float(retry_after) if retry_after else self.backoff_base * 2 ** min(waits, 6))
                continue
            if r.status_code == 401 and authed and not refreshed_after_401:
                refreshed_after_401 = True  # token expired early or was revoked: refresh once
                self._token = None
                continue
            if r.status_code >= 500:
                self.stats.server_errors += 1
                failures += 1
                if failures >= self.max_attempts:
                    break
                self._backoff(failures)
                continue
            raise ApiError(f"{method} {path} -> {r.status_code}: {r.text[:200]}")
        raise RetriesExhausted(f"{method} {path} failed after {failures} failures and {waits} rate-limit waits")

    # --- public ------------------------------------------------------------------
    def get(self, path: str, params: dict | None = None) -> dict:
        return self._send("GET", path, params=params)
