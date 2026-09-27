"""Run one ingestion pass: extract -> land -> load -> check -> alert.

Each endpoint runs in isolation: if one fails, the others still load, the
failed one keeps its old watermark (so nothing is skipped next time), and an
alert goes out. Parents load before children (centers, guests, then
appointments and invoices).
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from .checks import Check, run_checks
from .client import ApiClient, CallStats
from .extract import ENDPOINTS, Extractor
from .load import Loader


@dataclass
class Settings:
    base_url: str
    client_id: str
    client_secret: str
    db_url: str = "sqlite:///data/ingest.db"
    landing_dir: str = "data/landing"
    backfill_start: date = date(2026, 1, 1)
    lookback_days: int = 14          # re-pull this much history every run to catch late changes
    horizon_days: int = 14           # future bookings to pull
    overlap_minutes: int = 10        # updated_since overlap for clock skew
    rate_per_sec: float = 15.0
    timeout: float = 10.0
    page_size: int = 100
    max_window_days: int = 7
    freshness_hours: int = 48
    alert_webhook: str | None = field(default_factory=lambda: os.environ.get("ALERT_WEBHOOK_URL"))


@dataclass
class EndpointRun:
    endpoint: str
    status: str
    scope: str
    pages: int = 0
    received: int = 0
    duplicates_dropped: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    rejected: int = 0
    drift_fields: tuple = ()
    stats: CallStats = field(default_factory=CallStats)
    error: str = ""


@dataclass
class RunSummary:
    run_id: str
    as_of: datetime
    endpoints: list[EndpointRun]
    checks: list[Check]
    alerts: list[str]

    @property
    def ok(self) -> bool:
        return all(e.status == "success" for e in self.endpoints) and not any(c.status == "fail" for c in self.checks)

    def totals(self) -> CallStats:
        t = CallStats()
        for e in self.endpoints:
            t.merge(e.stats)
        return t


def _delta(before: CallStats, after: CallStats) -> CallStats:
    return CallStats(**{k: getattr(after, k) - getattr(before, k) for k in vars(after)})


def notify(settings: Settings, message: str) -> None:
    """Send an alert to a webhook (Slack, Teams, etc.) if configured."""
    if settings.alert_webhook:
        try:
            requests.post(settings.alert_webhook, json={"text": message}, timeout=10)
        except requests.RequestException:
            pass  # alerting must never break the pipeline; the alert is still in the run log


def run(settings: Settings, as_of: datetime | None = None, client: ApiClient | None = None,
        endpoints: list[str] | None = None) -> RunSummary:
    as_of = as_of or datetime.now(timezone.utc).replace(tzinfo=None)
    run_id = as_of.strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    client = client or ApiClient(settings.base_url, settings.client_id, settings.client_secret,
                                 rate_per_sec=settings.rate_per_sec, timeout=settings.timeout)
    loader = Loader(settings.db_url)
    extractor = Extractor(client, Path(settings.landing_dir), run_id, settings.page_size, settings.max_window_days)
    results, extracts, alerts = [], {}, []

    for name in endpoints or list(ENDPOINTS):
        ep = ENDPOINTS[name]
        state = loader.get_state(name)
        started = datetime.now(timezone.utc).replace(tzinfo=None)
        before = CallStats(**vars(client.stats))
        kwargs, scope = {}, "full refresh"
        if ep.mode == "updated_since":
            since = ""
            if state.get("watermark"):
                wm = datetime.fromisoformat(state["watermark"].rstrip("Z"))
                since = (wm - timedelta(minutes=settings.overlap_minutes)).isoformat() + "Z"
            kwargs, scope = {"since": since}, f"updated since {since or 'the beginning'}"
        elif ep.mode == "date_window":
            start = (date.fromisoformat(state["loaded_through"]) - timedelta(days=settings.lookback_days)
                     if state.get("loaded_through") else settings.backfill_start)
            end = as_of.date() + timedelta(days=settings.horizon_days)
            kwargs, scope = {"start": start, "end": end}, f"{start} to {end}"

        er = EndpointRun(name, "success", scope)
        try:
            ex = extractor.extract(ep, **kwargs)
            lr = loader.load(name, ex.records, run_id)
            er.pages, er.received, er.duplicates_dropped = ex.pages, ex.received, ex.duplicates_dropped
            er.inserted, er.updated, er.unchanged, er.rejected = lr.inserted, lr.updated, lr.unchanged, lr.rejected
            er.drift_fields = lr.drift_fields
            extracts[name] = ex
            if ep.mode == "updated_since" and ex.records:
                loader.set_state(name, watermark=max(r["updated_at"] for r in ex.records))
            elif ep.mode == "date_window":
                loader.set_state(name, loaded_through=str(as_of.date()))
            if lr.rejected:
                alerts.append(f"{name}: {lr.rejected} record(s) rejected, see ingest_rejects")
            if lr.new_drift_fields:  # alert once, the first time the API adds a field
                alerts.append(f"{name}: new field(s) from the API: {', '.join(lr.new_drift_fields)}")
        except Exception as e:  # isolate: one endpoint failing must not stop the others
            er.status, er.error = "failed", f"{type(e).__name__}: {e}"
            alerts.append(f"{name} FAILED ({er.error}). Watermark not advanced; next run retries.")
        er.stats = _delta(before, client.stats)
        loader.record_run(
            run_id=run_id, endpoint=name, started_at=started, finished_at=datetime.now(timezone.utc).replace(tzinfo=None),
            status=er.status, scope=scope, pages=er.pages, received=er.received,
            duplicates_dropped=er.duplicates_dropped, inserted=er.inserted, updated=er.updated,
            unchanged=er.unchanged, rejected=er.rejected, requests=er.stats.requests, retries=er.stats.retries,
            rate_limited=er.stats.rate_limited, server_errors=er.stats.server_errors, timeouts=er.stats.timeouts,
            token_refreshes=er.stats.token_refreshes, error=er.error or None)
        results.append(er)

    checks = run_checks(loader, as_of, extracts, settings.freshness_hours) if extracts else []
    loader.record_checks(run_id, checks)
    alerts += [f"check failed: {c.name} ({c.detail})" for c in checks if c.status == "fail"]
    summary = RunSummary(run_id, as_of, results, checks, alerts)
    if alerts:
        notify(settings, f"Ingestion run {run_id}: " + "; ".join(alerts))
    return summary
