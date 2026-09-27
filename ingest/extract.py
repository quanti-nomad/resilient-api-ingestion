"""Pull records from the API and land them, untouched, before loading.

Landing raw pages first (the "stage it on our own server" step) means a load
can be re-run or audited without calling the API again, and the database is
never written from a half-finished extraction.

Three extraction modes cover how real APIs expose data:
- full:          small reference tables, pulled whole every run
- updated_since: endpoints with a change filter; pull what changed since the
                 watermark, with a small overlap for clock skew
- date_window:   report-style endpoints that only filter by business date and
                 cap each request at N days; re-pull a lookback window every run
                 to catch late changes (a booking that later completes, a
                 discount applied after the visit)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from .client import ApiClient


@dataclass(frozen=True)
class Endpoint:
    name: str
    path: str
    mode: str                 # full | updated_since | date_window
    key: str = "id"
    updated_field: str | None = "updated_at"


ENDPOINTS = {
    "centers": Endpoint("centers", "/v1/centers", "full", updated_field=None),
    "guests": Endpoint("guests", "/v1/guests", "updated_since"),
    "appointments": Endpoint("appointments", "/v1/appointments", "date_window"),
    "invoices": Endpoint("invoices", "/v1/invoices", "date_window"),
}


class IncompleteExtract(Exception):
    """The API said N records matched, but a different number arrived."""


@dataclass
class ExtractResult:
    endpoint: str
    records: list[dict]
    pages: int = 0
    received: int = 0
    duplicates_dropped: int = 0
    expected: int = 0
    windows: list[tuple[str, str]] = field(default_factory=list)
    landing_path: str = ""


def date_windows(start: date, end: date, max_days: int = 7) -> list[tuple[date, date]]:
    """Split [start, end] into consecutive windows of at most max_days, inclusive."""
    out, cur = [], start
    while cur <= end:
        stop = min(end, cur + timedelta(days=max_days - 1))
        out.append((cur, stop))
        cur = stop + timedelta(days=1)
    return out


class Extractor:
    def __init__(self, client: ApiClient, landing_root: Path, run_id: str,
                 page_size: int = 100, max_window_days: int = 7):
        self.client, self.run_id = client, run_id
        self.landing_root, self.page_size, self.max_window_days = Path(landing_root), page_size, max_window_days

    def _paginate(self, ep: Endpoint, params: dict, sink, result: ExtractResult) -> dict[str, dict]:
        """Follow cursors to the end. Returns records keyed by id (duplicates collapsed)."""
        seen: dict[str, dict] = {}
        cursor, total = None, None
        while True:
            q = dict(params, page_size=self.page_size) | ({"cursor": cursor} if cursor else {})
            page = self.client.get(ep.path, q)
            result.pages += 1
            total = page.get("total", total)
            for rec in page["data"]:
                sink.write(json.dumps(rec) + "\n")
                result.received += 1
                prev = seen.get(rec[ep.key])
                if prev is not None:
                    result.duplicates_dropped += 1
                    if ep.updated_field and rec.get(ep.updated_field, "") < prev.get(ep.updated_field, ""):
                        continue
                seen[rec[ep.key]] = rec
            cursor = page.get("next_cursor")
            if not cursor:
                break
        if total is not None and len(seen) != total:
            raise IncompleteExtract(f"{ep.name} {params}: API reported {total}, received {len(seen)} unique")
        result.expected += total or 0
        return seen

    def extract(self, ep: Endpoint, *, since: str | None = None,
                start: date | None = None, end: date | None = None) -> ExtractResult:
        result = ExtractResult(ep.name, [])
        folder = self.landing_root / ep.name / f"run={self.run_id}"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / "records.jsonl"
        result.landing_path = str(path)
        collected: dict[str, dict] = {}
        with path.open("w") as sink:
            if ep.mode == "full":
                collected = self._paginate(ep, {}, sink, result)
            elif ep.mode == "updated_since":
                collected = self._paginate(ep, {"updated_since": since or ""}, sink, result)
            elif ep.mode == "date_window":
                for a, b in date_windows(start, end, self.max_window_days):
                    result.windows.append((str(a), str(b)))
                    collected |= self._paginate(ep, {"start_date": str(a), "end_date": str(b)}, sink, result)
            else:
                raise ValueError(f"unknown mode {ep.mode}")
        result.records = list(collected.values())
        (folder / "_manifest.json").write_text(json.dumps({
            "endpoint": ep.name, "run_id": self.run_id, "pages": result.pages, "received": result.received,
            "unique": len(result.records), "duplicates_dropped": result.duplicates_dropped,
            "api_total": result.expected, "windows": result.windows, "since": since,
        }, indent=1))
        return result
