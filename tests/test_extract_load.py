import json
from datetime import date, datetime

import pytest
from sqlalchemy import select

from ingest.extract import ENDPOINTS, Extractor, date_windows
from ingest.load import TABLES, Loader, ingest_rejects, schema_drift_events
from tests.helpers import CALM, Env


def test_date_windows_never_exceed_the_cap_and_cover_every_day():
    w = date_windows(date(2026, 1, 1), date(2026, 1, 31), 7)
    assert all((b - a).days < 7 for a, b in w)
    days = sum((b - a).days + 1 for a, b in w)
    assert days == 31 and w[0][0] == date(2026, 1, 1) and w[-1][1] == date(2026, 1, 31)


def test_overlapping_pages_are_deduplicated_and_complete(tmp_path):
    env = Env(tmp_path, {**CALM, "page_overlap_rate": 1.0})
    try:
        ex = Extractor(env.client, tmp_path / "landing", "r1", page_size=25)
        res = ex.extract(ENDPOINTS["appointments"], start=date(2026, 1, 1), end=date(2026, 2, 20))
        assert res.duplicates_dropped > 0
        assert len(res.records) == res.expected == len({r["id"] for r in res.records})
        landed = (tmp_path / "landing/appointments/run=r1/records.jsonl").read_text().splitlines()
        assert len(landed) == res.received  # raw landing keeps exactly what the API sent
    finally:
        env.close()


def rec(**kw):
    base = {"id": "APT-1", "center_id": "CTR-001", "guest_id": "GST-1", "service": "Sauna",
            "start_time": "2026-01-05T10:00:00Z", "status": "booked", "price": 49.0,
            "updated_at": "2026-01-01T09:00:00Z"}
    return base | kw


def test_last_write_wins_on_updated_at(tmp_path):
    loader = Loader(f"sqlite:///{tmp_path}/l.db")
    loader.load("appointments", [rec(status="completed", updated_at="2026-01-05T12:00:00Z")], "r1")
    stale = loader.load("appointments", [rec(status="booked", updated_at="2026-01-01T09:00:00Z")], "r2")
    assert (stale.inserted, stale.updated, stale.unchanged) == (0, 0, 1)
    newer = loader.load("appointments", [rec(status="no_show", updated_at="2026-01-06T08:00:00Z")], "r3")
    assert newer.updated == 1
    with loader.engine.connect() as con:
        assert con.execute(select(TABLES["appointments"].c.status)).scalar() == "no_show"


def test_bad_records_are_rejected_not_fatal(tmp_path):
    loader = Loader(f"sqlite:///{tmp_path}/l.db")
    res = loader.load("appointments", [rec(id="A1"), rec(id="A2", start_time="not a date"),
                                       rec(id="A3", status=None)], "r1")
    assert (res.inserted, res.rejected) == (1, 2)
    with loader.engine.connect() as con:
        reasons = {r.record_id: r.reason for r in con.execute(select(ingest_rejects))}
    assert "start_time" in reasons["A2"] and "status" in reasons["A3"]


def test_schema_drift_is_kept_and_logged_once(tmp_path):
    loader = Loader(f"sqlite:///{tmp_path}/l.db")
    first = loader.load("appointments", [rec(id="A1", booking_channel="online")], "r1")
    second = loader.load("appointments", [rec(id="A2", booking_channel="phone")], "r2")
    assert first.new_drift_fields == ("booking_channel",) and second.new_drift_fields == ()  # alert once
    with loader.engine.connect() as con:
        events = con.execute(select(schema_drift_events)).fetchall()
        extra = con.execute(select(TABLES["appointments"].c.extra_json)
                            .where(TABLES["appointments"].c.id == "A2")).scalar()
    assert len(events) == 1 and events[0].field == "booking_channel" and events[0].first_seen_run == "r1"
    assert json.loads(extra) == {"booking_channel": "phone"}


def test_reference_rows_only_update_when_content_changes(tmp_path):
    loader = Loader(f"sqlite:///{tmp_path}/l.db")
    c = {"id": "CTR-1", "name": "A", "franchisee_id": "FR-1", "timezone": "UTC", "opened_on": "2025-01-01"}
    loader.load("centers", [c], "r1")
    assert loader.load("centers", [c], "r2").unchanged == 1
    assert loader.load("centers", [c | {"name": "B"}], "r3").updated == 1
