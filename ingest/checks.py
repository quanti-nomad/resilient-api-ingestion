"""Post-load data quality checks. Failures raise an alert; warnings are logged."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import func, select

from .load import TABLES


@dataclass
class Check:
    name: str
    status: str  # pass | warn | fail
    detail: str


def run_checks(loader, as_of: datetime, extracts: dict, freshness_hours: int = 48) -> list[Check]:
    out: list[Check] = []
    with loader.engine.connect() as con:
        # 1. every extract received exactly what the API said matched
        for name, ex in extracts.items():
            ok = len(ex.records) == ex.expected
            out.append(Check(f"{name}.complete", "pass" if ok else "fail",
                             f"{len(ex.records):,} unique of {ex.expected:,} reported by API"))

        # 2. freshness: the newest change we hold is recent. Bookings change every
        #    day, so stale appointments fail; guest signups can legitimately be
        #    quiet for a couple of days at a small center, so they only warn.
        for name, severity in (("appointments", "fail"), ("guests", "warn")):
            t = TABLES[name]
            newest = con.execute(select(func.max(t.c.updated_at))).scalar()
            if newest is None:
                out.append(Check(f"{name}.fresh", "fail", "table is empty"))
                continue
            newest = newest if isinstance(newest, datetime) else datetime.fromisoformat(str(newest))
            age = as_of - newest
            ok = age <= timedelta(hours=freshness_hours)
            out.append(Check(f"{name}.fresh", "pass" if ok else severity,
                             f"newest change {age.total_seconds() / 3600:.1f}h before as-of time"))

        # 3. referential integrity (warn: the API can send a child before its parent)
        a, g, c, i = (TABLES[k] for k in ("appointments", "guests", "centers", "invoices"))
        checks = {
            "appointments.guest_exists": select(func.count()).select_from(a).where(
                a.c.guest_id.is_not(None), a.c.guest_id.not_in(select(g.c.id))),
            "appointments.center_exists": select(func.count()).select_from(a).where(
                a.c.center_id.not_in(select(c.c.id))),
            "invoices.appointment_exists": select(func.count()).select_from(i).where(
                i.c.appointment_id.not_in(select(a.c.id))),
        }
        for name, q in checks.items():
            n = con.execute(q).scalar()
            out.append(Check(name, "pass" if n == 0 else "warn", f"{n:,} orphan rows"))
    return out
