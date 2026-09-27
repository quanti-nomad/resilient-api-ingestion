"""Compare what the pipeline landed with what the API actually holds."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import select

from .load import TABLES

FIELDS = {
    "centers": ["name", "franchisee_id"],
    "guests": ["center_id", "email", "updated_at"],
    "appointments": ["center_id", "guest_id", "status", "price", "updated_at"],
    "invoices": ["appointment_id", "total", "updated_at"],
}


def _norm(v):
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, (Decimal, float)):
        return f"{float(v):.2f}"
    s = str(v)
    return s.replace("Z", "") if s.endswith("Z") else s


def compare(engine, truth: dict[str, list[dict]]) -> dict[str, dict]:
    """Per endpoint: records missing, extra, and mismatched on key fields."""
    report = {}
    with engine.connect() as con:
        for name, rows in truth.items():
            t = TABLES[name]
            landed = {r["id"]: r for r in con.execute(select(t)).mappings()}
            expected = {r["id"]: r for r in rows}
            mismatched = [
                i for i in expected.keys() & landed.keys()
                if any(_norm(expected[i][f]) != _norm(landed[i][f]) for f in FIELDS[name])
            ]
            report[name] = {"expected": len(expected), "landed": len(landed),
                            "missing": len(expected.keys() - landed.keys()),
                            "extra": len(landed.keys() - expected.keys()), "mismatched": len(mismatched)}
    return report


def truth_from_dataset(ds) -> dict[str, list[dict]]:
    return {"centers": ds.centers, "guests": list(ds.guests.values()),
            "appointments": list(ds.appointments.values()), "invoices": list(ds.invoices.values())}
