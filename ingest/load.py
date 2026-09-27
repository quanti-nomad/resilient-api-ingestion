"""Load landed records into SQL: typed, validated, idempotent.

Written with SQLAlchemy Core so the same code targets SQLite (local, tests) and
Microsoft SQL Server (production), e.g.
    mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+18+for+SQL+Server

- Validation: records missing required fields or with unparseable values go
  to ingest_rejects with a reason, instead of failing the whole run.
- Schema drift: fields the table doesn't know yet are kept in extra_json and
  logged once in schema_drift_events, so nothing is silently dropped.
- Upserts are last-write-wins on updated_at: an older copy of a record (from
  an overlapping page or a lookback re-pull) never overwrites a newer one.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

from sqlalchemy import (Column, Date, DateTime, Integer, MetaData, Numeric, String, Table, Text,
                        create_engine, delete, insert, select, update)

metadata = MetaData()


def _audit_cols():
    return [Column("extra_json", Text), Column("_run_id", String(40)), Column("_ingested_at", DateTime)]


TABLES = {
    "centers": Table("api_centers", metadata,
                     Column("id", String(40), primary_key=True), Column("name", String(200), nullable=False),
                     Column("franchisee_id", String(40)), Column("timezone", String(60)),
                     Column("opened_on", Date), *_audit_cols()),
    "guests": Table("api_guests", metadata,
                    Column("id", String(40), primary_key=True), Column("center_id", String(40), nullable=False),
                    Column("first_name", String(100)), Column("last_name", String(100)),
                    Column("email", String(200)), Column("created_at", DateTime),
                    Column("updated_at", DateTime, nullable=False), *_audit_cols()),
    "appointments": Table("api_appointments", metadata,
                          Column("id", String(40), primary_key=True),
                          Column("center_id", String(40), nullable=False), Column("guest_id", String(40)),
                          Column("service", String(100)), Column("start_time", DateTime, nullable=False),
                          Column("status", String(20), nullable=False), Column("price", Numeric(10, 2)),
                          Column("updated_at", DateTime, nullable=False), *_audit_cols()),
    "invoices": Table("api_invoices", metadata,
                      Column("id", String(40), primary_key=True), Column("appointment_id", String(40)),
                      Column("center_id", String(40), nullable=False), Column("invoice_date", Date, nullable=False),
                      Column("total", Numeric(10, 2), nullable=False),
                      Column("updated_at", DateTime, nullable=False), *_audit_cols()),
}

ingest_state = Table("ingest_state", metadata,
                     Column("endpoint", String(40), primary_key=True), Column("watermark", String(40)),
                     Column("loaded_through", String(10)), Column("updated_at", DateTime))
ingest_runs = Table("ingest_runs", metadata,
                    Column("run_id", String(40), primary_key=True), Column("endpoint", String(40), primary_key=True),
                    Column("started_at", DateTime), Column("finished_at", DateTime), Column("status", String(20)),
                    Column("scope", String(200)), Column("pages", Integer), Column("received", Integer),
                    Column("duplicates_dropped", Integer), Column("inserted", Integer), Column("updated", Integer),
                    Column("unchanged", Integer), Column("rejected", Integer), Column("requests", Integer),
                    Column("retries", Integer), Column("rate_limited", Integer), Column("server_errors", Integer),
                    Column("timeouts", Integer), Column("token_refreshes", Integer), Column("error", Text))
ingest_rejects = Table("ingest_rejects", metadata,
                       Column("id", Integer, primary_key=True, autoincrement=True), Column("run_id", String(40)),
                       Column("endpoint", String(40)), Column("record_id", String(40)),
                       Column("reason", String(400)), Column("record_json", Text))
schema_drift_events = Table("schema_drift_events", metadata,
                            Column("endpoint", String(40), primary_key=True),
                            Column("field", String(100), primary_key=True),
                            Column("first_seen_run", String(40)), Column("example_value", String(400)))
ingest_checks = Table("ingest_checks", metadata,
                      Column("id", Integer, primary_key=True, autoincrement=True), Column("run_id", String(40)),
                      Column("check_name", String(100)), Column("status", String(10)), Column("detail", Text))


def _ts(v) -> datetime:
    return datetime.fromisoformat(str(v).replace("Z", "+00:00")).replace(tzinfo=None)


def _date(v) -> date:
    return date.fromisoformat(str(v)[:10])


def _money(v) -> Decimal:
    try:
        return Decimal(str(v)).quantize(Decimal("0.01"))
    except InvalidOperation as e:
        raise ValueError(f"not a number: {v!r}") from e


PARSERS = {DateTime: _ts, Date: _date, Numeric: _money}


@dataclass
class LoadResult:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    rejected: int = 0
    drift_fields: tuple = ()       # unknown fields present in this load
    new_drift_fields: tuple = ()   # unknown fields seen for the first time ever


class Loader:
    def __init__(self, url: str):
        self.engine = create_engine(url, future=True)
        metadata.create_all(self.engine)

    def _coerce(self, table: Table, rec: dict) -> dict:
        row = {}
        for col in table.columns:
            if col.name in ("extra_json", "_run_id", "_ingested_at"):
                continue
            v = rec.get(col.name)
            if v is None or v == "":
                if not col.nullable or col.primary_key:
                    raise ValueError(f"missing required field '{col.name}'")
                row[col.name] = None
                continue
            parser = next((p for t, p in PARSERS.items() if isinstance(col.type, t)), None)
            try:
                row[col.name] = parser(v) if parser else str(v)
            except ValueError as e:
                raise ValueError(f"bad value for '{col.name}': {e}") from e
        return row

    def load(self, endpoint: str, records: list[dict], run_id: str) -> LoadResult:
        table = TABLES[endpoint]
        known = {c.name for c in table.columns}
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        result, rows, rejects, drift = LoadResult(), [], [], {}

        for rec in records:
            try:
                row = self._coerce(table, rec)
            except ValueError as e:
                rejects.append({"run_id": run_id, "endpoint": endpoint, "record_id": str(rec.get("id")),
                                "reason": str(e)[:400], "record_json": json.dumps(rec)})
                continue
            extra = {k: v for k, v in rec.items() if k not in known}
            for k, v in extra.items():
                drift.setdefault(k, v)
            row.update(extra_json=json.dumps(extra) if extra else None, _run_id=run_id, _ingested_at=now)
            rows.append(row)

        fresh: list[dict] = []
        with self.engine.begin() as con:
            has_updated = "updated_at" in known
            data_cols = [c for c in table.columns if c.name not in ("extra_json", "_run_id", "_ingested_at")]
            for i in range(0, len(rows), 500):
                chunk = rows[i:i + 500]
                ids = [r["id"] for r in chunk]
                existing = {r["id"]: r for r in con.execute(
                    select(*data_cols, table.c.extra_json).where(table.c.id.in_(ids))).mappings()}

                def is_newer(r):
                    old = existing[r["id"]]
                    if has_updated:  # last-write-wins on the source's own timestamp
                        return r["updated_at"] > old["updated_at"]
                    # reference tables without updated_at: replace only if something changed
                    return any(r[c.name] != old[c.name] for c in data_cols) or r["extra_json"] != old["extra_json"]

                new_rows = [r for r in chunk if r["id"] not in existing]
                changed = [r for r in chunk if r["id"] in existing and is_newer(r)]
                result.unchanged += len(chunk) - len(new_rows) - len(changed)
                if changed:
                    con.execute(delete(table).where(table.c.id.in_([r["id"] for r in changed])))
                    con.execute(insert(table), changed)
                if new_rows:
                    con.execute(insert(table), new_rows)
                result.inserted += len(new_rows)
                result.updated += len(changed)
            if rejects:
                con.execute(insert(ingest_rejects), rejects)
            if drift:
                seen = {r[0] for r in con.execute(select(schema_drift_events.c.field)
                                                  .where(schema_drift_events.c.endpoint == endpoint))}
                fresh = [{"endpoint": endpoint, "field": k, "first_seen_run": run_id,
                          "example_value": str(v)[:400]} for k, v in drift.items() if k not in seen]
                if fresh:
                    con.execute(insert(schema_drift_events), fresh)
        result.rejected = len(rejects)
        result.drift_fields = tuple(sorted(drift))
        result.new_drift_fields = tuple(sorted(f["field"] for f in fresh))
        return result

    # --- state ----------------------------------------------------------------------
    def get_state(self, endpoint: str) -> dict:
        with self.engine.connect() as con:
            r = con.execute(select(ingest_state).where(ingest_state.c.endpoint == endpoint)).mappings().first()
        return dict(r) if r else {}

    def set_state(self, endpoint: str, **values) -> None:
        values["updated_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
        with self.engine.begin() as con:
            if con.execute(select(ingest_state.c.endpoint).where(ingest_state.c.endpoint == endpoint)).first():
                con.execute(update(ingest_state).where(ingest_state.c.endpoint == endpoint).values(**values))
            else:
                con.execute(insert(ingest_state).values(endpoint=endpoint, **values))

    def record_run(self, **row) -> None:
        with self.engine.begin() as con:
            con.execute(insert(ingest_runs).values(**row))

    def record_checks(self, run_id: str, checks: list) -> None:
        if checks:
            with self.engine.begin() as con:
                con.execute(insert(ingest_checks), [{"run_id": run_id, "check_name": c.name,
                                                     "status": c.status, "detail": c.detail} for c in checks])
