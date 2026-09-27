import requests
import pytest

from ingest.extract import Extractor
from ingest.pipeline import run
from ingest.verify import compare, truth_from_dataset
from tests.helpers import CALM, STORMY, Env


def exact(report):
    return all(v["missing"] == v["extra"] == v["mismatched"] == 0 for v in report.values())


def test_backfill_and_incremental_are_exact_under_chaos(tmp_path):
    env = Env(tmp_path, STORMY)
    try:
        first = run(env.settings, as_of=env.ds.clock, client=env.client)
        assert first.ok, first.alerts
        assert exact(compare(env.engine, truth_from_dataset(env.ds)))
        stats = first.totals()
        assert stats.server_errors > 0 and stats.token_refreshes > 1  # chaos really happened
        assert any(e.duplicates_dropped for e in first.endpoints)

        requests.post(env.url + "/admin/advance?days=3")
        second = run(env.settings, as_of=env.ds.clock, client=env.client)
        assert second.ok, second.alerts
        assert sum(e.updated for e in second.endpoints) > 0  # late changes were picked up
        assert exact(compare(env.engine, truth_from_dataset(env.ds)))
    finally:
        env.close()


def test_rerun_with_no_changes_writes_nothing(tmp_path):
    env = Env(tmp_path, CALM)
    try:
        run(env.settings, as_of=env.ds.clock, client=env.client)
        again = run(env.settings, as_of=env.ds.clock, client=env.client)
        assert sum(e.inserted + e.updated for e in again.endpoints) == 0
    finally:
        env.close()


def test_one_failing_endpoint_does_not_block_the_others(tmp_path, monkeypatch):
    env = Env(tmp_path, CALM)
    real = Extractor.extract

    def flaky(self, ep, **kw):
        if ep.name == "invoices":
            raise ConnectionError("simulated outage")
        return real(self, ep, **kw)

    try:
        monkeypatch.setattr(Extractor, "extract", flaky)
        s = run(env.settings, as_of=env.ds.clock, client=env.client)
        status = {e.endpoint: e.status for e in s.endpoints}
        assert status == {"centers": "success", "guests": "success", "appointments": "success", "invoices": "failed"}
        assert any("invoices FAILED" in a for a in s.alerts) and not s.ok
        from ingest.load import Loader
        assert Loader(env.settings.db_url).get_state("invoices") == {}  # watermark not advanced

        monkeypatch.setattr(Extractor, "extract", real)
        recovered = run(env.settings, as_of=env.ds.clock, client=env.client)
        assert recovered.ok and exact(compare(env.engine, truth_from_dataset(env.ds)))
    finally:
        env.close()


def test_lookback_window_tradeoff_is_real(tmp_path):
    """A change older than the lookback window is missed by a normal run and
    caught by a deep refresh. This is why the lookback is a setting."""
    env = Env(tmp_path, CALM, lookback_days=14)
    try:
        run(env.settings, as_of=env.ds.clock, client=env.client)
        old = next(a for a in env.ds.appointments.values()
                   if a["start_time"] < "2026-01-15" and a["status"] == "completed")
        old["status"], old["updated_at"] = "no_show", "2026-02-20T20:00:00Z"  # corrected long after the fact

        run(env.settings, as_of=env.ds.clock, client=env.client)
        assert compare(env.engine, truth_from_dataset(env.ds))["appointments"]["mismatched"] == 1

        env.settings.lookback_days = 60  # weekly deep refresh
        run(env.settings, as_of=env.ds.clock, client=env.client)
        assert exact(compare(env.engine, truth_from_dataset(env.ds)))
    finally:
        env.close()
