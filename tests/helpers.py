from datetime import date

from sqlalchemy import create_engine

from ingest.client import ApiClient
from ingest.pipeline import Settings
from mock_api.data import Dataset
from mock_api.server import CLIENT_ID, CLIENT_SECRET, Chaos, serve

CALM = dict(rate_limit_per_sec=500, error_rate=0, timeout_rate=0, page_overlap_rate=0)
STORMY = dict(rate_limit_per_sec=60, retry_after_seconds=0.1, error_rate=0.12, timeout_rate=0.03,
              timeout_seconds=0.5, page_overlap_rate=0.25, token_ttl_seconds=1.5, advertised_ttl_seconds=2.5)


class Env:
    """A mock API plus a pipeline configured against it, in a temp folder."""

    def __init__(self, tmp_path, chaos: dict, seed=5, **settings):
        self.ds = Dataset(date(2026, 1, 1), date(2026, 2, 20), seed=seed, n_centers=3, n_guests=250,
                          drift_from=date(2026, 2, 10))
        self.server, self.api, self.url = serve(self.ds, Chaos(seed=seed, **chaos))
        self.settings = Settings(self.url, CLIENT_ID, CLIENT_SECRET, db_url=f"sqlite:///{tmp_path}/t.db",
                                 landing_dir=str(tmp_path / "landing"), backfill_start=self.ds.start,
                                 page_size=40, **settings)
        self.client = ApiClient(self.url, CLIENT_ID, CLIENT_SECRET, rate_per_sec=80, timeout=0.25,
                                backoff_base=0.02, backoff_cap=0.2)
        self.engine = create_engine(self.settings.db_url)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
