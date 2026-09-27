import pytest

from ingest.client import ApiClient, ApiError, RetriesExhausted, TokenBucket
from mock_api.server import CLIENT_ID, CLIENT_SECRET
from tests.helpers import CALM, Env


def test_token_bucket_waits_when_empty():
    now = [0.0]
    waits = []
    b = TokenBucket(2, clock=lambda: now[0], sleep=waits.append)
    b.acquire(); b.acquire()          # burst capacity used
    assert b.acquire() == pytest.approx(0.5)  # third call must wait half a second at 2/s
    assert waits == [pytest.approx(0.5)]


@pytest.fixture
def env(tmp_path, request):
    e = Env(tmp_path, {**CALM, **getattr(request, "param", {})})
    yield e
    e.close()


@pytest.mark.parametrize("env", [{"error_rate": 1.0}], indirect=True)
def test_gives_up_after_retry_budget(env):
    c = ApiClient(env.url, CLIENT_ID, CLIENT_SECRET, max_attempts=4, backoff_base=0.001)
    with pytest.raises(RetriesExhausted):
        c.get("/v1/centers")
    assert c.stats.server_errors == 4


@pytest.mark.parametrize("env", [{"rate_limit_per_sec": 5, "retry_after_seconds": 0.05}], indirect=True)
def test_honors_retry_after_and_still_succeeds(env):
    c = ApiClient(env.url, CLIENT_ID, CLIENT_SECRET, rate_per_sec=1000)  # deliberately too fast
    for _ in range(15):
        assert c.get("/v1/centers")["total"] == 3
    assert c.stats.rate_limited > 0
    assert c.stats.server_errors == 0


@pytest.mark.parametrize("env", [{"token_ttl_seconds": 0.3, "advertised_ttl_seconds": 60}], indirect=True)
def test_refreshes_token_when_it_dies_early(env):
    import time
    c = ApiClient(env.url, CLIENT_ID, CLIENT_SECRET)
    c.get("/v1/centers")
    time.sleep(0.4)                   # token is now dead although it claimed 60s
    assert c.get("/v1/centers")["total"] == 3
    assert c.stats.token_refreshes == 2


def test_non_retryable_errors_fail_fast(env):
    c = ApiClient(env.url, CLIENT_ID, CLIENT_SECRET)
    with pytest.raises(ApiError):
        c.get("/v1/appointments", {"start_date": "2026-01-01", "end_date": "2026-01-31"})  # 31 days > 7
    assert c.stats.retries == 0
    bad = ApiClient(env.url, CLIENT_ID, "wrong-secret")
    with pytest.raises(ApiError):
        bad.get("/v1/centers")
