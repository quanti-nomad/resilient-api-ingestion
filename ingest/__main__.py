"""Run the pipeline against a real API.

    python -m ingest --base-url https://api.example.com --db "sqlite:///data/ingest.db"
    python -m ingest --base-url ... --db "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+18+for+SQL+Server"

Credentials come from API_CLIENT_ID and API_CLIENT_SECRET. Set ALERT_WEBHOOK_URL
to post alerts to Slack or Teams. Exit code is 1 if any endpoint or check failed,
so a scheduler (cron, Task Scheduler, SQL Agent, Airflow) can flag the run.
"""

import argparse
import os
import sys
from datetime import date

from .pipeline import Settings, run

ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("--base-url", required=True)
ap.add_argument("--db", default="sqlite:///data/ingest.db")
ap.add_argument("--landing", default="data/landing")
ap.add_argument("--backfill-start", type=date.fromisoformat, default=date(2026, 1, 1))
ap.add_argument("--lookback-days", type=int, default=14, help="use a larger value for a weekly deep refresh")
ap.add_argument("--endpoints", nargs="*")
a = ap.parse_args()

summary = run(Settings(a.base_url, os.environ["API_CLIENT_ID"], os.environ["API_CLIENT_SECRET"], db_url=a.db,
                       landing_dir=a.landing, backfill_start=a.backfill_start, lookback_days=a.lookback_days),
              endpoints=a.endpoints)
for e in summary.endpoints:
    print(f"{e.endpoint:<14}{e.status:<9}+{e.inserted} ~{e.updated} ={e.unchanged} rejected {e.rejected}  {e.error}")
for alert in summary.alerts:
    print("ALERT:", alert)
sys.exit(0 if summary.ok else 1)
