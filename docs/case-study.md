# Case study: ingesting from an API that fights back

*Company and vendor details are anonymized. The code in this repository is a clean-room rebuild on synthetic data.*

## Context

A healthcare organization acquired a franchise wellness brand with dozens of locations. The brand ran on a third-party clinic management platform, and franchisees expected regular performance reporting from corporate.

## The problem

The platform's data was only available through its API, and the API had hard constraints: tight rate limits, report endpoints that accepted only short date ranges, tokens that expired during long pulls, and no reliable way to ask "what changed since yesterday?" for bookings. Loading it directly into the company's reporting platform failed repeatedly, which left franchisee reporting on shaky ground and made one person the fallback for every broken load.

## What I built

- **A staging database** on a self-built Microsoft SQL Server, so API data landed somewhere we controlled before any reporting used it.
- **Python-automated pipelines** that split history into API-sized date windows, paginated safely, retried through rate limits and outages, and refreshed tokens mid-run.
- **Incremental loading** that re-pulls a recent window every run, because bookings change status after the fact.
- **Automated distribution** of recurring reports to every franchisee from the staged data.

## Outcome

- Reliable, automated reporting for the franchise brand despite the API's limits.
- Franchisee reporting delivered on schedule from one consistent source.
- A clear migration path: the staged, cleaned data is now being moved into the company's certified data platform.

## What I'd do next

The honest weakness of the original build was that it depended on one person: when a load broke, franchisees came to me. The next steps are the ones this repository demonstrates: run logs, post-load checks and automatic alerts, a documented deep-refresh schedule, and moving the server onto company-managed infrastructure so a second person can operate it.

## Skills demonstrated

API integration under real-world constraints, Python data engineering, SQL Server, incremental loading design, data quality checks, operational monitoring and franchisee reporting.
