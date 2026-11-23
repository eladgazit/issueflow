# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`issueflow` fetches GitHub issues (`microsoft/vscode`, pull requests excluded), publishes them to Kafka, and a consumer turns them into CSV files with derived fields. `README.md` explains how it works (architecture diagram, per-file guide); `DESIGN.md` explains why and lists the trade-offs. Read those before changing the pipeline.

## Setup and commands

- Python 3.12 is required. The code uses `str | None` and generic function syntax, so the system `python3` (3.9) will not import it. The virtualenv lives at `.venv/`.
- Install: `.venv/bin/pip install -e . pytest`
- Run tests (no network and no Kafka needed): `.venv/bin/python -m pytest`
- Run one test: `.venv/bin/python -m pytest tests/test_pipeline.py::test_name`
- Start Kafka (single KRaft broker, creates the topic): `docker compose up -d`; stop with `docker compose stop` (keeps data) or `docker compose down -v` (erases it).
- Run: `issueflow produce --limit 200`, then `issueflow consume --out out`; or `issueflow run --limit 200 --out out` for both.
- No linter or formatter is configured.

## Architecture (the part that spans files)

- Two programs share one Kafka topic, `issueflow.issues`, **one partition** (order matters: the aggregate step is a merge join over files written in fetch order).
- Producer: `sources/` (HTTP, retry, rate limit, GitHub paging) -> `pipeline.produce` -> `messaging/kafka.py`. It sends START, one message per issue (with a `seq`), then END with the count.
- Consumer: `messaging/kafka.py` -> `pipeline.consume` -> `transforms/derived.enrich` -> `sinks/csv_sink.CsvSink`, then `aggregate.py`. It commits its Kafka position only after the files are closed.
- `messaging/kafka.py` is the only file that imports the Kafka client. `pipeline.py` uses only the `Publisher`/`Reader`/`Sink` protocols, so it is tested with plain lists and an in-memory fake broker (`tests/conftest.py`).
- Output goes to `<out>/as_of=YYYY-MM-DD/<table>_<creation timestamp>.csv` and files are never overwritten.

## Conventions

- Python 3.12, type hints throughout, small focused modules.
- Dependencies are the standard library plus `pytest`, and `kafka-python-ng` (the approved Kafka client). Do not add any other dependency without asking first.
- Time, sleep, randomness, the HTTP opener and the Kafka producer/consumer are injected, so tests never need the network, a clock or a broker. Keep new code injectable in the same way.
- Run pytest after every change.

## Decisions that look like bugs but are not

- `retry.py` never retries 429; rate limiting is the rate limiter's job.
- Day counts are whole, floored days; `is_stale` is open and `days_since_update > 30`; a timestamp after `as_of` counts as 0.
- Do not detect duplicate Kafka messages by issue number: real data has issue 64 created after issue 65. Use `seq`.
- No `GITHUB_TOKEN` support, on purpose (unauthenticated limit is 60 requests per hour).
- `.gitignore` excludes `.venv/`, `out/` (generated output), `*.db`, and `.env`.
