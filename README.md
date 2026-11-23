# issueflow

Streams open and closed issues (pull requests excluded) from `microsoft/vscode`, adds derived fields, and writes CSVs ready for database import. Python 3.12, standard library plus pytest only.

## Architecture

```
                 ┌────────────── fetch thread ──────────────┐        ┌────────────── main thread ─────────────┐
                 │                                          │        │                                        │
  GitHub API ──► HttpClient ──► RetryPolicy ──► RateLimiter │        │                                        │
  (paged, Link   (urllib,       (5xx, URLError, (wait for   │        │   enrich()          CsvSink            │
   header)        HttpError)     timeouts)       reset or   │        │   derived fields    issues.csv         │
                 │                                fail fast)│        │        │            issue_labels.csv   │
                 │      GitHubIssuesSource                  │ bounded│        ▼            issue_assignees.csv│
                 │      · follows Link rel="next"           │ queue  │   EnrichedIssue ──► (streamed rows)    │
                 │      · drops pull requests         ──────┼───────►│                                        │
                 │      · applies --limit                   │        │                                        │
                 │      · parse_issue() -> Issue            │        │                                        │
                 └──────────────────────────────────────────┘        └───────────────────┬────────────────────┘
                                                                                         │
                                                                                         ▼
                                                                          aggregate: stream CSVs ──► by_label.csv
```

- **Streaming:** every stage is a generator, so only about one page (100 issues) is in memory at a time.
- **Concurrency:** the fetch thread overlaps network waits with CSV writing. The bounded queue gives backpressure, and a producer error is re-raised in the main thread.
- **Extensible:** source, transform and sink meet at small interfaces, so a new source, derived field or output format touches one file.
- **Deterministic:** `as_of` is injected and stored on every row. Time, sleep and randomness are injected too, so tests never touch the network.

## Files

| File | Purpose | Status |
|---|---|---|
| `src/issueflow/models.py` | `Issue` and `EnrichedIssue` dataclasses. | done |
| `src/issueflow/sources/base.py` | `Source` protocol: `iter_records()` yields raw dicts. | done |
| `src/issueflow/sources/http.py` | `urllib` GET client that raises `HttpError(status, headers)` on non-2xx. | done |
| `src/issueflow/sources/retry.py` | Exponential backoff with full jitter for 5xx, `URLError` and timeouts. | done |
| `src/issueflow/sources/rate_limit.py` | Reads `X-RateLimit-*` / `Retry-After`; waits for the reset or fails fast. | done |
| `src/issueflow/sources/github.py` | Paginated issues fetcher: drops PRs, applies the limit, parses payloads into `Issue`. | done |
| `src/issueflow/transforms/derived.py` | Pure `enrich(issue, as_of)` that computes the derived fields. | done |
| `src/issueflow/sinks/csv_sink.py` | Streaming writer for the three CSV tables. | planned |
| `src/issueflow/pipeline.py` | Wires source, transform and sink with a producer thread and bounded queue. | planned |
| `src/issueflow/aggregate.py` | Streams the CSVs and writes `by_label.csv`. | planned |
| `src/issueflow/config.py` | Settings: repo, limit, `as_of`, output directory, rate-limit cap. | planned |
| `src/issueflow/cli.py` | `issueflow` command-line entry point. | planned |
| `tests/` | Mirrors `src/`, using fake HTTP, clock and sleep. | done for built modules |

## Derived fields

All day counts are whole days, floored, measured against `as_of` (default: run start, UTC). A timestamp later than `as_of` counts as 0.

| Field | Rule |
|---|---|
| `open_age_days` | `as_of - created_at` for open issues; empty for closed issues. |
| `days_since_update` | `as_of - updated_at`. |
| `is_unassigned` | `true` when `assignees` is empty. |
| `is_stale` | Issue is open and `days_since_update > 30`. |

## Output CSVs

UTF-8, header row, ISO 8601 UTC timestamps, `true`/`false` booleans, empty string for null.

| File | Content |
|---|---|
| `issues.csv` | One row per issue: `number`, `id`, `title`, `state`, `state_reason`, `author`, `created_at`, `updated_at`, `closed_at`, `comments`, `milestone`, `html_url`, the four derived fields, `as_of`. |
| `issue_labels.csv` | `issue_number`, `label`. |
| `issue_assignees.csv` | `issue_number`, `assignee`. |

## Bonus: aggregate by label (`by_label.csv`)

One row per label, to show where the backlog is concentrated and neglected.

| Metric | Definition | Why |
|---|---|---|
| `total` | Issues with the label. | Size of the area. |
| `open` | Of those, currently open. | Current backlog. |
| `stale` | Open issues with `is_stale = true`. | Neglected work. |
| `unassigned` | Issues with `is_unassigned = true`. | Ownership gaps. |
| `median_open_age_days` | Median `open_age_days` of open issues. | How long work waits; the median resists a few very old issues. |
| `median_days_to_close` | Median whole days from `created_at` to `closed_at` for closed issues. | Resolution speed. |

## Run and test

```
.venv/bin/python -m pytest
```

The CLI (`issueflow --limit 200 --out out/`) arrives with the planned modules. Without a token the GitHub API allows 60 requests per hour (about 6,000 issues at 100 per page); the fetcher fails fast with a clear message rather than waiting out a long reset.
