# issueflow

Streams the open and closed issues of `microsoft/vscode` from the GitHub REST API (pull requests excluded), adds four derived fields to each issue, and writes CSV files ready for database import. Python 3.12, standard library plus pytest only.

**Status:** complete. Fetch, transform, CSV sink, pipeline, aggregate, config and CLI are built and covered by tests, and a live run against the GitHub API is shown under [Demo](#demo). Design decisions and trade-offs are in [DESIGN.md](DESIGN.md).

## How it works in one minute

1. A **fetch thread** asks GitHub for issues, 100 per page, and follows the `Link: rel="next"` header until the last page.
2. Each request goes through a **rate limiter** (wait for the quota to reset, or stop with a clear message) and a **retry policy** (backoff on 5xx, network errors and timeouts).
3. Pull requests are dropped, `--limit` is applied, and each payload becomes a typed `Issue`.
4. Issues cross a **bounded queue** to the **main thread**, which computes the derived fields and appends rows to three CSV files in a daily folder named `as_of=YYYY-MM-DD`.
5. A final **aggregate** step streams those CSVs and writes `by_label_<timestamp>.csv` in the same folder.

Every stage is a generator or a queue, so memory holds at most about 500 issues (the queue size) at a time, not the 19,000+ issues of the full dataset.

## Architecture

Two threads connected by one queue. Each box names its module, the technology it uses and what it is for. Data flows from top to bottom.

```
      ┌────────────────────────────────────────────────────────────────┐
      │ GitHub REST API                                                │
      │ Technology: HTTPS + JSON                                       │
      │ Use: source of the issues (open and closed, PRs included)      │
      └────────────────────────────────────────────────────────────────┘
                                       │
                                       │ JSON pages
                                       ▼
╔════════════════════════════════════════════════════════════════════════════╗
║ THREAD 1: FETCHER                                                          ║
║                                                                            ║
║   ┌────────────────────────────────────────────────────────────────────┐   ║
║   │ Fetcher        sources/http, retry, rate_limit, github             │   ║
║   │ Technology: urllib, Link-header paging, exponential backoff        │   ║
║   │ Use: downloads page after page, retries failures, waits out        │   ║
║   │      the rate limit, drops pull requests, applies --limit,         │   ║
║   │      turns each payload into an Issue                              │   ║
║   └────────────────────────────────────────────────────────────────────┘   ║
╚════════════════════════════════════════════════════════════════════════════╝
                                       │
                                       │ Issue
                                       ▼
    ┌────────────────────────────────────────────────────────────────────┐
    │ Queue          pipeline.py                                         │
    │ Technology: queue.Queue, bounded to 500 issues                     │
    │ Use: hands issues from thread 1 to thread 2; blocks the fetcher    │
    │      when the writer falls behind, so memory stays flat            │
    └────────────────────────────────────────────────────────────────────┘
                                       │
                                       │ Issue
                                       ▼
╔════════════════════════════════════════════════════════════════════════════╗
║ THREAD 2: MAIN                                                             ║
║                                                                            ║
║     ┌────────────────────────────────────────────────────────────────┐     ║
║     │ Transform      transforms/derived.py                           │     ║
║     │ Technology: one pure Python function, datetime maths           │     ║
║     │ Use: adds open_age_days, days_since_update,                    │     ║
║     │      is_unassigned, is_stale                                   │     ║
║     └────────────────────────────────────────────────────────────────┘     ║
║                                      │                                     ║
║                                      ▼                                     ║
║     ┌────────────────────────────────────────────────────────────────┐     ║
║     │ CSV sink       sinks/csv_sink.py                               │     ║
║     │ Technology: csv module, one row at a time                      │     ║
║     │ Use: writes the three CSV files below, in the daily folder     │     ║
║     │      for the run's as_of date; never overwrites a file         │     ║
║     └────────────────────────────────────────────────────────────────┘     ║
╚════════════════════════════════════════════════════════════════════════════╝
                                       │
                                       │ rows
                                       ▼
      ┌────────────────────────────────────────────────────────────────┐
      │ Daily partition folder      out/as_of=2026-11-23/              │
      │   issues_20261123T090233Z.csv                                  │
      │   issue_labels_20261123T090233Z.csv                            │
      │   issue_assignees_20261123T090233Z.csv                         │
      │ Folder = as_of date. File = table name + creation timestamp.   │
      └────────────────────────────────────────────────────────────────┘
                                       │
                                       │ read back
                                       ▼
      ┌────────────────────────────────────────────────────────────────┐
      │ Aggregate      aggregate.py                                    │
      │ Technology: csv + statistics, streaming merge join             │
      │ Use: counts and medians per label, from this run's files       │
      └────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
      ┌────────────────────────────────────────────────────────────────┐
      │ by_label_20261123T090233Z.csv   saved in the same daily folder │
      │ bonus: where work piles up or goes stale                       │
      └────────────────────────────────────────────────────────────────┘
```

`pipeline.py` starts the fetcher thread, owns the queue and runs the main thread. `cli.py` and `config.py` read the options and choose the concrete source and sink.

### Modules at a glance

| Module | What it does | Technology |
|---|---|---|
| `models.py` | Defines `Issue` and `EnrichedIssue`, the data handed from stage to stage. | `dataclasses` (frozen) |
| `sources/base.py` | Defines the `Source` contract: a lazy stream of raw records. | `typing.Protocol` |
| `sources/http.py` | Makes one GET request and raises `HttpError` on any non-2xx reply. | `urllib.request`, `json` |
| `sources/retry.py` | Retries temporary failures (5xx, network errors, timeouts) with growing, randomized delays. | exponential backoff, full jitter |
| `sources/rate_limit.py` | Reads GitHub's quota headers and waits for the reset, or stops if the wait is too long. | `X-RateLimit-*`, `Retry-After` headers |
| `sources/github.py` | Fetches every page of issues, skips pull requests, applies `--limit`, and parses each into an `Issue`. | `Link` header paging, `datetime` |
| `transforms/derived.py` | Computes `open_age_days`, `days_since_update`, `is_unassigned` and `is_stale` for one issue. | pure function, `datetime` |
| `sinks/base.py` | Defines the `Sink` contract: write one item, close. | `typing.Protocol` |
| `sinks/csv_sink.py` | Writes each issue as one row in the issues file plus child rows in the labels and assignees files, inside the daily `as_of=` folder. | `csv` module, streaming, `pathlib` |
| `pipeline.py` | Runs fetcher and writer at the same time and passes errors between the two threads. | `threading`, `queue.Queue` |
| `aggregate.py` | Reads one run's issues and labels CSVs and writes per-label counts and medians to a `by_label` CSV. | `csv`, `statistics`, merge join |
| `config.py` | Holds the validated run settings: repo, limit, `as_of`, output folder, wait cap. | frozen `dataclass` |
| `cli.py` | Parses options, wires everything together, prints a summary, and turns failures into exit codes. | `argparse` |
| `__main__.py` | Lets you run the tool as `python -m issueflow`. | standard module entry point |

### What one page request does

```
GitHubIssuesSource._get(url, params)
  │
  ├─► limiter.before_request()      quota exhausted? sleep until reset if the wait is short,
  │                                 otherwise raise RateLimitExceeded (stop the run)
  │
  ├─► client.get(url, params)       ── 2xx ──► limiter.update(headers) ──► return Response
  │        │
  │        └── HttpError ──► limiter.update(headers), then:
  │              5xx / URLError / timeout ... RetryPolicy sleeps (backoff) and calls again
  │              403/429 rate limited ....... wait via limiter, retry (at most 3 times)
  │              any other 4xx .............. raised at once, never retried
  │
  └─► _pages() yields resp.json, then follows the Link rel="next" URL (no params: the URL carries them)
```

## Data model

`src/issueflow/models.py` defines what each stage hands to the next.

| Type | Fields |
|---|---|
| `Issue` | `number`, `id`, `title`, `state` (`open`/`closed`), `state_reason`, `author`, `created_at`, `updated_at`, `closed_at`, `comments`, `milestone`, `html_url`, `labels` (tuple of names), `assignees` (tuple of logins). All datetimes are timezone-aware UTC. |
| `EnrichedIssue` | `issue`, `as_of`, plus the four derived fields below. |

## Derived fields

All day counts are **whole days, floored**, measured against `as_of` (default: run start, UTC). A timestamp later than `as_of` counts as 0.

| Field | Rule | Edge cases |
|---|---|---|
| `open_age_days` | `as_of - created_at` | Empty for closed issues. |
| `days_since_update` | `as_of - updated_at` | Computed for open and closed issues. |
| `is_unassigned` | `assignees` is empty | A single assignee is enough to make it `false`. |
| `is_stale` | open and `days_since_update > 30` | 30 days 23 hours floors to 30, so it is not stale; 31 days is. Closed issues are never stale. |

## Output CSVs

### Where the files go

Each run writes into a **daily partition folder named after its `as_of` date**, and every file is named **table name + the time the file was created**:

```
out/
└── as_of=2026-11-23/                           <- partition: the run's as_of date (UTC)
    ├── issues_20261123T090233Z.csv             <- table name + creation timestamp (UTC)
    ├── issue_labels_20261123T090233Z.csv
    ├── issue_assignees_20261123T090233Z.csv
    └── by_label_20261123T090233Z.csv           <- the aggregate, same run, same timestamp
```

- **Folder:** `as_of=YYYY-MM-DD` is a Hive-style partition name that databases and query engines read as a column. It follows `as_of`, so `--as-of 2026-01-31T00:00:00Z` writes to `as_of=2026-01-31`.
- **File name:** `<table>_<YYYYMMDDTHHMMSSZ>.csv`. The timestamp is when the run created the files, and all files of one run share it. The table name is kept in the name because three files cannot all be called just a timestamp.
- **Several runs in one day** add more files to the same folder, so a day's history is kept. Existing files are never overwritten; a clash (the same second) stops with an error instead.
- **Reading a run:** match files by the shared timestamp. The latest run of a day is the newest timestamp.

### Format and columns

UTF-8, header row, ISO 8601 UTC timestamps with second precision (`2025-02-01T08:30:00Z`), `true`/`false` booleans, empty string for null. Labels and assignees live in child files so the data imports cleanly into a relational database.

| Table (file prefix) | Columns |
|---|---|
| `issues` | `number`, `id`, `title`, `state`, `state_reason`, `author`, `created_at`, `updated_at`, `closed_at`, `comments`, `milestone`, `html_url`, `open_age_days`, `days_since_update`, `is_unassigned`, `is_stale`, `as_of` |
| `issue_labels` | `issue_number`, `label` |
| `issue_assignees` | `issue_number`, `assignee` |

## Bonus: aggregate by label (`by_label_<timestamp>.csv`)

One row per label, to show where the backlog is concentrated and neglected.

| Metric | Definition | Why |
|---|---|---|
| `total` | Issues with the label. | Size of the area. |
| `open` | Of those, currently open. | Current backlog. |
| `stale` | Open issues with `is_stale = true`. | Neglected work. |
| `unassigned` | Issues with `is_unassigned = true`. | Ownership gaps. |
| `median_open_age_days` | Median `open_age_days` of open issues. | How long work waits; the median resists a few very old issues. |
| `median_days_to_close` | Median whole days from `created_at` to `closed_at` for closed issues. | Resolution speed. |

## File guide

Files are listed in data-flow order.

### `src/issueflow/models.py` (built)
Frozen dataclasses `Issue` and `EnrichedIssue` (see Data model). Frozen so a stage cannot change data another stage still holds.

### `src/issueflow/sources/base.py` (built)
- `Source.iter_records() -> Iterator[dict]`: the contract every source follows, a lazy single pass over raw records.
- **Why:** the pipeline depends on this protocol, so adding another source (for example GitLab) means writing one new class.

### `src/issueflow/sources/http.py` (built)
- `HttpClient(opener=urlopen, timeout=30, headers=None).get(url, params) -> Response`: one GET with `urllib`. Encodes `params`, drops `None` values, parses the JSON body (empty body gives `None`), and lower-cases header names. Sends a default `User-Agent` because GitHub rejects requests without one.
- `Response(status, headers, json)`: the parsed result.
- `HttpError(status, headers, body)`: raised for every non-2xx response, so callers decide what each status means.
- **Why:** transport only. The `opener` argument lets tests inject a fake instead of using the network.

### `src/issueflow/sources/retry.py` (built)
- `RetryPolicy(max_attempts=5, base_delay=1, max_delay=30).call(fn)`: runs `fn`, retrying transient failures and re-raising the last error when attempts run out. No sleep after the final attempt.
- `delay_for(attempt)`: full jitter, `random() * min(max_delay, base_delay * 2**(attempt-1))`.
- `is_retryable(exc)`: true for `HttpError` with status 500 or above, `URLError` and `TimeoutError`. Every other 4xx, including 429, is never retried here.
- **Why:** full jitter spreads retries so clients do not retry in lockstep. `sleep` and `random` are injected so tests run instantly and deterministically. 429 is left to the rate limiter, which knows when the quota resets.

### `src/issueflow/sources/rate_limit.py` (built)
- `RateLimiter(max_wait=60, clock, sleep)`:
  - `update(headers)` records `X-RateLimit-Remaining`/`X-RateLimit-Reset`, or `Retry-After` (treated as quota 0 until that moment).
  - `before_request()` returns at once if quota remains. If it is exhausted, it sleeps until the reset when the wait is at most `max_wait`, otherwise raises `RateLimitExceeded`.
- `is_rate_limit_error(exc)`: true for a 403 or 429 `HttpError` with remaining quota `0` or a `Retry-After` header. A plain 403 permission error does not match.
- **Why:** unauthenticated GitHub allows 60 requests per hour. Waiting an hour would look like a hang, so long waits fail fast with a message instead.

### `src/issueflow/sources/github.py` (built)
- `GitHubIssuesSource(owner="microsoft", repo="vscode", *, limit=None, per_page=100, client, retry, limiter)`: the GitHub-specific source.
  - `iter_records()`: yields raw issue dicts. Drops pull requests (payloads with a `pull_request` key), counts `limit` after that filter, and stops without fetching a page it does not need.
  - `iter_issues()`: the same stream, mapped through `parse_issue` into `Issue` objects.
  - `_pages()`: first request is `GET /repos/{owner}/{repo}/issues` with `state=all`, `sort=created`, `direction=asc`, `per_page=100`, `page=1`; later pages follow the `Link` header.
  - `_get(url, params)`: combines limiter, retry and the HTTP client as shown in "What one page request does". Rate-limit waits are capped at 3 per page, then the error is raised.
- `next_url(link_header)`: extracts the `rel="next"` URL, or `None` on the last page.
- `parse_issue(raw)`: the only place that knows GitHub's JSON shape. Converts timestamps to UTC, flattens `user`, `milestone`, `labels` and `assignees`, and tolerates null fields.
- **Why:** the `Link` header is GitHub's documented way to paginate. `sort=created asc` keeps pages stable when new issues arrive during a run. Dropping pull requests here is right because a PR is not an issue in our model.

### `src/issueflow/transforms/derived.py` (built)
- `enrich(issue, as_of) -> EnrichedIssue`: pure function that applies the four rules in "Derived fields".
- `_whole_days(earlier, later)`: floored whole days, clamped at 0.
- **Why:** no I/O and no clock, because `as_of` is a parameter. The same input always gives the same output, which makes the stale boundary easy to test.

### `src/issueflow/sinks/base.py` (built)
- `Sink`: the contract every output follows: `write(item: EnrichedIssue)`, `close()`, and use as a context manager.
- **Why:** the pipeline depends on this protocol, so a new format (Parquet, SQLite) is a new sink, not a change to the pipeline.

### `src/issueflow/sinks/csv_sink.py` (built)
- `CsvSink(out_dir, as_of, *, clock=...)`: creates the daily folder `<out_dir>/as_of=YYYY-MM-DD/`, opens `issues_<ts>.csv`, `issue_labels_<ts>.csv` and `issue_assignees_<ts>.csv` once, writes the headers, then for every `EnrichedIssue` appends one row to the issues file plus one row per label and per assignee to the child files. `<ts>` is the creation time from `clock`, shared by all files of the run. Files are opened in exclusive mode, so an existing file is never overwritten. `close()` (or leaving the `with` block, even on an error) flushes and closes all three files.
- Exposes `directory`, `stamp`, `issues_path`, `labels_path`, `assignees_path` and `path_for(table)`, so later steps (the aggregate) find this run's files without guessing names.
- `partition_dir(root, as_of)` and `file_stamp(created)`: build the folder and timestamp text; both use UTC.
- `issue_row(item)`: formats one issues row. Timestamps become UTC with second precision, booleans become `true`/`false`, and nulls become empty strings.
- `ISSUE_COLUMNS`, `LABEL_COLUMNS`, `ASSIGNEE_COLUMNS` and the `*_TABLE` name constants: the schema in one place.
- **Why:** writing row by row keeps memory flat. Child files avoid packing several values into one cell, so the CSVs import into a relational database without parsing. A dated partition folder with timestamped files keeps every run and lets a database load one day, or one run, at a time.

### `src/issueflow/pipeline.py` (built)
- `run_pipeline(issues, transform, sink, *, queue_size=500) -> int`: moves issues from any iterable through `transform` into `sink` and returns how many were written. A **producer thread** pulls from `issues` into a bounded `queue.Queue`; the calling thread takes items off the queue, transforms and writes them.
- A sentinel marks the end of the stream. An exception in the producer travels through the queue and is **re-raised in the main thread**. An exception in the main thread (sink or transform) sets a stop flag so the producer exits instead of blocking on a full queue, and closes the source generator.
- The caller owns the sink and closes it (`with CsvSink(...)`).
- **Why:** network waits and disk writes overlap, and the bound caps memory at about `queue_size` issues. The function knows nothing about GitHub or CSV, so it works with any source and sink.

### `src/issueflow/aggregate.py` (built)
- `aggregate_by_label(issues_path, labels_path, output_path) -> int`: reads one run's issues and labels files and writes the by-label file, returning the number of label rows.
- The two files are streamed together as a **merge join**: `CsvSink` writes the labels file in the same order as the issues file, so no issue lookup table is needed. Memory holds per-label counters and the integer lists for the medians. A label file out of order raises `ValueError`.
- Rows are sorted by `total` (largest first), then label name. Issues without labels do not appear.
- **Why:** it reads the CSV output, as the task asks, and shows that the output schema is enough to answer questions without going back to the API. It takes explicit paths, so it works on any run, not only the latest.

### `src/issueflow/config.py` (built)
- `Settings`: a frozen dataclass with `repo` (`owner/name`), `limit`, `as_of` (default: now, UTC), `out_dir` (the root; the daily folder is created inside it) and `rate_limit_max_wait`. It validates its input (repo format, non-negative limit, timezone-aware `as_of`).
- **Why:** one validated object instead of loose arguments, so the rest of the code never re-checks them.

### `src/issueflow/cli.py` and `__main__.py` (built)
- `main(argv=None, *, client=None) -> int`: parses `--repo`, `--limit`, `--as-of`, `--out`, `--max-wait`, builds the source, runs the pipeline inside a `CsvSink`, then runs the aggregate on that run's files. `client` lets tests inject a fake HTTP client.
- Exit codes: `0` success, `1` for a rate limit that is too long or a failed request (one-line message, no traceback), `2` for invalid options, `130` on Ctrl-C. A failed run says the output is incomplete.
- `parse_as_of(text)`: ISO 8601; a value without a timezone is taken as UTC.
- `python -m issueflow` runs `main()`.

### `tests/` (built)
- Mirrors `src/`. Tests inject a fake `opener`, clock, `sleep` and `random`, so they run in milliseconds and never touch the network.
- The suite (143 tests) covers retry counts and give-up, the rate-limit wait and fail-fast paths, Link pagination, PR filtering, limit edge cases, the stale boundary at 30 versus 31 days, CSV formatting, the merge join, and the pipeline's bounded queue and error propagation in both directions.

## Design principles

- **Streaming:** generators and a bounded queue, never a full in-memory dataset.
- **Overlap:** fetching and writing run at the same time in two threads.
- **Deterministic:** `as_of`, the clock, sleep and randomness are injected.
- **Extensible:** a new source, derived field or output format changes one file, because stages meet at small interfaces.

## Run and test

Python 3.12 is required (the code uses `str | None` and generic function syntax).

```
.venv/bin/python -m pytest                                          # all tests, no network needed
PYTHONPATH=src .venv/bin/python -m issueflow --limit 200 --out out  # small live sample
PYTHONPATH=src .venv/bin/python -m issueflow --out out              # everything (19k+ issues)
```

Options: `--repo owner/name`, `--limit N`, `--as-of 2026-01-31T00:00:00Z`, `--out DIR` (root folder; default `out`), `--max-wait SECONDS`. Without a token the GitHub API allows 60 requests per hour, about 6,000 issues at 100 per page. The fetcher waits for a quota reset only if it is within `--max-wait` seconds (60 by default); otherwise it stops with a clear message and says the output is incomplete. A full run of all 19k+ issues therefore needs `--max-wait 3600`, which lets it sleep through the roughly three hourly resets. See [DESIGN.md](DESIGN.md) for why there is no token support.

## Demo

A real run, `issueflow --limit 150 --out out` (2 requests, about 2 seconds):

```
wrote 150 issues and 31 labels to out/as_of=2026-11-23/ (files stamped 20261123T091159Z)
```

First rows of the by-label file (these are the oldest issues in the repository, because pages are fetched oldest first):

| label | total | open | stale | unassigned | median_open_age_days | median_days_to_close |
|---|---|---|---|---|---|---|
| verified | 58 | 0 | 0 | 0 | | 6 |
| bug | 50 | 0 | 0 | 1 | | 7 |
| feature-request | 39 | 1 | 1 | 17 | 4022 | 201.5 |
| on-testplan | 8 | 0 | 0 | 2 | | 50.5 |
| help wanted | 7 | 0 | 0 | 2 | | 117 |

Files created by that run:

```
out/as_of=2026-11-23/issues_20261123T091159Z.csv
out/as_of=2026-11-23/issue_labels_20261123T091159Z.csv
out/as_of=2026-11-23/issue_assignees_20261123T091159Z.csv
out/as_of=2026-11-23/by_label_20261123T091159Z.csv
```

The output directory `out/` is gitignored.
