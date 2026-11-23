# Design decisions and trade-offs

This document explains why issueflow is built the way it is, what each choice costs, and what was left out on purpose. The README explains how it works; this explains why.

## Goals that shaped the design

1. **Never hold the full dataset in memory.** The repository has 19,000+ issues, and the same code should work for ten times that.
2. **Make the slow part overlap with the rest.** The network is the bottleneck, so waiting for it should not stop the writer, and the reverse.
3. **Make it easy to extend.** New inputs, output formats and derived fields should each touch one place.
4. **Make it testable.** No test should need the network, the real clock or real sleeping.
5. **Fail clearly.** A rate limit or an outage should end with a short message, not a hang or a traceback.

## Architecture in one paragraph

A source yields typed `Issue` objects lazily. A pipeline moves them through a pure transform into a sink, with the source running in a producer thread and a bounded queue in between. The CSV sink writes three normalized files as rows arrive. An aggregate step reads those files back. Each stage depends only on a small interface (`Source`, `Sink`, a plain function for the transform), and `config` and `cli` are the only places that choose concrete implementations.

## Key decisions

### 1. Streaming with generators and a bounded queue
- **Choice:** every stage passes one issue at a time; the queue between fetch and write holds at most 500 issues (about five pages).
- **Why:** memory use is fixed by the queue size, not the dataset size. The bound also gives backpressure: a slow disk slows the fetcher instead of letting it pile up pages.
- **Cost:** no cross-record operations in the pipeline (sorting, deduplicating, grouping). The aggregate step works around this by relying on file order (decision 6).

### 2. Producer/consumer threads, not parallel page fetching
- **Choice:** one fetch thread, one writer (the main thread).
- **Why:** the work is I/O-bound, so threads overlap network waits with parsing and writing without the cost of multiprocessing. Pages must be fetched in order anyway, because each page's `Link: rel="next"` header gives the next URL.
- **Rejected:** a thread pool fetching pages 1..N concurrently. It is faster on paper, but the unauthenticated limit is 60 requests per hour, so the quota, not latency, is the limit. It also returns pages out of order and makes `--limit` and error handling harder.
- **Cost:** fetch speed is bounded by one request's latency, about one second per page. At 100 issues per page the full dataset is roughly 190 requests.
- **Error handling:** an exception in the producer travels through the queue and is re-raised in the main thread. An exception in the main thread sets a stop flag so the producer exits instead of blocking forever on a full queue.

### 3. Follow the `Link` header instead of counting pages
- **Choice:** the first request uses `page=1`; every later request uses the URL from `Link: rel="next"`, and the run ends when there is no `next` link.
- **Why:** this is GitHub's documented pagination contract and needs no knowledge of the page count.
- **Stable ordering:** requests use `sort=created&direction=asc`. New issues are created after the last page, so they do not shift earlier pages while a long run is in progress. Sorting by `updated` would move issues between pages mid-run and could skip or repeat them.

### 4. Pull requests are dropped in the source
- **Choice:** payloads with a `pull_request` key are skipped in `GitHubIssuesSource`, and `--limit` counts issues after that filter.
- **Why:** GitHub's issues endpoint returns pull requests too. A pull request is not an issue in our model, so nothing downstream should see one.
- **Cost:** pages are not full of issues, so a page of 100 may yield fewer than 100. The limit logic accounts for this, and it never fetches a page it does not need.

### 5. Separate layers for HTTP, retry and rate limiting
- **Choice:** `HttpClient` only does transport and raises `HttpError`. `RetryPolicy` decides what is transient. `RateLimiter` understands GitHub's quota headers. `GitHubIssuesSource` is the only place that combines them.
- **Why:** each layer is small and testable alone, and each takes injected `sleep`, `random` or `clock`, so tests run in milliseconds.
- **Retry rule:** retry 5xx, network errors and timeouts with exponential backoff and full jitter; never retry other 4xx. Full jitter spreads out retries so many clients do not hit the server at the same moment.
- **429 is not a retry case.** A rate-limit response will not clear by waiting a few seconds, so the limiter handles it using the reset time from the headers. The source waits and retries at most three times, then raises.

### 6. Rate limit: wait if short, fail fast if long
- **Choice:** if the quota is exhausted, sleep until the reset only when the wait is within `--max-wait` (60 seconds by default); otherwise stop with a message.
- **Why:** unauthenticated GitHub allows 60 requests per hour, and a reset can be up to an hour away. Sleeping silently for that long looks like a hang. Stopping lets the user choose.
- **Cost:** a full run of all issues needs `--max-wait 3600`, so the process can sleep through the hourly resets. A run that stops early leaves incomplete files, and the message says so.

### 7. No token support (deliberate scope cut)
- **Choice:** requests are unauthenticated.
- **Why:** the task needs a basic fetcher and a demonstration on a small sample, and an optional token adds configuration and secret handling that the evaluation does not need.
- **Cost:** the 60 requests per hour limit (about 6,000 issues). Adding `Authorization: Bearer <token>` is a few lines in `HttpClient` and raises the limit to 5,000 requests per hour; no other layer would change.

### 8. Normalized CSV output
- **Choice:** three tables, written as three files: `issues`, `issue_labels`, `issue_assignees`.
- **Why:** labels and assignees are one-to-many. Putting several values in one cell would force every consumer to parse a delimiter and would make "issues with label X" awkward in SQL. Child files load as ordinary tables joined on `issue_number`.
- **Format:** UTF-8, header row, ISO 8601 UTC timestamps with second precision, `true`/`false` booleans, empty string for null. The `csv` module handles quoting, so commas, quotes, newlines and emoji in titles round-trip.
- **Columns:** core metadata only. The issue body is excluded: it is large, multi-line and rarely needed for these metrics.
- **Cost:** three files to keep in step. `CsvSink` writes them together, in the same issue order.

#### Where the files are stored
- **Choice:** `<out>/as_of=YYYY-MM-DD/<table>_<YYYYMMDDTHHMMSSZ>.csv`: a daily partition folder named after the run's `as_of` date, and file names made of the table name plus the creation timestamp.
- **Why:** the partition folder is the Hive convention, so databases and query engines treat `as_of` as a column and can load or prune by day. Keeping every run's files (instead of overwriting one `out/` folder) preserves history and lets two snapshots be compared. The creation timestamp tells runs on the same day apart, and all files of one run share it so they can be matched.
- **Why the table name stays in the file name:** three files cannot all be named by a bare timestamp, because they would collide, so each name is `table + timestamp`.
- **Folder vs file time:** the folder follows `as_of` (the data's reference time, which can be set with `--as-of`); the file name follows the real creation time. They differ when a run uses a past `as_of`.
- **Never overwrite:** files are opened in exclusive mode, so a clash raises an error instead of silently replacing data.
- **Cost:** folders grow with each run, and "the latest run" means the newest timestamp. Cleanup of old runs is left to the operator.

### 9. Derived fields: pure, whole days, injected `as_of`
- **Choice:** `enrich(issue, as_of)` is a pure function. Day counts are whole days, floored. `as_of` defaults to the run start and is written on every row.
- **Why:** the same input always gives the same output, so boundaries are testable. Storing `as_of` makes a CSV reproducible and lets a reader tell how old the numbers are.
- **Rules:**
  - `open_age_days` is empty for closed issues, as the task defines it.
  - `is_stale` means open and `days_since_update > 30`. Flooring means 30 days 23 hours is not stale.
  - A timestamp after `as_of` (possible with a manual `--as-of` in the past) counts as 0 instead of going negative.
- **Cost:** flooring loses the fraction of a day, which does not matter against a 30-day threshold.
- **Note:** `updated_at` is GitHub's last-activity time (comments, labels and similar count). "Stale" therefore means no activity of any kind, not only no edit to the issue text.

### 10. Aggregate by label, as a streaming merge join
- **Choice:** `aggregate_by_label` reads one run's issues and labels files together and writes a `by_label` file with the same timestamp in the same folder.
- **Why these metrics:**
  - `total`, `open`: how big an area is and how much of it is still open.
  - `stale`, `unassigned`: where work is neglected or has no owner.
  - `median_open_age_days`: how long open work has been waiting; the median is not distorted by a few very old issues.
  - `median_days_to_close`: how quickly an area gets resolved.
- **How it avoids a lookup table:** `CsvSink` writes child rows in the same order as the issues file, so the two files can be read side by side. A label file out of order raises an error instead of giving wrong numbers.
- **Cost:** memory holds per-label counters plus an integer list per label for the medians, so it grows with the number of labelled issues (small integers, a few hundred kilobytes for this repository), not with row size. Issues without labels are not counted. A very large input would call for an approximate median.

## Extensibility

| To add | Touch |
|---|---|
| Another source (GitLab, a file, a database) | One class with `iter_records()` and a parser to `Issue`. |
| Another output format (Parquet, SQLite) | One class implementing `Sink`. |
| Another derived field | A field on `EnrichedIssue`, a line in `enrich`, a column in `csv_sink`. |
| Authentication | `HttpClient` headers; nothing else changes. |

## Testing approach

- 143 tests run in under a second with no network: a fake HTTP opener, and fake clock, sleep and random.
- Boundaries are tested explicitly: stale at 30 versus 31 days, closed issues, empty assignees, `--limit` stopping early, retries giving up, rate-limit wait versus fail-fast, a full queue, and errors crossing the thread boundary in both directions.
- A real run against the API (150 issues, two requests) was done by hand and is shown in the README.

## Known limitations and next steps

- **No resume.** A failed or stopped run starts over and writes a new set of files beside the partial ones. A checkpoint (last `Link` URL or `created_at`) would fix it.
- **Output is not atomic.** A failed run leaves partial files in the daily folder, and nothing marks them as incomplete. Writing to a temporary name and renaming on success would make runs all-or-nothing.
- **Sequential fetching.** With a token, a small pool of fetch threads could raise throughput, at the cost of the ordering and error-handling points above.
- **Snapshot semantics.** Issues that change, close or are transferred while a run is in progress may appear in their state at fetch time. `as_of` records when the run began, not when each issue was read.
- **Approximate memory claim.** The pipeline holds about 500 issues; the aggregate holds per-label integer lists. Neither depends on the full dataset size in the way loading all rows would, but the aggregate is not constant-memory.
