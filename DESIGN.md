# Design decisions and trade-offs

This document explains why issueflow is built the way it is, what each choice costs, and what was left out on purpose. The README explains how it works; this explains why.

## Goals that shaped the design

1. **Never hold the full dataset in memory.** The repository has 19,000+ issues, and the same code should work for ten times that.
2. **Decouple the slow part from the rest.** The network is the bottleneck and the disk is not, so waiting on GitHub should not stop the writer, and a failure on one side should not lose the work of the other.
3. **Make it easy to extend.** New inputs, output formats and derived fields should each touch one place.
4. **Make it testable.** No test should need the network, the real clock or real sleeping.
5. **Fail clearly.** A rate limit or an outage should end with a short message, not a hang or a traceback.

## Architecture in one paragraph

Two programs share one Kafka topic. The producer (`issueflow produce`) fetches issues lazily and publishes them as messages, wrapped in a START and an END marker. The consumer (`issueflow consume`) reads one run, passes each issue through a pure transform into a sink that writes three normalized CSV files, then an aggregate step reads those files back. Each stage depends only on a small interface (`Source`, `Publisher`, `Reader`, `Sink`, and a plain function for the transform). `config` and `cli` are the only places that choose concrete implementations, and `messaging/kafka.py` is the only file that imports the Kafka client.

## Key decisions

### 1. Kafka between the fetcher and the CSV writer (replacing an in-process queue)
- **Choice:** the fetcher publishes issues to a Kafka topic, and a separate consumer reads them and writes the CSVs. An earlier version used a bounded `queue.Queue` between two threads.
- **Why:** the two halves have different speeds and failure modes (a rate-limited network on one side, a disk on the other). A durable log between them means either side can stop, fail or restart without refetching from GitHub. It also allows a run to be read again, and a second consumer (a database loader, say) could read the same topic.
- **Memory:** each program handles one issue at a time, so memory does not depend on the dataset size. The broker, not the program, stores the backlog, and a slow consumer cannot make the producer grow.
- **Cost:** one more moving part (a Docker container), one more dependency (`kafka-python-ng`), and a slower path for a small job than an in-process queue. For 19k issues the queue was enough; Kafka is chosen for decoupling and replay, not for throughput.
- **Client choice:** `kafka-python-ng` is pure Python, so it installs without a compiler or a native library. `confluent-kafka` is faster, but throughput is not the limit here, since GitHub's rate limit is.

### 2. One topic, one partition, and a START/END frame around each run
- **Choice:** topic `issueflow.issues` has one partition. Every run sends `START`, then one message per issue, then `END` with the count.
- **Why one partition:** Kafka orders messages only within a partition, and `aggregate_by_label` relies on issues arriving in the order they were fetched. A single partition gives that order for free.
- **Why the markers:** a topic has no natural end, so the consumer needs to know when a run is complete. `END` carries the number of issues sent, so the consumer can tell a complete run from one where the producer died, and a lost message from a clean finish. If fetching fails, the producer never sends `END`, so a half run is never mistaken for a whole one. This check already caught a real bug during development (see decision 3).
- **Cost:** one partition means one consumer in a group at a time. That is fine for a single ordered CSV writer and would not be for a high-volume stream. A topic can hold several runs, which is why every message carries a `run_id`.

### 3. At-least-once delivery, made safe by `seq` and a commit after the write
- **Choice:** the producer uses `acks=all` and waits for every acknowledgement before it finishes. The consumer turns auto-commit off and commits its position only after the CSV files are closed.
- **Why:** if the consumer crashes before the commit, the run is read again and written as a new set of files, never skipped. If the producer's send is retried and an issue is delivered twice, the consumer drops the copy using a per-run sequence number (`seq`).
- **Why `seq` and not the issue number:** the first version dropped issues whose number was not larger than the previous one. The real data broke that rule (issue 64 was created after issue 65, so a sorted-by-creation fetch is not sorted by number), and the END count check reported 149 of 150 issues. The sequence number is assigned by the producer, so it is always increasing.
- **Cost:** this is not exactly-once. A crash after the write but before the commit leaves one complete extra set of files, and the operator must pick the newest. Kafka transactions would remove that, at a cost in complexity that a CSV job does not need.
- **Abandoned runs:** if a new `START` arrives while a run is open, the consumer closes the unfinished files and starts the new run. Its partial files stay in the daily folder and are not marked as incomplete (see limitations).

### 4. Follow the `Link` header instead of counting pages
- **Choice:** the first request uses `page=1`; every later request uses the URL from `Link: rel="next"`, and the run ends when there is no `next` link.
- **Why:** this is GitHub's documented pagination contract and needs no knowledge of the page count.
- **Stable ordering:** requests use `sort=created&direction=asc`. New issues are created after the last page, so they do not shift earlier pages while a long run is in progress. Sorting by `updated` would move issues between pages mid-run and could skip or repeat them.

### 5. Pull requests are dropped in the source
- **Choice:** payloads with a `pull_request` key are skipped in `GitHubIssuesSource`, and `--limit` counts issues after that filter.
- **Why:** GitHub's issues endpoint returns pull requests too. A pull request is not an issue in our model, so nothing downstream should see one.
- **Cost:** pages are not full of issues, so a page of 100 may yield fewer than 100. The limit logic accounts for this, and it never fetches a page it does not need.

### 6. Separate layers for HTTP, retry and rate limiting
- **Choice:** `HttpClient` only does transport and raises `HttpError`. `RetryPolicy` decides what is transient. `RateLimiter` understands GitHub's quota headers. `GitHubIssuesSource` is the only place that combines them.
- **Why:** each layer is small and testable alone, and each takes injected `sleep`, `random` or `clock`, so tests run in milliseconds.
- **Retry rule:** retry 5xx, network errors and timeouts with exponential backoff and full jitter; never retry other 4xx. Full jitter spreads out retries so many clients do not hit the server at the same moment.
- **429 is not a retry case.** A rate-limit response will not clear by waiting a few seconds, so the limiter handles it using the reset time from the headers. The source waits and retries at most three times, then raises.

### 7. Rate limit: wait if short, fail fast if long
- **Choice:** if the quota is exhausted, sleep until the reset only when the wait is within `--max-wait` (60 seconds by default); otherwise stop with a message.
- **Why:** unauthenticated GitHub allows 60 requests per hour, and a reset can be up to an hour away. Sleeping silently for that long looks like a hang. Stopping lets the user choose.
- **Cost:** a full run of all issues needs `--max-wait 3600`, so the process can sleep through the hourly resets. A run that stops early leaves incomplete files, and the message says so.

### 8. No token support (deliberate scope cut)
- **Choice:** requests are unauthenticated.
- **Why:** the task needs a basic fetcher and a demonstration on a small sample, and an optional token adds configuration and secret handling that the evaluation does not need.
- **Cost:** the 60 requests per hour limit (about 6,000 issues). Adding `Authorization: Bearer <token>` is a few lines in `HttpClient` and raises the limit to 5,000 requests per hour; no other layer would change.

### 9. Normalized CSV output
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

### 10. Derived fields: pure, whole days, injected `as_of`
- **Choice:** `enrich(issue, as_of)` is a pure function. Day counts are whole days, floored. `as_of` defaults to the run start and is written on every row.
- **Why:** the same input always gives the same output, so boundaries are testable. Storing `as_of` makes a CSV reproducible and lets a reader tell how old the numbers are.
- **Rules:**
  - `open_age_days` is empty for closed issues, as the task defines it.
  - `is_stale` means open and `days_since_update > 30`. Flooring means 30 days 23 hours is not stale.
  - A timestamp after `as_of` (possible with a manual `--as-of` in the past) counts as 0 instead of going negative.
- **Cost:** flooring loses the fraction of a day, which does not matter against a 30-day threshold.
- **Note:** `updated_at` is GitHub's last-activity time (comments, labels and similar count). "Stale" therefore means no activity of any kind, not only no edit to the issue text.

### 11. Aggregate by label, as a streaming merge join
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
| Another consumer of the same data (a database loader, a search index) | A new reader with its own Kafka consumer group; the producer and the topic do not change. |
| Another message system (Pub/Sub, Redis Streams) | New `Publisher` and `Reader` classes next to `messaging/kafka.py`; `pipeline.py` does not change. |
| Another derived field | A field on `EnrichedIssue`, a line in `enrich`, a column in `csv_sink`. |
| Authentication | `HttpClient` headers; nothing else changes. |

## Testing approach

- 191 tests run in under a second with neither the network nor a Kafka broker: a fake HTTP opener, fake clock, sleep and random, and an in-memory fake Kafka producer and consumer that store the real encoded bytes.
- Boundaries are tested explicitly: stale at 30 versus 31 days, closed issues, empty assignees, `--limit` stopping early, retries giving up, rate-limit wait versus fail-fast, the START/END frame, a count mismatch, a missing END, duplicate and out-of-order messages (including issue numbers that do not increase), an abandoned run, and a commit only after a successful write.
- A real run against the GitHub API and a real Kafka broker (150 issues, produce then consume, replay with a new group, and a stopped broker) was done by hand and is shown in the README.

## Known limitations and next steps

- **No resume.** A failed or stopped run starts over and writes a new set of files beside the partial ones. A checkpoint (last `Link` URL or `created_at`) would fix it.
- **Output is not atomic.** A failed run leaves partial files in the daily folder, and nothing marks them as incomplete. Writing to a temporary name and renaming on success would make runs all-or-nothing.
- **Sequential fetching.** Pages are fetched one at a time, because each `Link` header gives the next URL and because the unauthenticated quota is the limit anyway. With a token, a small pool of fetchers could raise throughput, at the cost of fetch order, which the single partition and the merge join depend on.
- **Not exactly-once.** A consumer crash between writing the files and committing leaves one extra complete set of files; Kafka transactions would remove it.
- **Several producers at once are not supported.** Two runs sent at the same time interleave in the one partition. The consumer keeps them apart by `run_id`, but it reads only one run per invocation.
- **`run` skips older unread runs.** `issueflow run` reads its own run and commits past any earlier unread run, so those are not read later. Use `produce` and `consume` separately to keep every run.
- **No schema registry.** The message format is documented JSON checked on decode; a change to it needs the producer and consumer upgraded together. Old messages in a topic stay readable only by an old consumer.
- **Single broker.** The Compose file runs one broker with replication factor 1, which is right for a laptop and not for production.
- **Snapshot semantics.** Issues that change, close or are transferred while a run is in progress may appear in their state at fetch time. `as_of` records when the run began, not when each issue was read.
- **Approximate memory claim.** The producer and the consumer each hold one issue at a time (plus the Kafka client's small buffers); the aggregate holds per-label integer lists. None of them loads all rows, but the aggregate is not constant-memory.
