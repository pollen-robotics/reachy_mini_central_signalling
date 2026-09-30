# Fleet usage statistics

Central counts how many robots are online and how much they are used,
aggregates that into UTC-aligned 10-minute windows plus a daily rollup,
and (when configured) publishes the aggregates to a public Hugging Face
dataset. The status page (`/`) charts the dataset by fetching
`summary.json` from huggingface.co in the browser, so the charts put no
load on this server.

Code: `fleet_usage.py` (`UsageTracker`, `FleetUsagePublisher`, sinks,
configuration, status-page section) and `robot_kinds.py` (kind
classification shared with `/health`); `app.py` only wires the tracker
hooks, the lifespan tasks and the dev route. Tests: `test_usage.py`, plus
the status-page and `/health` tests at the end of `test_routes.py`.

## What is counted

Everything is broken down by robot kind (`reachy_mini`, `microduck`,
`other`; the same bounded classification as `/health`, see
[`META_CONTRACT.md`](META_CONTRACT.md)). No user name, robot name,
`hardware_id` or raw `meta` value is ever published.

**Small kinds.** Per-kind series are public at 10-minute resolution and
never identify a robot or its owner. But while a kind has very few robots
(for example the first Micro Duck units), its series can reflect one
person's usage pattern - when their robot is online, when it is used - to
someone who already knows who owns those robots. Owners distributing
early units should be aware of this. No small-count suppression is
applied.

### Robot identity (in memory only)

To count *distinct* robots, each producer is mapped to a key, an 8-byte
BLAKE2b digest of:

- `meta.hardware_id`, when it is a string of 1 to 128 characters
  (Reachy Mini daemons >= v1.7.2, Micro Duck);
- otherwise (absent, empty, not a string, or longer than 128 characters)
  the owner's HF username plus `meta.name`. Robots without `hardware_id`
  are legacy Reachy Mini daemons; with no `kind` they count as
  `reachy_mini`.

Consequences of the legacy fallback: one owner's legacy robots that
share the same name (every legacy daemon defaults to `reachy_mini`)
merge into a single robot, so an account with two unnamed legacy robots
counts as one; the same name under two accounts counts as two; renaming
a legacy robot makes it a new robot.

`hardware_id` is client-controlled, so the key sets are capped, per
window and per day: at most 20 distinct robots per HF username and 20000
per kind. Sightings over a cap are ignored and logged once per window /
day. An account cycling fake ids therefore adds at most 20 robots to any
public number, and memory stays bounded.

Keys live only in memory, for the current window and the current day.

### Distinct vs peak

- **`robots_distinct`**: number of different robots seen as a connected
  producer at any moment of the window. A robot online for one second
  counts once; a robot that disconnects and reconnects every few
  seconds also counts once (same key). Every registration and every
  `setPeerStatus` heartbeat marks the robot as seen.
- **`robots_peak`**: highest number of robots connected *at the same
  time* during the window. Concurrency is counted by distinct robot key,
  so a legacy daemon briefly holding two registrations (a half-open old
  socket plus the new one) is still one robot.

`distinct >= peak` always holds. Distinct measures reach ("how many
robots were used"), peak measures load ("how many at once").

### Why daily distinct robots is its own metric

A robot online all day appears in every one of the 144 windows, so the
daily number of distinct robots cannot be obtained by summing windows
(144x overcount) nor by taking the max over windows (undercounts robots
that were never online at the same time). It needs the set of robot keys
seen during the whole day, which is why the tracker keeps a per-day key
set and emits a separate daily row. The same holds for an hour: the
status page's 7-day view shows, per hour, the **largest per-window
distinct count** within the hour, not the distinct robots over the hour.

### Sessions

A **session** is a signalling session: a consumer (mobile/desktop app)
asked central to connect to one of its robots (`startSession`) and got
`sessionStarted`, until central removed the session. It measures that
someone opened a connection to a robot through central, and for how
long central considered it open.

It does **not** measure: whether WebRTC media actually flowed (ICE can
fail after signalling succeeded), local/LAN connections that never touch
central, what the user did during the session, or sessions refused
because the robot was busy (`sessionRejected`). A very short session is
often a failed or aborted connection attempt, hence the "under 10 s"
share on the status page.

The tracker mirrors central's session table exactly, including a
pre-existing quirk: a consumer that starts a second session while still
holding one orphans the first; it stays in central's table (and counts
as active for `sessions_peak`) until its producer ends it or
disconnects.

- **`sessions_started`**: sessions started in the window. The kind is
  the producer's kind at start.
- **`sessions_peak`**: highest number of simultaneous sessions in the
  window.
- **`session_durations`**: for sessions that **ended** in the window
  (a session spanning a boundary counts in the window where it ends):
  `count`, `sum_s`, `max_s` and `hist`. Durations are rounded to 0.1 s
  before being aggregated. `hist` has 6 buckets with inclusive upper
  bounds `10, 60, 300, 900, 3600` s and a last bucket for `> 3600` s
  (exactly 10 s lands in the first bucket). Mean = `sum_s / count`;
  the status page interpolates an approximate median from `hist`.
- **`session_end_reasons`**: sessions that ended in the window, by
  server code path (the client-sent `reason` string is never
  published):

  | Category | Server path |
  | --- | --- |
  | `ended` | explicit `endSession` from a peer (app or daemon) |
  | `withdrawn` | the producer sent `setPeerStatus(roles=[])` |
  | `peer_disconnected` | the producer's or the consumer's SSE channel closed |
  | `swept` | stale-producer sweep evicted the silent producer |
  | `replaced` | a newer registration with the same `hardware_id`/`install_id` evicted the producer |
  | `other` | anything else |

  Server restarts close every SSE channel, so sessions open at shutdown
  are counted as `peer_disconnected` in the last (partial) window.

### Coverage and partial rows

- **`coverage_s`** (window): seconds of the window during which this
  server process was running and tracking. The first window after a
  start is partial (e.g. started at 12:03:20 -> `coverage_s` 400 for
  the 12:00 window).
- **`coverage_s`** (day): sum of the day's window coverages.
- **`partial`** (day): `true` when the day was not fully observed by
  one process: the server started after 00:00 UTC, a window was skipped,
  the row is a shutdown snapshot of an unfinished day, or two processes'
  rows were merged.

If the tracker notices that whole windows went by without it running
(event loop stalled or process paused for more than a window), nothing
is emitted for those windows - a gap in the series means "not
observed" - and the next window's coverage starts when it resumed. A
wall clock stepping backwards never reopens a past window.

At a rollover the next window starts **seeded** with the current state:
robots still connected count as seen, and both peaks start at the
current concurrency. A robot connected across a boundary therefore
appears in both windows even if it sends nothing in the second one.

### Restart behaviour

The Space has no persistent disk, so in-memory state is lost on
restart. To limit the damage:

1. On shutdown, central makes one short (10 s overall) attempt to
   publish its pending rows plus a snapshot of the open window and the
   open day (both partial by construction).
2. The next process bootstraps lazily at its first publish (one publish
   interval after start, which also lets the old process finish its
   final commit): it downloads `data/daily.jsonl` and the
   `data/windows/<day>.jsonl` files of the last 8 days (needed to
   rebuild `summary.json`), then any older day file the first time it
   writes a row for that day. Only a Hub 404 means "file missing, start
   empty"; any other error (connection error, timeout, 5xx - which
   huggingface_hub reports as `LocalEntryNotFoundError`) fails the whole
   attempt before anything is adopted or committed, and it is retried
   next cycle. A transient Hub error therefore never makes central
   overwrite remote files with near-empty content.
3. Rows are deduplicated by `window_start` / `day`. When the remote row
   and the new row are **both partial** (typically the old process's
   shutdown snapshot and the new process's first window), they are
   merged: event counts (`sessions_started`, `session_durations`,
   `session_end_reasons`) and `coverage_s` (capped) are summed;
   `robots_distinct`, `robots_peak` and `sessions_peak` take the max,
   which is a lower bound since the two processes' robot sets cannot be
   combined exactly. Otherwise the new row wins. A row this process
   already published is always replaced, never merged twice, so retries
   are idempotent.

Sessions open across a restart are lost (they end as
`peer_disconnected` in the old process; clients reconnect and start new
ones in the new process).

## Dataset layout

```
summary.json                      # chart-only digest: the only file the status page fetches
data/daily.jsonl                  # one row per UTC day (full rows)
data/windows/YYYY-MM-DD.jsonl     # one row per 10-minute window of that UTC day (full rows)
```

All files are UTF-8, compact JSON; JSONL files are sorted by
`window_start` / `day`. Use the JSONL files for analysis; `summary.json`
is shaped for the page and may change with it.

### Window row (`data/windows/*.jsonl`)

```json
{
  "schema_version": 1,
  "window_start": "2026-09-30T12:00:00Z",
  "window_seconds": 600,
  "coverage_s": 600,
  "robots_distinct": {"reachy_mini": 212, "microduck": 9, "other": 0},
  "robots_peak": {"reachy_mini": 198, "microduck": 8, "other": 0},
  "sessions_started": {"reachy_mini": 31, "microduck": 2, "other": 0},
  "sessions_peak": {"reachy_mini": 14, "microduck": 1, "other": 0},
  "session_durations": {
    "reachy_mini": {"count": 29, "sum_s": 7310.4, "max_s": 2411.0, "hist": [6, 5, 9, 6, 3, 0]},
    "microduck": {"count": 2, "sum_s": 95.2, "max_s": 80.1, "hist": [0, 1, 1, 0, 0, 0]},
    "other": {"count": 0, "sum_s": 0.0, "max_s": 0.0, "hist": [0, 0, 0, 0, 0, 0]}
  },
  "session_end_reasons": {"ended": 25, "withdrawn": 2, "peer_disconnected": 4, "swept": 0, "replaced": 0, "other": 0}
}
```

Every per-kind dict carries every kind (zero-filled), in the order
above.

### Daily row (`data/daily.jsonl`)

```json
{
  "schema_version": 1,
  "day": "2026-09-30",
  "coverage_s": 86400,
  "partial": false,
  "robots_distinct": {"reachy_mini": 287, "microduck": 12, "other": 0},
  "sessions_started": {"reachy_mini": 2210, "microduck": 64, "other": 0},
  "session_durations": {"reachy_mini": {"count": 2203, "sum_s": 512301.7, "max_s": 30112.9, "hist": [410, 380, 700, 420, 250, 43]}, "...": "..."}
}
```

A daily row is emitted when the UTC day ends (or as a partial snapshot
on shutdown).

### `summary.json`

Columnar series, one array per metric, `null` where nothing was
observed. About 35 KB with full history (8 days of windows, 365 days):

```json
{
  "schema_version": 1,
  "generated_at": "2026-09-30T12:30:04Z",
  "window_seconds": 600,
  "kinds": ["reachy_mini", "microduck", "other"],
  "duration_buckets_s": [10, 60, 300, 900, 3600],
  "recent": {
    "start": "2026-09-29T12:30:00Z", "step_seconds": 600, "count": 144,
    "coverage_s": [600, 600, null, "..."],
    "robots_distinct": {"reachy_mini": [212, 214, null, "..."], "microduck": ["..."], "other": ["..."]},
    "robots_peak": {"...": "..."},
    "sessions_started": {"...": "..."},
    "sessions_peak": {"...": "..."},
    "durations": {"count": ["..."], "sum_s": ["..."], "max_s": ["..."], "hist": [[6, 5, 9, 6, 3, 0], "..."]}
  },
  "hourly": {"start": "2026-09-23T13:00:00Z", "step_seconds": 3600, "count": 168, "...": "same fields as recent"},
  "daily": {
    "days": ["2026-09-29", "..."],
    "coverage_s": [86400, "..."],
    "partial": [false, "..."],
    "robots_distinct": {"reachy_mini": [287, "..."], "microduck": ["..."], "other": ["..."]}
  }
}
```

- `recent`: the last 24 h, one slot per window, up to the open window
  (excluded).
- `hourly`: the last 7 days by UTC hour, the last slot being the current
  (partial) hour. Per hour: `robots_distinct`, `robots_peak` and
  `sessions_peak` are the max over the hour's windows (so hourly
  "distinct" is the busiest window's distinct count, not distinct over
  the hour); `sessions_started`, `durations` and `coverage_s` are sums.
- `durations` in both series are merged across robot kinds (the page
  shows fleet-wide duration statistics).
- `daily`: the last 365 daily rows, reduced to what the daily chart
  shows.

It is rewritten on every publish, in the same commit as the files it
summarises.

## Publishing

Every `FLEET_USAGE_PUBLISH_SECONDS` (default 1800 s; aggregation stays at
10 minutes) a background task takes the frozen rows (closed windows and
days), merges them into the files above and writes every changed file in
**one** `create_commit`. Nothing is committed when no row closed since
the last publish.

- The blocking Hub calls run in a dedicated daemon thread (never the
  shared default executor), under a 120 s timeout per attempt, and the
  huggingface_hub HTTP client is given a 30 s per-request timeout
  (its default is none). Only one attempt runs at a time: an attempt
  wedged on a half-dead connection makes later cycles skip, and never
  holds up shutdown or process exit.
- On any failure a warning is logged (token scrubbed) and the rows stay
  queued, bounded to 2000 rows (about 13 days; the oldest are dropped
  beyond that), for the next cycle.
- The signalling hot path never waits on, or fails because of, usage
  tracking or publishing: every tracker hook is wrapped and only logs
  on error.

`GET /health` exposes the publisher's aggregate state:

```json
"usage_publisher": {"enabled": true, "last_published_at": "2026-09-30T12:30:04Z", "pending_rows": 0, "dropped_rows": 0}
```

A growing `pending_rows` or a stale `last_published_at` means publishing
is failing; the Space logs say why.

## Configuration

All optional. With none set the tracker still runs in memory, nothing
is published and the status page has no usage section.

| Variable | Meaning |
| --- | --- |
| `FLEET_USAGE_DATASET` | Dataset repo id, e.g. `pollen-robotics/pollen_robotic_fleet_usage`. Must match `^[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*$` (otherwise ignored with a warning). Enables the status-page section. |
| `FLEET_USAGE_HF_TOKEN` | Write token for that dataset. Required to publish; without it the page still charts the dataset (read-only mirror). Never logged. |
| `FLEET_USAGE_PUBLISH_SECONDS` | Publish interval, default `1800`, minimum `5`. |
| `FLEET_USAGE_WINDOW_SECONDS` | Window length for local development only, default `600`; must divide 86400. Honoured only together with `FLEET_USAGE_LOCAL_DIR`; otherwise ignored with a warning, since a dataset must keep one window length. |
| `FLEET_USAGE_LOCAL_DIR` | Dev sink: write the same layout to this directory instead of HF, and serve its `summary.json` at `/dev/fleet-usage/summary.json` for the status page. Ignored (with a warning) whenever `SPACE_ID` is set, i.e. on any HF Space; the route is not registered at all then. Takes precedence over the dataset. |

### Operator setup

1. Create the dataset on huggingface.co as **public** (the page fetches
   it anonymously): `pollen-robotics/pollen_robotic_fleet_usage`. It can
   start empty; the first publish creates the files.
2. Create a **fine-grained** access token with write access to that
   dataset only (Settings -> Access Tokens -> Fine-grained ->
   Repositories permissions -> select the dataset -> write access to its
   contents). No other scope.
3. In the Space settings, add a **secret** `FLEET_USAGE_HF_TOKEN` with
   the token and a variable (or secret) `FLEET_USAGE_DATASET` with the
   repo id. Restart the Space.
4. Check the Space logs for `Fleet usage publishing to dataset ...` at
   start and `Fleet usage published to dataset ...` after the first
   interval, and `/health` `usage_publisher.last_published_at`; warnings
   name the failing step without the token.
5. Test / staging Spaces must use a **separate dataset** (e.g.
   `pollen-robotics/pollen_robotic_fleet_usage_test`) with its own token,
   so test traffic never lands in the production statistics.

### Maintenance: squash the history

Every publish is a commit (48 per day at the default interval) that
rewrites `summary.json` (~35 KB) and the current day file (up to ~100
KB), so the repo's git history grows by a few MB per day. Squash it
periodically, e.g. monthly, with a token that can write to the dataset:

```python
from huggingface_hub import HfApi

HfApi().super_squash_history(
    repo_id="pollen-robotics/pollen_robotic_fleet_usage", repo_type="dataset"
)
```

This keeps the current files and drops the history. A publish racing the
squash simply fails and is retried next cycle.

### Local development

```bash
DEV_TOKEN_SEED="tok-a:alice,tok-b:bob" \
FLEET_USAGE_LOCAL_DIR=/tmp/fleet_usage \
FLEET_USAGE_WINDOW_SECONDS=60 \
FLEET_USAGE_PUBLISH_SECONDS=20 \
uvicorn app:app --port 7860
```

The status page then charts `/tmp/fleet_usage/summary.json`.

## Known limits

- Two server processes publishing at the same time (overlapping
  deployments) would overwrite each other's rows for the overlap; the
  lazy bootstrap only protects the usual stop-then-start sequence.
- Distinct and peak values merged across a restart are lower bounds (see
  Restart behaviour).
