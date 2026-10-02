# TracePath

TracePath is a small backend for distributed tracing. It ingests spans, rebuilds
each trace into a span tree, derives the exclusive time of every span, finds the
critical path through the trace, marks how failures propagated through the tree,
and aggregates a service dependency graph.

The release deliberately supports a compact public contract:

- a span names a trace, a span id, an optional parent span id, a service, an
  operation, a kind, a start instant, a duration in nanoseconds and a status;
- the root span declares `span_count`, the total number of spans of the trace,
  including itself;
- spans may arrive in any order, so the parent may still be missing when a child
  arrives; a trace is only readable once it is complete;
- a child must be contained in its parent, siblings must not overlap, and a span
  is refused when those rules are violated by spans that are already stored;
- everything else is validated when the trace is reassembled, and reported as
  violations instead of guessed or silently dropped;
- the sum of `self_time_ns` along the critical path is the root duration;
- duplicate ingestion with the same `Idempotency-Key` returns the first result.

## Requirements

- Go 1.22 or newer
- no third-party dependencies

## Build and run

```bash
go build -o tracepath .
./tracepath --host 127.0.0.1 --port 8080 --database tracepath.db
```

The process prints `TracePath listening on http://127.0.0.1:8080` after it has
bound the port. `--database` is a JSON file that is rewritten atomically after
every accepted span; omitting it uses `tracepath.db`.

## Model

### Span

A span is exactly these fields; unknown fields are rejected.

```json
{
  "trace_id": "t-1",
  "span_id": "s-db",
  "parent_id": "s-root",
  "service": "db",
  "operation": "select",
  "kind": "client",
  "start_time": "2024-06-01T00:00:00.000000350Z",
  "duration_ns": 600,
  "status": "ok",
  "attributes": {"db.statement": "select 1"},
  "span_count": 6
}
```

| Field | Rules |
| --- | --- |
| `trace_id` | required, 1–64 characters, the same for every span of a trace |
| `span_id` | required, 1–64 characters, unique inside its trace |
| `parent_id` | `null` for the root, otherwise the id of another span of the same trace |
| `service`, `operation` | required, 1–128 characters |
| `kind` | one of `internal`, `server`, `client`, `producer`, `consumer` |
| `start_time` | required RFC3339 with optional fractional seconds, stored in UTC |
| `duration_ns` | required integer, zero or positive, nanoseconds |
| `status` | `ok` or `error` |
| `attributes` | optional object, at most 32 entries, key ≤ 64 chars, value ≤ 512 chars |
| `span_count` | only on a root span, required there, 1–100000 |

Timestamps are normalised with `time.RFC3339Nano` and re-rendered as
`2024-06-01T00:00:00.00000035Z`. Instants are compared with nanosecond
precision, which is the precision of the whole service.

### Tree and time rules

One trace has exactly one root, and every span of the trace must be reachable
from it. Three rules are checked:

1. **containment** — a span's `[start_time, start_time + duration_ns)` interval
   must lie inside its parent's interval;
2. **sibling order** — direct children of the same span must not overlap, so
   every pair of siblings is disjoint (touching ends are allowed);
3. **subtree size** — a span's whole subtree must fit in the span, which follows
   from containment and is checked separately as a cross-check.

The first two are checked at ingestion against the spans that already exist, and
all three are checked again when the trace is assembled. A rule that is only
violated by a span arriving later is therefore reported as a violation rather
than as a rejection, which is what makes out-of-order ingestion safe.

### `self_time_ns`

`self_time_ns` is the exclusive time of a span: everything in its duration that
is not carried by the single longest chain below it.

```
longest(t)   = duration(t)                        if t has no children
longest(t)   = self_time(t) + max(longest(c))     otherwise
self_time(t) = duration(t) - max(longest(c))      over the children c of t
```

For a leaf, `self_time_ns` equals `duration_ns`. For an interior span it is the
sum of all its gaps plus every branch that is not part of the longest chain, so
it is always non-negative once the child rules above hold.
`subtree_duration_ns` is the duration of the longest chain below a span and is
exposed so a caller can check the recurrence.

Worked example (the fixture used by the tests and by the examples below):

```
gateway    0–1000    children auth, db, queue
├─ auth    100–300
├─ db      350–950   children query1 400–600, query2 650–900
│  ├─ query1  400–600
│  └─ query2  650–900   status error
└─ queue   950–1000

query1: leaf                     self = 200
query2: leaf                     self = 250
db:     longest chain below is query2 (250), duration 600   self = 350
auth:   leaf                                                self = 200
queue:  leaf                                                self =  50
gateway: longest chain below is db (subtree 600), duration 1000  self = 400

critical path gateway → db → query2: 400 + 350 + 250 = 1000 = gateway duration
```

### Critical path

The critical path is the chain from the root downwards whose `self_time_ns`
values add up to the largest total. It is reported with `length_ns` equal to the
sum of the `self_time_ns` of its steps, which by construction equals the root
`duration_ns`:

```
sum(self_time along the critical path) = root duration
```

The path is chosen by walking down from the root and always taking the branch
with the longest chain below it; ties are broken by the longest sub-chain and
then by the lexicographically smallest span id sequence, so the same trace always
yields the same path.

`self_time_ns` and the critical path are derived from the same recurrence, and
the wording matters: `self_time` is *not* simply `duration - sum(child
durations)`. That simpler formula makes the sum of a path's self times equal the
root duration only when every span's children happen to tile its duration
exactly. Routing the exclusive time down the critical branch instead makes the
invariant hold for every valid trace, including branched and partially covered
ones, while keeping `self_time_ns` non-negative. `subtree_duration_ns` is
published on every node so the recurrence and the invariant can both be checked
by hand, and the test suite asserts the invariant on a leaf, a chain and a
branched trace.

## HTTP API

All request and response bodies are JSON, sent with `Content-Type:
application/json`. Query parameters that a route does not document are rejected.

### Health

```http
GET /health
```

Returns `{"status":"ok"}`.

### Ingest a span

```http
POST /spans
Idempotency-Key: span-1
Content-Type: application/json

{
  "trace_id": "t-1",
  "span_id": "s-root",
  "parent_id": null,
  "service": "gateway",
  "operation": "GET /checkout",
  "kind": "server",
  "start_time": "2024-06-01T00:00:00Z",
  "duration_ns": 1000,
  "status": "ok",
  "attributes": {},
  "span_count": 6
}
```

Returns HTTP 201 with the stored span plus its derived state:

```json
{
  "trace_id": "t-1",
  "span_id": "s-root",
  "parent_id": null,
  "service": "gateway",
  "operation": "GET /checkout",
  "kind": "server",
  "start_time": "2024-06-01T00:00:00Z",
  "duration_ns": 1000,
  "status": "ok",
  "attributes": {},
  "accepted": true,
  "span_count": 6,
  "complete": true,
  "valid": true,
  "violations": []
}
```

`complete` says every declared span of the trace has arrived, `valid` says the
trace has no violation so far, and `violations` explains anything already wrong.
While a trace is still filling up, `violations` therefore lists what the stored
spans cannot yet explain — typically `no_root_span` plus `orphan_span` entries —
and the last span of a correct trace answers with `complete: true`, `valid: true`
and an empty list. Those intermediate reports are informational; the trace is
only *unreadable* because it is incomplete, never because a span was accepted and
then retracted. The `Idempotency-Key` header is required; repeating the request
with the same key and the same span returns the identical body. Reusing a key for
another operation, or for another span, is a `conflict` (409).

Ingestion is refused with HTTP 409 when the span contradicts the stored spans:

- its parent is in another trace;
- a span with that id already exists in the trace;
- its interval is not contained in a parent that is already stored.

### Read a trace

```http
GET /traces/{trace_id}
```

Returns the reassembled trace. `spans` holds every node once in pre-order, and
`root` holds the same tree with `children` nested:

```json
{
  "trace_id": "t-1",
  "duration_ns": 1000,
  "span_count": 6,
  "service_count": 4,
  "error_spans": 1,
  "declared_span_count": 6,
  "complete": true,
  "valid": true,
  "violations": [],
  "root": {"span_id": "s-root", "self_time_ns": 400, "children": []},
  "spans": []
}
```

Each node carries `end_time` (start plus duration), `self_time_ns`,
`subtree_duration_ns`, `depth`, the error fields below, `attributes` and
`children`.

An incomplete or unusable trace answers HTTP 409 with code `trace_invalid` and
the full list:

```json
{"error":{"code":"trace_invalid","message":"trace t-1 violates span invariants: ...",
 "trace_id":"t-1","violations":[
   {"code":"time_not_contained","trace_id":"t-1","span_id":"s-x","message":"..."}]}}
```

The violation codes are:

| Code | Meaning |
| --- | --- |
| `no_root_span` | no span of the trace has a null `parent_id` |
| `multiple_root_spans` | more than one span claims to be the root |
| `orphan_span` | a span references a parent that is not part of the trace |
| `unreachable_span` | a span cannot be reached from the root |
| `trace_incomplete` | fewer spans arrived than `span_count` declares |
| `span_count_exceeded` | more spans arrived than `span_count` declares |
| `time_not_contained` | a span is not inside its parent, or its subtree is larger |
| `sibling_overlap` | two direct children of the same span overlap in time |

### Critical path

```http
GET /traces/{trace_id}/critical-path
```

```json
{
  "trace_id": "t-1",
  "method": "longest chain below the root, weighted by self_time_ns; ...",
  "trace_duration_ns": 1000,
  "length_ns": 1000,
  "complete": true,
  "valid": true,
  "violations": [],
  "path": [
    {"span_id": "s-root", "parent_id": null, "service": "gateway", "operation": "GET /checkout",
     "depth": 0, "self_time_ns": 400, "start_time": "2024-06-01T00:00:00Z",
     "end_time": "2024-06-01T00:00:00.000001Z"},
    {"span_id": "s-db", "parent_id": "s-root", "service": "db", "operation": "select",
     "depth": 1, "self_time_ns": 350, "start_time": "2024-06-01T00:00:00.000000350Z",
     "end_time": "2024-06-01T00:00:00.00000095Z"},
    {"span_id": "s-q2", "parent_id": "s-db", "service": "db", "operation": "query",
     "depth": 2, "self_time_ns": 250, "start_time": "2024-06-01T00:00:00.000000650Z",
     "end_time": "2024-06-01T00:00:00.0000009Z"}
  ]
}
```

A missing trace is a 404. A stored but unusable trace still answers 200 with
`valid: false` and its `violations`, so a caller can always see what is wrong; an
incomplete trace reports the same and returns the partial path.

### Service dependency graph

```http
GET /services/graph
GET /services/graph?trace_id=t-1
```

Without `trace_id` every stored trace is aggregated; an unknown `trace_id` is a
404. Traces that are incomplete or invalid are counted in `partial_traces` and
excluded from the aggregation.

```json
{
  "trace_id": null,
  "services": [
    {"service": "db", "span_count": 3, "error_spans": 1, "root_spans": 0,
     "self_time_ns": 800, "min_duration_ns": 200, "max_duration_ns": 600,
     "mean_duration_ns": 350, "p50_duration_ns": 250, "p95_duration_ns": 600,
     "operations": [{"operation": "query", "span_count": 2, "error_spans": 1,
       "self_time_ns": 450, "min_duration_ns": 200, "max_duration_ns": 250,
       "mean_duration_ns": 225, "p50_duration_ns": 200, "p95_duration_ns": 250}]}
  ],
  "edges": [
    {"caller": "gateway", "callee": "db", "call_count": 1, "error_count": 0,
     "min_duration_ns": 600, "max_duration_ns": 600, "mean_duration_ns": 600,
     "p50_duration_ns": 600, "p95_duration_ns": 600}
  ],
  "stats": {"trace_count": 1, "span_count": 6, "error_spans": 1,
            "duration_ns": 1000, "mean_duration_ns": 1000,
            "p50_duration_ns": 1000, "p95_duration_ns": 1000,
            "partial_traces": 0}
}
```

An edge is a parent/child pair whose two spans belong to different services, so
a call inside one service is not an edge. The duration statistics use the child's
`duration_ns`; percentiles are nearest rank (`index = ceil(p/100 * n) - 1`) on
the sorted samples, which is deterministic and exact for small traces.
`stats.duration_ns` is the sum of every accessible trace's root duration.

### List traces

```http
GET /traces
GET /traces?service=db&limit=50
```

```json
{"traces": [
  {"trace_id": "t-1", "root_span_id": "s-root", "root_service": "gateway",
   "start_time": "2024-06-01T00:00:00Z", "duration_ns": 1000, "span_count": 6,
   "service_count": 4, "error_spans": 1, "complete": true, "valid": true}]}
```

Traces are ordered by root start instant, newest first, then by trace id.
`service` keeps only traces that contain that service, `limit` defaults to 100
and must be between 1 and 400.

## Error propagation

Each node of a trace carries four derived fields:

| Field | Meaning |
| --- | --- |
| `error` | the declared `status` of the span, untouched by propagation |
| `error_origin` | the span itself failed (`status` is `error`) |
| `error_origins` | every failed span id in this span's subtree, sorted |
| `propagated` | the subtree contains a failure that did not happen on this span |

So a failing leaf has `error: true`, `error_origin: true`, `propagated: false`
and `error_origins: ["leaf"]`, while each ancestor has `error: false`,
`error_origin: false`, `propagated: true` and `error_origins: ["leaf"]`. This
separates "this step failed" from "this step was affected by a failure below
it", and lets a caller find the origin of an incident by reading the root alone.
`error_spans` counts spans with `error: true`, not propagated ones.

## Errors

Errors use this shape:

```json
{"error":{"code":"validation_error","message":"human readable detail"}}
```

Validation errors return 400, missing resources 404, and conflicts 409. The
codes are `validation_error`, `not_found`, `conflict`, `trace_invalid` and
`internal_error`. An invalid trace additionally carries `trace_id` and
`violations` in the error object.

## Determinism

The same input produces the same output: spans and traces are ordered
explicitly, JSON objects are marshalled with sorted keys, percentiles use nearest
rank, and no wall-clock value is ever exposed. The persisted database is the only
source of truth, so a restart reproduces every response byte for byte.

## Tests

```bash
go test ./...
```
